use std::fmt::{Display, Formatter};

use anyhow::Result;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct UserId(i64);

#[derive(Debug, PartialEq)]
pub(crate) struct User {
    pub(crate) id: UserId,
    pub(crate) username: String,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) enum SellOrderType {
    // Order will be immediately executed if there is a matching buy order
    Immediate,
    // Order will be executed only after the auction is over
    Auction,
}

impl Display for SellOrderType {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Immediate => write!(f, "immediate"),
            Self::Auction => write!(f, "auction"),
        }
    }
}

impl SellOrderType {
    pub(crate) fn from_str(s: &str) -> Option<Self> {
        match s {
            "immediate" => Some(Self::Immediate),
            "auction" => Some(Self::Auction),
            _ => None,
        }
    }
}

#[derive(Debug, PartialEq)]
pub(crate) struct SellOrder {
    pub(crate) id: i64,
    pub(crate) seller_name: String,
    pub(crate) item_name: String,
    pub(crate) quantity: i64,
    pub(crate) price: i64,
    pub(crate) expiration_time: String,
    pub(crate) order_type: SellOrderType,
}

struct SellOrderEntry {
    seller_id: UserId,
    item_id: i64,
    quantity: i64,
    price: i64,
    buyer_id: Option<UserId>,
}

impl SellOrderEntry {
    fn order_type(&self) -> SellOrderType {
        if self.buyer_id == Some(self.seller_id) {
            SellOrderType::Immediate
        } else {
            SellOrderType::Auction
        }
    }
}

pub(crate) struct Storage {
    db: rusqlite::Connection,
    funds_item_id: i64,
}

impl Storage {
    pub(crate) fn open(path: &str) -> Result<Self> {
        let db = rusqlite::Connection::open(path)?;

        // Enable Write-Ahead Logging (WAL) mode for better performance and to enable concurrent reads and writes.
        // This pragma speeds up the database aprox 10x times.
        // https://www.sqlite.org/wal.html
        let journal_mode: String = db.query_row("PRAGMA journal_mode=WAL", (), |row| row.get(0))?;
        if journal_mode != "wal" && path != ":memory:" {
            Err(anyhow::anyhow!(
                "Failed to enable WAL mode. Current journal mode: {journal_mode}"
            ))?;
        }

        // From SQLite documentation (https://www.sqlite.org/compile.html):
        // ```
        // For maximum database safety following a power loss, the setting of PRAGMA synchronous=FULL is
        // recommended. However, in WAL mode, complete database integrity is guaranteed with PRAGMA
        // synchronous=NORMAL. With PRAGMA synchronous=NORMAL in WAL mode, recent changes to the database
        // might be rolled back by a power loss, but the database will not be corrupted. Furthermore,
        // transaction commit is much faster in WAL mode using synchronous=NORMAL than with the default
        // synchronous=FULL. For these reasons, it is recommended that the synchronous setting be changed
        // from FULL to NORMAL when switching to WAL mode.
        //  ```
        // This pragma speeds up the database aprox 1.5x times on top of the gain from the WAL mode.
        // See https://www.sqlite.org/pragma.html#pragma_synchronous for more details
        db.execute("PRAGMA synchronous=NORMAL", ())?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE
            ) STRICT",
            (),
        )?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS items (
                id INTEGER PRIMARY KEY,
                name TEXT NOT NULL UNIQUE
            ) STRICT",
            (),
        )?;

        // We store balance in funds as a separate item to simplify the code
        db.execute("INSERT OR IGNORE INTO items (name) VALUES ('funds')", ())?;
        let funds_item_id =
            db.query_row("SELECT id FROM items WHERE name = 'funds'", [], |row| {
                row.get(0)
            })?;

        db.execute(
            "CREATE TABLE IF NOT EXISTS user_items (
                user_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity >= 0),
                FOREIGN KEY (user_id) REFERENCES users (id),
                FOREIGN KEY (item_id) REFERENCES items (id),
                PRIMARY KEY (user_id, item_id)
            ) STRICT",
            (),
        )?;

        // expiration_time - Unix timestamp in seconds
        // buyer_id stores either NULL or user_id:
        // - equal to the seller_id for immediate orders
        // - NULL for aution orders without bid
        // - Not NULL and not equal to the seller_id for auction orders with bid
        db.execute(
            "CREATE TABLE IF NOT EXISTS sell_orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                seller_id INTEGER NOT NULL,
                item_id INTEGER NOT NULL,
                quantity INTEGER NOT NULL CHECK(quantity > 0),
                price INTEGER NOT NULL CHECK(price > 0),
                expiration_time INTEGER NOT NULL,
                buyer_id INTEGER,
                FOREIGN KEY (seller_id) REFERENCES users (id),
                FOREIGN KEY (buyer_id) REFERENCES users (id),
                FOREIGN KEY (item_id) REFERENCES items (id)
            ) STRICT",
            (),
        )?;
        // Speed up filtering by expiration_time
        db.execute("CREATE INDEX IF NOT EXISTS sell_orders_expiration_time ON sell_orders (expiration_time)", ())?;

        Ok(Self { db, funds_item_id })
    }

    pub(crate) fn login(&self, username: &str) -> Result<User> {
        if username.is_empty() {
            Err(anyhow::anyhow!("Username cannot be empty"))?;
        }

        let mut stmt = self
            .db
            .prepare("SELECT id FROM users WHERE username = ?1")?;
        let user_id = match stmt.query_row([username], |row| row.get(0)) {
            Ok(user_id) => user_id,
            Err(rusqlite::Error::QueryReturnedNoRows) => {
                self.db
                    .execute("INSERT INTO users (username) VALUES (?1)", [username])?;
                let user_id = self.db.last_insert_rowid();

                // Initialize user's balance
                self.db.execute(
                    "INSERT INTO user_items (user_id, item_id, quantity) VALUES (?1, ?2, 0)",
                    [user_id, self.funds_item_id],
                )?;

                user_id
            }
            Err(err) => Err(err)?,
        };

        Ok(User {
            id: UserId(user_id),
            username: username.to_owned(),
        })
    }

    pub(crate) fn view_items(&self, user_id: UserId) -> Result<Vec<(String, i64)>> {
        let mut stmt = self.db.prepare(
            "SELECT items.name, user_items.quantity
            FROM user_items
            INNER JOIN items ON user_items.item_id = items.id
            WHERE user_items.user_id = ?1",
        )?;
        let items = stmt
            .query_map([user_id.0], |row| Ok((row.get(0)?, row.get(1)?)))?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(items)
    }

    pub(crate) fn deposit(&self, user_id: UserId, item_name: &str, quantity: i64) -> Result<()> {
        if item_name.is_empty() {
            return Err(anyhow::anyhow!("Item name cannot be empty"));
        }
        if quantity <= 0 {
            return Err(anyhow::anyhow!("Quantity must be positive"));
        }

        self.get_item_id(item_name)
            .or_else(|err| match err {
                rusqlite::Error::QueryReturnedNoRows => {
                    self.db
                        .execute("INSERT INTO items (name) VALUES (?1)", [item_name])?;
                    Ok(self.db.last_insert_rowid())
                }
                err => Err(err),
            })
            .and_then(|item_id| self.deposit_inner(user_id, item_id, quantity))
            .map_err(anyhow::Error::msg)
    }

    pub(crate) fn withdraw(&self, user_id: UserId, item_name: &str, quantity: i64) -> Result<()> {
        if item_name.is_empty() {
            return Err(anyhow::anyhow!("Item name cannot be empty"));
        }
        if quantity <= 0 {
            return Err(anyhow::anyhow!("Quantity must be positive"));
        }

        self.get_item_id(item_name)
            .map_err(|err| anyhow::anyhow!("no such item: {err}"))
            .and_then(|item_id| self.withdraw_inner(user_id, item_id, quantity))
            .map_err(|_| anyhow::anyhow!("Not enough {}(s) to withdraw", item_name))
    }

    pub(crate) fn view_sell_orders(&self) -> Result<Vec<SellOrder>> {
        let mut stmt = self.db.prepare(
            "SELECT
                sell_orders.id,
                users.username,
                items.name,
                sell_orders.quantity,
                sell_orders.price,
                DATETIME(sell_orders.expiration_time, 'unixepoch'),
                sell_orders.seller_id,
                sell_orders.buyer_id
            FROM sell_orders
            INNER JOIN users ON sell_orders.seller_id = users.id
            INNER JOIN items ON sell_orders.item_id = items.id",
        )?;
        let orders = stmt
            .query_map([], |row| {
                let seller_id: i64 = row.get(6)?;
                let buyer_id: Option<i64> = row.get(7)?;
                Ok(SellOrder {
                    id: row.get(0)?,
                    seller_name: row.get(1)?,
                    item_name: row.get(2)?,
                    quantity: row.get(3)?,
                    price: row.get(4)?,
                    expiration_time: row.get(5)?,
                    order_type: if buyer_id == Some(seller_id) {
                        SellOrderType::Immediate
                    } else {
                        SellOrderType::Auction
                    },
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(anyhow::Error::msg);
        orders
    }

    pub(crate) fn place_sell_order(
        &self,
        order_type: SellOrderType,
        seller_id: UserId,
        item_name: &str,
        quantity: i64,
        price: i64,
        unix_expiration_time: i64,
    ) -> Result<()> {
        if quantity < 0 {
            Err(anyhow::anyhow!("Cannot sell negative amount"))?;
        }
        if price < 0 {
            Err(anyhow::anyhow!("Cannot sell for negative price"))?;
        }
        if item_name == "funds" {
            Err(anyhow::anyhow!(
                "Cannot sell funds for funds, it's a speculation!"
            ))?;
        }

        let transaction_guard = self.db.unchecked_transaction()?;

        let item_id = self
            .get_item_id(item_name)
            .map_err(|err| anyhow::anyhow!("no such item: {err}"))
            .and_then(|item_id| {
                self.withdraw_inner(seller_id, item_id, quantity)
                    .map(|()| item_id)
            })
            .map_err(|_| anyhow::anyhow!("Not enough {item_name}(s) to sell"))?;

        // Fee is 5% of the price + 1 funds
        let fee = price / 20 + 1;
        self.withdraw_inner(seller_id, self.funds_item_id, fee)
            .map_err(|_err| {
                anyhow::anyhow!("Not enough funds to pay {fee} funds fee (which is 5% + 1)")
            })?;

        // For immediate orders, buyer_id is equal to the seller_id.
        // For auction orders, buyer_id is null untill someone places a bid.
        let buyer_id = match order_type {
            SellOrderType::Immediate => Some(seller_id.0),
            SellOrderType::Auction => None,
        };

        self.db.execute(
            "INSERT INTO sell_orders (seller_id, item_id, quantity, price, expiration_time, buyer_id)
            VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
            (seller_id.0, item_id, quantity, price, unix_expiration_time, buyer_id)
        )?;
        transaction_guard.commit()?;
        Ok(())
    }

    pub(crate) fn execute_immediate_sell_order(
        &self,
        buyer_id: UserId,
        order_id: i64,
    ) -> Result<()> {
        let order = self
            .get_sell_oder_entry(order_id)
            .map_err(|_| anyhow::anyhow!("Immediate sell order #{order_id} doesn't exist"))?;
        if order.order_type() != SellOrderType::Immediate {
            return Err(anyhow::anyhow!(
                "Order #{order_id} is not an immediate order"
            ));
        }
        if buyer_id == order.seller_id {
            return Err(anyhow::anyhow!("You can't buy your own items"));
        }

        let transaction_guard = self.db.unchecked_transaction()?;
        // deduce funds from the buyer
        self.withdraw_inner(buyer_id, self.funds_item_id, order.price)?;
        // add funds to the seller
        self.deposit_inner(order.seller_id, self.funds_item_id, order.price)?;
        // transfer item to the buyer
        self.deposit_inner(buyer_id, order.item_id, order.quantity)?;
        // delete the order
        self.db
            .execute("DELETE FROM sell_orders WHERE id = ?1", [order_id])?;
        transaction_guard.commit()?;
        Ok(())
    }

    pub(crate) fn place_bid_on_auction_sell_order(
        &self,
        buyer_id: UserId,
        sell_order_id: i64,
        bid: i64,
    ) -> Result<()> {
        let order = self
            .get_sell_oder_entry(sell_order_id)
            .map_err(|_| anyhow::anyhow!("Auction sell order #{sell_order_id} doesn't exist"))?;
        if order.order_type() != SellOrderType::Auction {
            return Err(anyhow::anyhow!(
                "Order #{sell_order_id} is not an auction order"
            ));
        }
        if buyer_id == order.seller_id {
            return Err(anyhow::anyhow!("You can't buy your own items"));
        }
        if bid <= order.price {
            return Err(anyhow::anyhow!("Bid must be higher than the current price"));
        }

        let transaction_guard = self.db.unchecked_transaction()?;
        if let Some(buyer_id) = order.buyer_id {
            // return funds to the previous buyer if any
            self.deposit_inner(buyer_id, self.funds_item_id, order.price)?;
        }

        // deduce funds from the buyer
        self.withdraw_inner(buyer_id, self.funds_item_id, bid)?;

        // update the order
        self.db.execute(
            "UPDATE sell_orders SET price = ?1, buyer_id = ?2 WHERE id = ?3",
            (bid, buyer_id.0, sell_order_id),
        )?;
        transaction_guard.commit()?;
        Ok(())
    }

    pub(crate) fn process_expired_sell_orders(&self, unix_now: i64) -> Result<()> {
        let transaction_guard = self.db.unchecked_transaction()?;

        // 1. Aggregate orders that sells the same item to the same user into `aggregated_orders`
        //   - for for immediate order and auction order without bid we return items to the seller
        //   - for auction order with bid we move items to the buyer
        // 2. Add funds as payment to the `aggregated_orders` via UNION ALL for all auction orders with bid
        // 3. Insert or update user_items with the aggregated orders
        self.db.execute(
            "WITH aggregated_orders AS (
              SELECT
                CASE
                  WHEN buyer_id IS NULL OR buyer_id = seller_id THEN seller_id
                  ELSE buyer_id
                END as user_id,
                item_id,
                SUM(quantity) as total_quantity
              FROM sell_orders
              WHERE sell_orders.expiration_time <= ?1
              GROUP BY user_id, item_id
              UNION ALL
              SELECT
                seller_id as user_id,
                ?2 as item_id,
                SUM(price) as total_quantity
              FROM sell_orders
              WHERE sell_orders.expiration_time <= ?1 AND buyer_id IS NOT NULL AND buyer_id != seller_id
              GROUP BY seller_id
            )
            INSERT OR REPLACE INTO user_items (user_id, item_id, quantity)
            SELECT
              aggregated_orders.user_id,
              aggregated_orders.item_id,
              IFNULL(user_items.quantity, 0) + aggregated_orders.total_quantity
            FROM aggregated_orders
            LEFT JOIN user_items ON user_items.user_id = aggregated_orders.user_id
              AND user_items.item_id = aggregated_orders.item_id",
            (unix_now, self.funds_item_id),
        )?;

        self.db.execute(
            "DELETE FROM sell_orders WHERE expiration_time <= ?1",
            [unix_now],
        )?;

        transaction_guard.commit()?;
        Ok(())
    }

    fn get_item_id(&self, item_name: &str) -> Result<i64, rusqlite::Error> {
        let mut stmt = self.db.prepare("SELECT id FROM items WHERE name = ?1")?;
        stmt.query_row([item_name], |row| row.get(0))
    }

    fn get_user_item_quantity(&self, user_id: UserId, item_id: i64) -> Result<i64> {
        let mut stmt = self
            .db
            .prepare("SELECT quantity FROM user_items WHERE user_id = ?1 AND item_id = ?2")?;
        let quantity = match stmt.query_row([user_id.0, item_id], |row| row.get(0)) {
            Ok(quantity) => quantity,
            Err(rusqlite::Error::QueryReturnedNoRows) => 0,
            Err(err) => Err(err)?,
        };
        Ok(quantity)
    }

    fn deposit_inner(
        &self,
        user_id: UserId,
        item_id: i64,
        quantity: i64,
    ) -> Result<(), rusqlite::Error> {
        self.db
            .execute(
                "INSERT INTO user_items (user_id, item_id, quantity)
                VALUES (?1, ?2, ?3)
                ON CONFLICT (user_id, item_id) DO UPDATE SET quantity = quantity + ?3",
                [user_id.0, item_id, quantity],
            )
            .map(|_| ())
    }

    fn withdraw_inner(&self, user_id: UserId, item_id: i64, quantity: i64) -> Result<()> {
        // if quantity reaches zero - remove the record from the table for all items except funds
        let current_quantity = self.get_user_item_quantity(user_id, item_id)?;
        if current_quantity < quantity {
            return Err(anyhow::anyhow!("Not enough items to withdraw"));
        }

        // Keep the record about funds even if the balance reaches zero
        if current_quantity > quantity || item_id == self.funds_item_id {
            self.db.execute(
                "UPDATE user_items SET quantity = quantity - ?3
                WHERE user_id = ?1 AND item_id = ?2",
                [user_id.0, item_id, quantity],
            )?;
        } else {
            self.db.execute(
                "DELETE FROM user_items WHERE user_id = ?1 AND item_id = ?2",
                [user_id.0, item_id],
            )?;
        }
        Ok(())
    }

    fn get_sell_oder_entry(&self, order_id: i64) -> Result<SellOrderEntry, rusqlite::Error> {
        let mut stmt = self.db.prepare(
            "SELECT
                seller_id,
                item_id,
                quantity,
                price,
                buyer_id
            FROM sell_orders
            WHERE id = ?1",
        )?;
        stmt.query_row([order_id], |row| {
            let buyer_id: Option<i64> = row.get(4)?;
            Ok(SellOrderEntry {
                seller_id: UserId(row.get(0)?),
                item_id: row.get(1)?,
                quantity: row.get(2)?,
                price: row.get(3)?,
                buyer_id: buyer_id.map(UserId),
            })
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use parameterized::parameterized;
    use pretty_assertions::assert_eq;

    // "2021-01-01 00:00"
    const EXPIRATION_TIME: i64 = 1609459200;

    #[test]
    fn login() {
        let storage = Storage::open(":memory:").unwrap();

        for _ in 0..3 {
            assert_eq!(
                storage.login("test1").unwrap(),
                User {
                    id: UserId(1),
                    username: "test1".into()
                }
            );
            assert_eq!(
                storage.login("test2").unwrap(),
                User {
                    id: UserId(2),
                    username: "test2".into()
                }
            );
            assert_eq!(
                storage.login("test3").unwrap(),
                User {
                    id: UserId(3),
                    username: "test3".into()
                }
            );
        }

        assert_eq!(
            storage.login("").unwrap_err().to_string(),
            "Username cannot be empty"
        );
    }

    #[test]
    fn funds() {
        let storage = Storage::open(":memory:").unwrap();

        // freshly created user always has 0 funds
        let user1 = storage.login("user1").unwrap();
        assert_eq!(
            storage.view_items(user1.id).unwrap(),
            vec![("funds".into(), 0)]
        );

        assert!(storage.deposit(user1.id, "funds", 10).is_ok());
        assert_eq!(
            storage.view_items(user1.id).unwrap(),
            vec![("funds".into(), 10)]
        );

        assert!(storage.withdraw(user1.id, "funds", 7).is_ok());
        assert_eq!(
            storage.view_items(user1.id).unwrap(),
            vec![("funds".into(), 3)]
        );

        // Withdraw all funds doesn't remove the record
        assert!(storage.withdraw(user1.id, "funds", 3).is_ok());
        assert_eq!(
            storage.view_items(user1.id).unwrap(),
            vec![("funds".into(), 0)]
        );

        assert!(storage.deposit(user1.id, "funds", 3).is_ok());

        // get_or_create_user should not create new funds
        let user = storage.login("user1").unwrap();
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 3)]
        );

        // Withdraw more than we have
        assert!(!storage.withdraw(user.id, "funds", 10).is_ok());
        // Deposit negative amount
        assert!(!storage.deposit(user.id, "funds", -10).is_ok());
        // Withdraw negative amount
        assert!(!storage.withdraw(user.id, "funds", -10).is_ok());
        // Deposit zero
        assert!(!storage.deposit(user.id, "funds", 0).is_ok());
        // Withdraw zero
        assert!(!storage.withdraw(user.id, "funds", 0).is_ok());
        // Nothing should change
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 3)]
        );

        // deposit to non-existing user
        assert!(!storage.deposit(UserId(100), "funds", 10).is_ok());
        assert!(!storage.withdraw(UserId(100), "funds", 10).is_ok());

        // and check that we can deposit and withdraw from different users
        let user2 = storage.login("user2").unwrap();
        assert!(storage.deposit(user2.id, "funds", 20).is_ok());

        let user3 = storage.login("user3").unwrap();
        assert!(storage.deposit(user3.id, "funds", 30).is_ok());

        let user1 = storage.login("user1").unwrap();
        assert_eq!(
            storage.view_items(user1.id).unwrap(),
            vec![("funds".into(), 3)]
        );

        let user2 = storage.login("user2").unwrap();
        assert_eq!(
            storage.view_items(user2.id).unwrap(),
            vec![("funds".into(), 20)]
        );

        let user3 = storage.login("user3").unwrap();
        assert_eq!(
            storage.view_items(user3.id).unwrap(),
            vec![("funds".into(), 30)]
        );

        // Big numbers also are ok
        assert!(storage.deposit(user1.id, "funds", 100500).is_ok());
        assert!(storage.withdraw(user1.id, "funds", 100400).is_ok());
    }

    #[test]
    fn items() {
        let storage = Storage::open(":memory:").unwrap();

        let user = storage.login("user1").unwrap();
        // freshly created user always has 0 items
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 0)]
        );

        assert!(storage.deposit(user.id, "item1", 10).is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 0), ("item1".into(), 10)]
        );

        assert!(storage.deposit(user.id, "item2", 20).is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 0),
                ("item1".into(), 10),
                ("item2".into(), 20)
            ]
        );

        assert!(storage.withdraw(user.id, "item1", 5).is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 0),
                ("item1".into(), 5),
                ("item2".into(), 20)
            ]
        );

        assert!(storage.withdraw(user.id, "item2", 10).is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 0),
                ("item1".into(), 5),
                ("item2".into(), 10)
            ]
        );

        // withdraw to zero should remove the record
        assert!(storage.withdraw(user.id, "item1", 5).is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 0), ("item2".into(), 10)]
        );

        // login with the same username should return the same user with the same items
        let user = storage.login("user1").unwrap();
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 0), ("item2".into(), 10)]
        );

        // Withdraw more than we have
        assert!(!storage.withdraw(user.id, "item2", 20).is_ok());

        // Negative quantity
        assert!(!storage.deposit(user.id, "item2", -10).is_ok());
        assert!(!storage.withdraw(user.id, "item2", -10).is_ok());

        // Zero quantity
        assert!(!storage.deposit(user.id, "item2", 0).is_ok());
        assert!(!storage.withdraw(user.id, "item2", 0).is_ok());

        // empty item name
        assert!(!storage.deposit(user.id, "", 10).is_ok());
        assert!(!storage.withdraw(user.id, "", 10).is_ok());

        // Nothing should change
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 0), ("item2".into(), 10)]
        );

        // deposit to non-existing user
        assert!(!storage.deposit(UserId(100), "item1", 10).is_ok());
        assert!(!storage.withdraw(UserId(100), "item1", 10).is_ok());
    }

    #[parameterized(order_type = {
        SellOrderType::Immediate,
        SellOrderType::Auction,
    })]
    fn test_sell_orders_negative(order_type: SellOrderType) {
        // workaround for resolution ambiguity caused by `parameterized` macro
        use pretty_assertions::assert_eq;

        let storage = Storage::open(":memory:").unwrap();

        let user = storage.login("user").unwrap();
        assert!(storage.deposit(user.id, "funds", 100).is_ok());
        assert!(storage.deposit(user.id, "item1", 10).is_ok());
        assert!(storage.deposit(user.id, "item2", 20).is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 100),
                ("item1".into(), 10),
                ("item2".into(), 20)
            ]
        );

        // more than we have
        assert!(storage
            .place_sell_order(order_type, user.id, "item1", 110, 10, EXPIRATION_TIME)
            .is_err());
        // negative quantity
        assert!(storage
            .place_sell_order(order_type, user.id, "item1", -10, 10, EXPIRATION_TIME)
            .is_err());
        // negative price
        assert!(storage
            .place_sell_order(order_type, user.id, "item1", 10, -10, EXPIRATION_TIME)
            .is_err());
        // non-existing item
        assert!(storage
            .place_sell_order(
                order_type,
                user.id,
                "non-existing-item",
                1,
                10,
                EXPIRATION_TIME
            )
            .is_err());
        // non-existing user
        assert!(storage
            .place_sell_order(order_type, UserId(100), "item1", 1, 10, EXPIRATION_TIME)
            .is_err());
        // funds
        assert!(storage
            .place_sell_order(order_type, user.id, "funds", 1, 2, EXPIRATION_TIME)
            .is_err());

        // Finally, nothing should be changed
        assert_eq!(storage.view_sell_orders().unwrap(), vec![]);
    }

    #[parameterized(order_type = {
        SellOrderType::Immediate,
        SellOrderType::Auction,
    })]
    fn test_sell_orders_positive(order_type: SellOrderType) {
        // workaround for resolution ambiguity caused by `parameterized` macro
        use pretty_assertions::assert_eq;

        let storage = Storage::open(":memory:").unwrap();

        let user = storage.login("user").unwrap();
        assert!(storage.deposit(user.id, "funds", 100).is_ok());
        assert!(storage.deposit(user.id, "item1", 10).is_ok());
        assert!(storage.deposit(user.id, "item2", 20).is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 100),
                ("item1".into(), 10),
                ("item2".into(), 20)
            ]
        );

        for i in 1..10 {
            assert!(storage
                .place_sell_order(order_type, user.id, "item1", 1, 10 + i, EXPIRATION_TIME)
                .is_ok());
            assert_eq!(
                storage.view_items(user.id).unwrap(),
                vec![
                    ("funds".into(), 100 - i /* fee is 1 for each order */),
                    ("item1".into(), 10 - i),
                    ("item2".into(), 20)
                ]
            );
        }
        assert!(storage
            .place_sell_order(order_type, user.id, "item2", 15, 100, EXPIRATION_TIME)
            .is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 100 - 9 - 6),
                ("item1".into(), 1),
                ("item2".into(), 5)
            ]
        );

        // Item entry dissapears when quantity reaches zero
        assert!(storage
            .place_sell_order(order_type, user.id, "item2", 5, 120, EXPIRATION_TIME + 1)
            .is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![("funds".into(), 100 - 9 - 6 - 7), ("item1".into(), 1)]
        );

        pretty_assertions::assert_eq!(
            storage.view_sell_orders().unwrap(),
            vec![
                SellOrder {
                    id: 1,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 11,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 2,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 12,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 3,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 13,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 4,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 14,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 5,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 15,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 6,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 16,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 7,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 17,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 8,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 18,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 9,
                    seller_name: "user".into(),
                    item_name: "item1".into(),
                    quantity: 1,
                    price: 19,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 10,
                    seller_name: "user".into(),
                    item_name: "item2".into(),
                    quantity: 15,
                    price: 100,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type,
                },
                SellOrder {
                    id: 11,
                    seller_name: "user".into(),
                    item_name: "item2".into(),
                    quantity: 5,
                    price: 120,
                    expiration_time: "2021-01-01 00:00:01".into(), // as expected
                    order_type,
                },
            ]
        );

        // cancel expired orders
        assert!(storage.process_expired_sell_orders(EXPIRATION_TIME).is_ok());
        assert_eq!(
            storage.view_sell_orders().unwrap(),
            vec![SellOrder {
                id: 11,
                seller_name: "user".into(),
                item_name: "item2".into(),
                quantity: 5,
                price: 120,
                expiration_time: "2021-01-01 00:00:01".into(),
                order_type,
            }]
        );

        // items are returned but fee is not
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 100 - 9 - 6 - 7),
                ("item1".into(), 10),
                ("item2".into(), 15)
            ]
        );

        // And finally, cancel the last order
        assert!(storage
            .process_expired_sell_orders(EXPIRATION_TIME + 2)
            .is_ok());
        assert_eq!(
            storage.view_items(user.id).unwrap(),
            vec![
                ("funds".into(), 100 - 9 - 6 - 7),
                ("item1".into(), 10),
                ("item2".into(), 20)
            ]
        );
    }

    #[test]
    fn test_execute_immediate_sell_order_err() {
        let storage = Storage::open(":memory:").unwrap();

        let seller = storage.login("seller").unwrap();
        assert!(storage.deposit(seller.id, "funds", 100).is_ok());
        assert!(storage.deposit(seller.id, "item1", 10).is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item1",
                7,
                10,
                EXPIRATION_TIME
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Auction,
                seller.id,
                "item1",
                3,
                11,
                EXPIRATION_TIME
            )
            .is_ok());

        assert_eq!(
            storage.view_sell_orders().unwrap(),
            vec![
                SellOrder {
                    id: 1,
                    seller_name: "seller".into(),
                    item_name: "item1".into(),
                    quantity: 7,
                    price: 10,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Immediate,
                },
                SellOrder {
                    id: 2,
                    seller_name: "seller".into(),
                    item_name: "item1".into(),
                    quantity: 3,
                    price: 11,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                }
            ]
        );

        // You can't buy your own items
        assert!(!storage.execute_immediate_sell_order(seller.id, 1).is_ok());

        let buyer = storage.login("buyer").unwrap();

        // try to buy non-existing sell order
        assert!(!storage.execute_immediate_sell_order(buyer.id, 100).is_ok());

        // try to buy from non-existing user
        assert!(!storage.execute_immediate_sell_order(UserId(100), 1).is_ok());

        // try to buy without enough funds
        assert!(!storage.execute_immediate_sell_order(buyer.id, 1).is_ok());

        // try to buy auction order with not enough funds
        assert!(!storage.execute_immediate_sell_order(buyer.id, 2).is_ok());

        // repeat with funds
        assert!(storage.deposit(buyer.id, "funds", 100).is_ok());

        // still can't buy auction order
        assert!(!storage.execute_immediate_sell_order(buyer.id, 2).is_ok());

        // while immediate order should be bought
        assert!(storage.execute_immediate_sell_order(buyer.id, 1).is_ok());
    }

    #[test]
    fn test_execute_immediate_sell_order_ok() {
        let storage = Storage::open(":memory:").unwrap();

        let seller = storage.login("seller").unwrap();
        assert!(storage.deposit(seller.id, "funds", 100).is_ok());
        assert!(storage.deposit(seller.id, "item1", 10).is_ok());
        assert!(storage.deposit(seller.id, "item2", 20).is_ok());
        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![
                ("funds".into(), 100),
                ("item1".into(), 10),
                ("item2".into(), 20),
            ]
        );

        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item1",
                2,
                2,
                EXPIRATION_TIME + 1
            )
            .is_ok());

        // sell fee is (5% + 1)
        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![
                ("funds".into(), 99),
                ("item1".into(), 8),
                ("item2".into(), 20),
            ]
        );

        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item1",
                3,
                3,
                EXPIRATION_TIME + 2
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item1",
                4,
                4,
                EXPIRATION_TIME + 3
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item1",
                1,
                4,
                EXPIRATION_TIME + 4
            )
            .is_ok());

        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item2",
                5,
                5,
                EXPIRATION_TIME + 5
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item2",
                10,
                10,
                EXPIRATION_TIME + 6
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item2",
                5,
                15,
                EXPIRATION_TIME + 7
            )
            .is_ok());

        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![("funds".into(), 93)]
        );

        let buyer = storage.login("buyer").unwrap();
        assert!(storage.deposit(buyer.id, "funds", 20).is_ok());
        // 1 item1 for 4 funds
        assert!(storage.execute_immediate_sell_order(buyer.id, 4).is_ok());

        // check items and funds
        assert_eq!(
            storage.view_items(buyer.id).unwrap(),
            vec![("funds".into(), 16), ("item1".into(), 1)]
        );
        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![("funds".into(), 97)]
        );

        assert!(storage
            .process_expired_sell_orders(EXPIRATION_TIME + 4)
            .is_ok());

        // try to buy expired order
        assert!(!storage.execute_immediate_sell_order(buyer.id, 3).is_ok());

        // check items and funds
        assert_eq!(
            storage.view_items(buyer.id).unwrap(),
            vec![("funds".into(), 16), ("item1".into(), 1)]
        );

        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![("funds".into(), 97), ("item1".into(), 9)]
        );

        // buy the rest
        assert!(storage.execute_immediate_sell_order(buyer.id, 5).is_ok());
        assert!(storage.execute_immediate_sell_order(buyer.id, 6).is_ok());
        // not enough money
        assert!(!storage.execute_immediate_sell_order(buyer.id, 7).is_ok());

        // check items and funds
        assert_eq!(
            storage.view_items(buyer.id).unwrap(),
            vec![
                ("funds".into(), 1),
                ("item1".into(), 1),
                ("item2".into(), 15)
            ]
        );
        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![("funds".into(), 97 + 15), ("item1".into(), 9)]
        );

        // check remaining orders
        assert_eq!(
            storage.view_sell_orders().unwrap(),
            vec![SellOrder {
                id: 7,
                seller_name: "seller".into(),
                item_name: "item2".into(),
                quantity: 5,
                price: 15,
                expiration_time: "2021-01-01 00:00:07".into(),
                order_type: SellOrderType::Immediate,
            }]
        );
    }

    #[test]
    fn test_place_a_bid() {
        let storage = Storage::open(":memory:").unwrap();

        let seller = storage.login("seller").unwrap();
        assert!(storage.deposit(seller.id, "funds", 100).is_ok());
        assert!(storage.deposit(seller.id, "item1", 10).is_ok());
        assert!(storage.deposit(seller.id, "item2", 20).is_ok());
        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![
                ("funds".into(), 100),
                ("item1".into(), 10),
                ("item2".into(), 20),
            ]
        );
        assert!(storage
            .place_sell_order(
                SellOrderType::Immediate,
                seller.id,
                "item1",
                7,
                10,
                EXPIRATION_TIME
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Auction,
                seller.id,
                "item1",
                3,
                11,
                EXPIRATION_TIME
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Auction,
                seller.id,
                "item2",
                1,
                20,
                EXPIRATION_TIME
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Auction,
                seller.id,
                "item2",
                2,
                45,
                EXPIRATION_TIME
            )
            .is_ok());
        assert!(storage
            .place_sell_order(
                SellOrderType::Auction,
                seller.id,
                "item2",
                2,
                50,
                EXPIRATION_TIME
            )
            .is_ok());

        // check orders
        assert_eq!(
            storage.view_sell_orders().unwrap(),
            vec![
                SellOrder {
                    id: 1,
                    seller_name: "seller".into(),
                    item_name: "item1".into(),
                    quantity: 7,
                    price: 10,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Immediate,
                },
                SellOrder {
                    id: 2,
                    seller_name: "seller".into(),
                    item_name: "item1".into(),
                    quantity: 3,
                    price: 11,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                },
                SellOrder {
                    id: 3,
                    seller_name: "seller".into(),
                    item_name: "item2".into(),
                    quantity: 1,
                    price: 20,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                },
                SellOrder {
                    id: 4,
                    seller_name: "seller".into(),
                    item_name: "item2".into(),
                    quantity: 2,
                    price: 45,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                },
                SellOrder {
                    id: 5,
                    seller_name: "seller".into(),
                    item_name: "item2".into(),
                    quantity: 2,
                    price: 50,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                }
            ]
        );

        // You can't can't place a bid on your own items
        assert!(!storage
            .place_bid_on_auction_sell_order(seller.id, 2, 20)
            .is_ok());

        let buyer = storage.login("buyer").unwrap();

        // can't place a bid on non-existing sell order
        assert!(!storage
            .place_bid_on_auction_sell_order(buyer.id, 100, 20)
            .is_ok());

        // can't place a bid from non-existing user
        assert!(!storage
            .place_bid_on_auction_sell_order(UserId(100), 2, 20)
            .is_ok());

        // can't place a bid without enough funds
        assert!(!storage
            .place_bid_on_auction_sell_order(buyer.id, 20, 20)
            .is_ok());

        // can't place a bid on auction order with not enough funds
        assert!(!storage
            .place_bid_on_auction_sell_order(buyer.id, 1, 20)
            .is_ok());

        // repeat with funds
        assert!(storage.deposit(buyer.id, "funds", 100).is_ok());

        // still can't place a bid on immediate order
        assert!(!storage
            .place_bid_on_auction_sell_order(buyer.id, 1, 20)
            .is_ok());

        // while it is possible to place a bid on auction order
        assert!(storage
            .place_bid_on_auction_sell_order(buyer.id, 2, 20)
            .is_ok());

        assert_eq!(
            storage.view_items(buyer.id).unwrap(),
            vec![("funds".into(), 80)]
        );

        // check that bid is placed
        assert_eq!(
            storage.view_sell_orders().unwrap(),
            vec![
                SellOrder {
                    id: 1,
                    seller_name: "seller".into(),
                    item_name: "item1".into(),
                    quantity: 7,
                    price: 10,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Immediate,
                },
                SellOrder {
                    id: 2,
                    seller_name: "seller".into(),
                    item_name: "item1".into(),
                    quantity: 3,
                    price: 20, // a bid was made!
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                },
                SellOrder {
                    id: 3,
                    seller_name: "seller".into(),
                    item_name: "item2".into(),
                    quantity: 1,
                    price: 20,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                },
                SellOrder {
                    id: 4,
                    seller_name: "seller".into(),
                    item_name: "item2".into(),
                    quantity: 2,
                    price: 45,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                },
                SellOrder {
                    id: 5,
                    seller_name: "seller".into(),
                    item_name: "item2".into(),
                    quantity: 2,
                    price: 50,
                    expiration_time: "2021-01-01 00:00:00".into(),
                    order_type: SellOrderType::Auction,
                }
            ]
        );

        // but you can't repeat a bid
        assert!(!storage
            .place_bid_on_auction_sell_order(buyer.id, 2, 20)
            .is_ok());

        let another_buyer = storage.login("another buyer").unwrap();
        assert!(storage.deposit(another_buyer.id, "funds", 100).is_ok());

        // and you can't lower previous bid
        assert!(!storage
            .place_bid_on_auction_sell_order(another_buyer.id, 2, 19)
            .is_ok());

        // but you can increase it, but not greater than funds allow
        assert!(!storage
            .place_bid_on_auction_sell_order(another_buyer.id, 2, 121)
            .is_ok());
        assert!(storage
            .place_bid_on_auction_sell_order(another_buyer.id, 2, 21)
            .is_ok());

        assert!(storage
            .place_bid_on_auction_sell_order(another_buyer.id, 3, 25)
            .is_ok());
        assert!(storage
            .place_bid_on_auction_sell_order(another_buyer.id, 4, 50)
            .is_ok());

        assert!(storage
            .place_bid_on_auction_sell_order(buyer.id, 3, 27)
            .is_ok());

        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![("funds".into(), 90), ("item2".into(), 20 - 1 - 2 - 2)]
        );
        assert_eq!(
            storage.view_items(buyer.id).unwrap(),
            vec![("funds".into(), 100 - 27)]
        );
        assert_eq!(
            storage.view_items(another_buyer.id).unwrap(),
            vec![("funds".into(), 100 - 21 - 50)]
        );

        // and finally process expired orders - make them real
        assert!(storage
            .process_expired_sell_orders(EXPIRATION_TIME + 1)
            .is_ok());
        assert_eq!(
            storage.view_items(seller.id).unwrap(),
            vec![
                ("funds".into(), 90 + 27 + 21 + 50),
                ("item1".into(), 7),
                ("item2".into(), 20 - 5 + 2)
            ]
        );
        assert_eq!(
            storage.view_items(buyer.id).unwrap(),
            vec![("funds".into(), 100 - 27), ("item2".into(), 1),]
        );
        assert_eq!(
            storage.view_items(another_buyer.id).unwrap(),
            vec![
                ("funds".into(), 100 - 21 - 50),
                ("item1".into(), 3),
                ("item2".into(), 2)
            ]
        );
    }
}
