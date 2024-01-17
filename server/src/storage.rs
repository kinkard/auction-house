use anyhow::Result;

#[derive(Clone, Copy, Debug, PartialEq)]
pub(crate) struct UserId(i64);

#[derive(Debug, PartialEq)]
pub(crate) struct User {
    pub(crate) id: UserId,
    pub(crate) username: String,
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
        let mut stmt = self.db.prepare(
            "INSERT INTO user_items (user_id, item_id, quantity)
            VALUES (?1, ?2, ?3)
            ON CONFLICT (user_id, item_id) DO UPDATE SET quantity = quantity + ?3",
        )?;
        stmt.execute([user_id.0, item_id, quantity])?;
        Ok(())
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

    // tl::expected<void, std::string> deposit_inner(UserId user_id, int item_id, int quantity);
    // tl::expected<void, std::string> withdraw_inner(UserId user_id, int item_id, int quantity);
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
