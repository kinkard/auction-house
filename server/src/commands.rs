use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::Mutex;

use crate::storage::{SellOrderType, Storage, User};

pub(crate) struct CommandsProcessor {
    user: User,
    storage: Arc<Mutex<Storage>>,
}

const HELP_MESSAGE: &str =
    "Available commands:
    - whoami: Displays the username of the current user
    - ping: Replies 'pong'
    - help: Prints this help message about all available commands

    - deposit: Deposits a specified amount into the user's account. Format: 'deposit <item name> [<quantity>]'.
      'fund' is a special item name that can be used to deposit funds into the user's account
      Example: 'deposit funds 100' - deposits 100 funds, 'deposit Sword' - deposits 1 Sword
    - withdraw: Withdraws a specified amount from the user's account. Format: 'withdraw <item name> [<quantity>]'
      Example: 'withdraw arrow 5' - withdraws 5 arrows, 'withdraw Sword' - withdraws 1 Sword
    - view_items: Displays a list items for the current user

    - view_sell_orders: Displays a list of all sell orders from all users
    - sell: Places an item for sale at a specified price. Format: 'sell [immediate|auction] <item_name> [<quantity>] <price>'
      - immediate sell order - will be executed immediately once someone buys it. Otherwise it will expire in 5 minutes
        and items will be returned to the seller, but not the fee, which is `5% of the price + 1` funds
      - auction sell order - will be executed once it expires if someone placed a bid on it
    - buy: Executes immediate sell order or places a bid on a auction sell order. Format: 'buy <sell_order_id> [<bid>]'
      - no bid - executes immediate sell order
      - bid - places a bid on a auction sell order

    Usage: <command> [<args>], where `[]` annotates optional argumet(s)";

impl CommandsProcessor {
    pub(crate) fn new(user: User, storage: Arc<Mutex<Storage>>) -> Self {
        Self { user, storage }
    }

    pub(crate) async fn process_request(&self, request: &str) -> Result<String> {
        let (command, args) = if let Some(pos) = request.find(' ') {
            (&request[..pos], request[pos + 1..].trim())
        } else {
            (request, "")
        };

        match command {
            "ping" => Ok("pong".to_string()),
            "whoami" => Ok(self.user.username.clone()),
            "help" => Ok(HELP_MESSAGE.to_string()),

            "view_items" => self.view_items().await,
            "deposit" => self.deposit(args).await,
            "withdraw" => self.withdraw(args).await,

            "view_sell_orders" => self.view_sell_orders().await,
            "sell" => self.sell(args).await,
            "buy" => self.buy(args).await,
            _ => Err(anyhow!("Unknown command '{command}'")),
        }
    }

    async fn view_items(&self) -> Result<String> {
        self.storage
            .lock()
            .await
            .view_items(self.user.id)
            .map(|items| format!("Items: {items:?}"))
    }

    async fn deposit(&self, args: &str) -> Result<String> {
        if args.is_empty() {
            return Err(anyhow!(
                "Argument is required. Format: 'deposit <item name> [<quantity>]'"
            ));
        }

        let (item_name, quantity) = parse_item_name_and_quantity(args);
        self.storage
            .lock()
            .await
            .deposit(self.user.id, item_name, quantity)
            .with_context(|| format!("Failed to deposit {quantity} {item_name}(s)"))
            .map(|()| format!("Successfully deposited {quantity} {item_name}(s)"))
    }

    async fn withdraw(&self, args: &str) -> Result<String> {
        if args.is_empty() {
            return Err(anyhow!(
                "Argument is required. Format: 'withdraw <item name> [<quantity>]'"
            ));
        }

        let (item_name, quantity) = parse_item_name_and_quantity(args);
        self.storage
            .lock()
            .await
            .withdraw(self.user.id, item_name, quantity)
            .with_context(|| format!("Failed to withdraw {quantity} {item_name}(s)"))
            .map(|()| format!("Successfully withdrawed {quantity} {item_name}(s)"))
    }

    async fn view_sell_orders(&self) -> Result<String> {
        let orders = self.storage.lock().await.view_sell_orders()?;
        let mut result = String::from("Sell orders:");
        for order in orders {
            let order_type_str = match order.order_type {
                SellOrderType::Auction => "on auction ",
                SellOrderType::Immediate => "",
            };

            if order.quantity == 1 {
                result.push_str(&format!(
                    "\n- #{}: {} is selling a {} for {} funds {}until {}",
                    order.id,
                    order.seller_name,
                    order.item_name,
                    order.price,
                    order_type_str,
                    order.expiration_time
                ));
            } else {
                result.push_str(&format!(
                    "\n- #{}: {} is selling {} {}(s) for {} funds {}until {}",
                    order.id,
                    order.seller_name,
                    order.quantity,
                    order.item_name,
                    order.price,
                    order_type_str,
                    order.expiration_time
                ));
            }
        }
        Ok(result)
    }

    // args should be in the format "[immediate|auction] <item_name> [quantity] <price>".
    // Price is mandatory, quantity is optional and defaults to 1.
    // Examples:
    // - "arrow 5 10" -> {"arrow", .quantity=5, .price=10, .type=Immediate}
    // - "holy sword 1 100" -> {"holy sword", .quantity=1, .price=100, .type=Immediate}
    // - "arrow 10" -> {"arrow", .quantity=1, .price=10, .type=Immediate}
    // - "immidiate arrow 10 5" -> {"arrow", .quantity=10, .price=5, .type=Immediate}
    // - "auction arrow 10 5" -> {"arrow", .quantity=10, .price=5, .type=Auction}
    async fn sell(&self, args: &str) -> Result<String> {
        let (order_type, args) = args
            .find(' ')
            .and_then(|pos| {
                SellOrderType::from_str(&args[..pos])
                    .map(|order_type| (order_type, &args[pos + 1..]))
            })
            .unwrap_or((SellOrderType::Immediate, args));

        let (price, args) = args
            .rfind(' ')
            .and_then(|pos| {
                args[pos + 1..]
                    .parse::<i64>()
                    .ok()
                    .map(|price| (price, &args[..pos]))
            })
            .ok_or(anyhow!(
                "Unable to parse order. \
                Expected: 'sell [immediate|auction] <item_name> [<quantity>] <price>'. \
                Default type is 'immediate' and default quantity is 1"
            ))?;

        let (item_name, quantity) = parse_item_name_and_quantity(args);

        let order_lifetime_seconds = 5 * 60; // 5 min
        let unix_now = std::time::UNIX_EPOCH.elapsed()?.as_secs() as i64;

        self.storage
            .lock()
            .await
            .place_sell_order(
                order_type,
                self.user.id,
                item_name,
                quantity,
                price,
                unix_now + order_lifetime_seconds,
            )
            .with_context(|| {
                format!("Failed to place {order_type} sell order for {quantity} {item_name}(s)")
            })
            .map(|()| {
                format!("Successfully placed {order_type} sell order for {quantity} {item_name}(s)")
            })
    }

    // args should be in the format "<sell_order_id> [<bid>]"
    // if bid provided - try to make a bid on the auction sell order
    // otherwise - try to execute the immediate sell order
    async fn buy(&self, args: &str) -> Result<String> {
        let (bid, args) = args
            .rfind(' ')
            .and_then(|pos| {
                args[pos + 1..]
                    .parse::<i64>()
                    .ok()
                    .map(|bid| (Some(bid), &args[..pos]))
            })
            .unwrap_or((None, args));

        let sell_order_id = args
            .parse::<i64>()
            .with_context(|| "Unable to parse sell order id")?;

        if let Some(bid) = bid {
            self.storage
                .lock()
                .await
                .place_bid_on_auction_sell_order(self.user.id, sell_order_id, bid)
                .with_context(|| format!("Failed to place bid on sell order #{sell_order_id}"))
                .map(|()| format!("Successfully placed bid on sell order #{sell_order_id}"))
        } else {
            self.storage
                .lock()
                .await
                .execute_immediate_sell_order(self.user.id, sell_order_id)
                .with_context(|| {
                    format!("Failed to executed immediate sell order #{sell_order_id}")
                })
                .map(|()| format!("Successfully executed immediate sell order #{sell_order_id}"))
        }
    }
}

// Parses the last word as a quantity and if failed - uses the whole string as an item name
// Examples:
// - "arrow 5" -> {"arrow", 5}
// - "holy sword 1" -> {"holy sword", 1}
// - "arrow" -> {"arrow", 1}
// - "holy sword" -> {"holy sword", 1}
fn parse_item_name_and_quantity(args: &str) -> (&str, i64) {
    if let Some(pos) = args.rfind(' ') {
        if let Ok(quantity) = args[pos + 1..].parse::<i64>() {
            return (&args[..pos], quantity);
        }
    }
    (args, 1)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_item_name_and_quantity() {
        assert_eq!(parse_item_name_and_quantity("arrow 5"), ("arrow", 5));
        assert_eq!(
            parse_item_name_and_quantity("holy sword 1"),
            ("holy sword", 1)
        );
        assert_eq!(parse_item_name_and_quantity("arrow"), ("arrow", 1));
        assert_eq!(
            parse_item_name_and_quantity("holy sword"),
            ("holy sword", 1)
        );
        assert_eq!(parse_item_name_and_quantity(""), ("", 1));
    }
}
