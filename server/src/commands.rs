use std::sync::Arc;

use anyhow::{anyhow, Context, Result};
use tokio::sync::Mutex;

use crate::storage::{Storage, UserId};

pub(crate) struct CommandsProcessor {
    user_id: UserId,
    storage: Arc<Mutex<Storage>>,
}

impl CommandsProcessor {
    pub(crate) fn new(user_id: UserId, storage: Arc<Mutex<Storage>>) -> Self {
        Self { user_id, storage }
    }

    pub(crate) async fn process_request(&self, request: &str) -> Result<String> {
        let (command, args) = if let Some(pos) = request.find(' ') {
            (&request[..pos], request[pos + 1..].trim())
        } else {
            (request, "")
        };

        match command {
            "deposit" => self.deposit(args).await,
            "withdraw" => self.withdraw(args).await,
            "view_items" => self.view_items().await,
            _ => Err(anyhow!("Unknown command '{command}'")),
        }
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
            .deposit(self.user_id, item_name, quantity)
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
            .withdraw(self.user_id, item_name, quantity)
            .with_context(|| format!("Failed to withdraw {quantity} {item_name}(s)"))
            .map(|()| format!("Successfully withdrawed {quantity} {item_name}(s)"))
    }

    async fn view_items(&self) -> Result<String> {
        self.storage
            .lock()
            .await
            .view_items(self.user_id)
            .map(|items| format!("Items: {items:?}"))
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
