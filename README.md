# The Auction House

This is a port of the [Auction House](https://github.com/kinkard/auction-house-cpp) test project to Rust

## Supported functionality

- User can login using `client` or telnet, once `server` is launched
- User can deposit or withdraw items, using the following command: `deposit/withdraw <item name> [quantity]`. For example, `deposit funds 100`
- User can see own items via `view_items`
- User can create immediate or auction sell orders using `sell [immediate|auction] <item_name> [<quantity>] <price>` command. For example, `sell Sword 1 100` will create a immediate sell order for 1 Sword for 100 funds. 5% + 1 funds will be taken as a fee
- User can see all sell orders via `view_sell_orders`
- User can buy item that is on sale or make a bid on auction order. Sell orders are refered by id. For example, `buy 20` will buy order #20, while `buy 20 200` will made a bid to the order #20 with 200 funds. User will see errors if order is not matched, if bid is smaller than current price and so on
- **todo:** User will see notifications (if they are still connected) once their sell order is executed, either immediate or auction
- **todo:** All transactions are available in transaction log

## Build & Run

```sh
cargo run --release 3000 db.sqlite
```

Transaction log can be monitored via `tail -f transaction.log`.

## Client

This repo also contains a minimalistic client that sends everything you type in console to the server and prints everything server sends back. The telnet can be used instead.

```sh
$ cargo run --release --bin client localhost:3000
> Welcome to Sundris Auction House, stranger! How can I call you?
Stepan
> Successfully logged in as Stepan
help
> Available commands:
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
      - immediate sell order - will be executed immediately once someone buys it. Otherwise will expire in 5 minutes and
        item will be returned to the seller, but not the fee, which is `5% of the price + 1` funds
      - auction sell order - will be executed once it expires if someone placed a bid on it
    - buy: Executes immediate sell order or places a bid on a auction sell order. Format: 'buy <sell_order_id> [<bid>]'
      - no bid - executes immediate sell order
      - bid - places a bid on a auction sell order

    Usage: <command> [<args>], where `[]` annotates optional argumet(s)

```

## License

All code in this project is dual-licensed under either:

- [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0) ([`LICENSE-APACHE`](LICENSE-APACHE))
- [MIT license](https://opensource.org/licenses/MIT) ([`LICENSE-MIT`](LICENSE-MIT))

at your option.
