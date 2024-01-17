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
}

impl Storage {
    pub(crate) fn open(path: &str) -> Result<Self> {
        let db = rusqlite::Connection::open(path)?;
        db.execute(
            "CREATE TABLE IF NOT EXISTS users (
                id INTEGER PRIMARY KEY,
                username TEXT NOT NULL UNIQUE
            ) STRICT",
            (),
        )?;

        Ok(Self { db })
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
                self.db.last_insert_rowid()
            }
            Err(err) => Err(err)?,
        };

        Ok(User {
            id: UserId(user_id),
            username: username.to_owned(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_login() {
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
}
