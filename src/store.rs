use rocket_contrib::databases::{r2d2, DatabaseConfig, DbError, Poolable};

use crate::skill_base::Connection;

// Thin wrapper around r2d2_redis to implement Poolable for a newer version of
// redis-rs.
pub struct ConnectionManager(r2d2_redis::RedisConnectionManager);

impl r2d2::ManageConnection for ConnectionManager {
    type Connection = Connection;
    type Error = r2d2_redis::Error;

    fn connect(&self) -> Result<Self::Connection, Self::Error> {
        self.0.connect().map(Connection::new)
    }

    fn is_valid(&self, con: &mut Self::Connection) -> Result<(), Self::Error> {
        self.0.is_valid(&mut con.con())
    }

    fn has_broken(&self, con: &mut Self::Connection) -> bool {
        self.0.has_broken(&mut con.con())
    }
}

impl Poolable for Connection {
    type Manager = ConnectionManager;
    type Error = DbError<redis::RedisError>;

    fn pool(config: DatabaseConfig) -> Result<r2d2::Pool<Self::Manager>, Self::Error> {
        let manager =
            r2d2_redis::RedisConnectionManager::new(config.url).map_err(DbError::Custom)?;
        r2d2::Pool::builder()
            .max_size(config.pool_size)
            .build(ConnectionManager(manager))
            .map_err(DbError::PoolError)
    }
}

#[database("fooskill")]
pub struct Store(Connection);
