use rocket_db_pools::{deadpool_redis, Database};

#[derive(Database)]
#[database("fooskill")]
pub struct Store(deadpool_redis::Pool);
