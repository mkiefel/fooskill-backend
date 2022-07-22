use std::convert::TryInto;
use std::env;
use std::fs::File;
use std::io::Read;

use rocket::tokio;
use rocket_db_pools::deadpool_redis::{Config, Runtime};

use serde::Deserialize;

use fooskill::skill_base;

#[derive(Deserialize, Debug)]
struct Snapshot {
    games: Vec<Game>,
    users: Vec<User>,
}

#[derive(Deserialize, Debug)]
struct User {
    id: String,
    name: String,
}

#[derive(Deserialize, Debug)]
struct Game {
    id: String,
    winner_ids: Vec<String>,
    loser_ids: Vec<String>,
    timestamp: u128,
}

async fn go() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();

    let input_path = &args[1];
    let secret_group_id = &args[2];
    let group_key = &args[3];

    // Read the input file to string.
    let mut file = File::open(input_path)?;
    let mut contents = String::new();
    file.read_to_string(&mut contents)?;

    // Deserialize and print Rust data structure.
    let snaphot: Snapshot = serde_json::from_str(&contents)?;

    let cfg = Config::from_url("redis://127.0.0.1/");
    let pool = cfg.create_pool(Some(Runtime::Tokio1)).unwrap();

    let mut connection = pool.get().await?;

    let group_key = skill_base::GroupKey::new(group_key.to_owned()).unwrap();
    let group_id = skill_base::decode_and_validate_group_id(
        &group_key,
        percent_encoding::percent_decode_str(secret_group_id)
            .decode_utf8()?
            .into_owned(),
    )?;

    for user in snaphot.users {
        skill_base::create_user(&mut connection, &group_id, &user.id.into(), &user.name).await?;
    }
    for game in snaphot.games {
        let datetime = chrono::DateTime::<chrono::Utc>::from_utc(
            chrono::NaiveDateTime::from_timestamp(
                (game.timestamp / 1000).try_into().unwrap(),
                (game.timestamp % 1000 * 1_000_000).try_into().unwrap(),
            ),
            chrono::Utc,
        );

        skill_base::create_game(
            &mut connection,
            &group_id,
            &game.id.into(),
            &game
                .winner_ids
                .into_iter()
                .map(skill_base::UserId::from)
                .collect::<Vec<_>>(),
            &game
                .loser_ids
                .into_iter()
                .map(skill_base::UserId::from)
                .collect::<Vec<_>>(),
            datetime,
        )
        .await?;
    }

    Ok(())
}

#[tokio::main]
async fn main() {
    go().await.unwrap();
}
