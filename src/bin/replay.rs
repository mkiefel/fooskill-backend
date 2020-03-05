use std::convert::TryInto;
use std::env;

use fooskill::skill_base;

#[macro_use]
extern crate serde_derive;

use std::error::Error;
use std::fs::File;
use std::io::Read;

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

fn main() -> Result<(), Box<dyn Error>> {
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

    let client = redis::Client::open("redis://127.0.0.1/")?;
    let con = client.get_connection()?;
    let mut connection = skill_base::Connection::new(con);

    let group_key = skill_base::GroupKey::new(group_key.to_owned()).unwrap();
    let group_id = skill_base::decode_and_validate_group_id(
        &group_key,
        percent_encoding::percent_decode_str(secret_group_id)
            .decode_utf8()?
            .into_owned(),
    )?;

    for user in snaphot.users {
        skill_base::create_user(&mut connection, &group_id, &user.id.into(), &user.name)?;
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
        )?;
    }

    Ok(())
}
