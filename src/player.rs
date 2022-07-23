use serde::{Deserialize, Serialize};

use crate::message::Message;

/// Represents a player.
#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Player {
    /// Represents the combined belief of the skill of this player.
    skill: Message,
    /// The point in time when the above skill was estimated.
    datetime: chrono::DateTime<chrono::Utc>,
}

impl Default for Player {
    fn default() -> Self {
        Player {
            skill: Message::from_mu_sigma2(Player::default_mean(), Player::default_sigma().powi(2)),
            datetime: chrono::Utc::now(),
        }
    }
}

impl Player {
    pub fn skill_at(&self, query: &chrono::DateTime<chrono::Utc>) -> Option<Message> {
        let time_delta = *query - self.datetime;
        // The temporal model can only look into the future. Fail here, whenever
        // this gets queried for something clearly in the past.
        if time_delta < chrono::Duration::zero() {
            return None;
        }
        let (mu, sigma2) = self.skill.to_mu_sigma2();
        Some(Message::from_mu_sigma2(
            mu,
            sigma2 + Self::default_sigma2_change_speed() * (time_delta.num_seconds() as f64),
        ))
    }

    pub fn set_skill(&mut self, skill: Message, datetime: chrono::DateTime<chrono::Utc>) {
        self.skill = skill;
        self.datetime = datetime;
    }

    pub fn default_mean() -> f64 {
        25.0
    }

    pub fn default_sigma() -> f64 {
        Player::default_mean() / 3.0
    }

    /// Speed at which sigma2 increases per second.
    fn default_sigma2_change_speed() -> f64 {
        20.0 / (chrono::Duration::days(90).num_seconds() as f64)
    }
}
