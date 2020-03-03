use crate::message::Message;

/// Represents a player.
#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Player {
    // TODO(mkiefel): remove visibility of members.
    /// Represents the combined belief of the skill of this player.
    skill: Message,
    /// The point in time when the above skill was estimated.
    datetime: chrono::DateTime<chrono::Utc>,
}

impl Default for Player {
    fn default() -> Self {
        Player {
            skill: Message::from_mu_sigma2(Player::default_mean(), Player::default_sigma().powi(2)),
            // Not super ideal, but we just pick something really long ago.
            datetime: chrono::DateTime::<chrono::Utc>::from_utc(
                chrono::NaiveDateTime::from_timestamp(0, 0),
                chrono::Utc,
            ),
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
        let delta = (-(time_delta.num_milliseconds() as f64)
            / (Self::default_length_scale().num_milliseconds() as f64))
            .exp();
        let (mu, sigma2) = self.skill.to_mu_sigma2();
        Some(Message::from_mu_sigma2(
            (mu - Self::default_mean()) * delta + Self::default_mean(),
            Self::default_sigma().powi(2) * (1.0 - delta.powi(2)) + delta.powi(2) * sigma2,
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

    fn default_length_scale() -> chrono::Duration {
        chrono::Duration::days(90)
    }
}
