use crate::message::Message;

/// Represents a player.
#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Player {
    // TODO(mkiefel): remove visibility of members.
    /// Represents the combined belief of the skill of this player.
    skill: Message,
}

impl Default for Player {
    fn default() -> Self {
        Player {
            skill: Message::from_mu_sigma2(Player::default_mean(), Player::default_sigma().powi(2)),
        }
    }
}

impl Player {
    pub fn skill(&self) -> &Message {
        &self.skill
    }

    pub fn set_skill(&mut self, skill: Message) {
        self.skill = skill;
    }

    pub fn default_mean() -> f64 {
        25.0
    }

    pub fn default_sigma() -> f64 {
        Player::default_mean() / 3.0
    }
}
