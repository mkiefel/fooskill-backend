use serde::{Deserialize, Serialize};

/// Gaussian message.
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct Message {
    pub pi: f64,
    pub tau: f64,
}

impl Message {
    /// Initializes a Message from the regular Gaussian parameters.
    pub fn from_mu_sigma2(mu: f64, sigma2: f64) -> Message {
        Message {
            pi: 1.0 / sigma2,
            tau: mu / sigma2,
        }
    }

    /// Returns the regular Gaussian parameters for a message.
    pub fn to_mu_sigma2(&self) -> (f64, f64) {
        let sigma2 = 1.0 / self.pi;
        let mu = self.tau * sigma2;
        (mu, sigma2)
    }

    /// Includes the belief of the other message into this one.
    pub fn include(&self, rhs: &Message) -> Message {
        Message {
            pi: self.pi + rhs.pi,
            tau: self.tau + rhs.tau,
        }
    }

    /// Removes the belief of the other message from this one.
    pub fn exclude(&self, rhs: &Message) -> Message {
        Message {
            pi: self.pi - rhs.pi,
            tau: self.tau - rhs.tau,
        }
    }
}
