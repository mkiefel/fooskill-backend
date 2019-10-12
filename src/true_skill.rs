use std::f64;

pub enum GameResult {
    Won,
    Draw,
    Lost,
}

/// Gaussian message.
#[derive(Serialize, Deserialize, Clone, Copy, Debug)]
pub struct Message {
    pi: f64,
    tau: f64,
}

impl Message {
    /// Initializes a Message from the regular Gaussian parameters.
    fn from_mu_sigma2(mu: f64, sigma2: f64) -> Message {
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

/// Represents a player.
#[derive(Serialize, Clone, Deserialize, Debug)]
pub struct Player {
    /// Represents the combined belief of the skill of this player.
    pub skill: Message,
}

impl Player {
    pub fn new() -> Player {
        Player {
            skill: Message::from_mu_sigma2(Player::default_mean(), Player::default_sigma2()),
        }
    }

    pub fn default_mean() -> f64 {
        25.0
    }

    pub fn default_sigma() -> f64 {
        Player::default_mean() / 3.0
    }

    pub fn default_sigma2() -> f64 {
        Player::default_sigma().powi(2)
    }
}

/// Implements the TrueSkill ranking algorithm.
pub struct TrueSkill {
    beta: f64,
    eps: f64,
}

impl TrueSkill {
    pub fn new(default_sigma: f64) -> TrueSkill {
        // eps set by
        //   0.2166588675713617 = 2 * normcdf(eps / (sqrt 2 * ((25.0 / 3.0) / 2.0))) - 1
        // >> norminv(1.2166588675713617 / 2)
        // ans =
        //   0.2750
        let beta = default_sigma / 2.0;
        TrueSkill {
            beta,
            eps: 0.2750 * 2.0f64.sqrt() * beta,
        }
    }

    fn from_skill(&self, skill: &Message) -> Message {
        let c2 = self.beta.powi(2);
        let a = 1.0 / (1.0 + c2 * skill.pi);
        Message {
            pi: a * skill.pi,
            tau: a * skill.tau,
        }
    }

    fn weighted_pass(weighted_messages: &[(f64, Message)]) -> Message {
        // TODO(mkiefel): this could potentially also a fold and take an iterator as
        // input.
        let pi = 1.0
            / (weighted_messages
                .iter()
                .map(|(weight, message)| weight.powi(2) / message.pi)
                .sum::<f64>());
        let tau = pi
            * weighted_messages
                .iter()
                .map(|(weight, message)| weight * message.tau / message.pi)
                .sum::<f64>();
        Message { pi, tau }
    }

    fn from_performance(messages: &[Message]) -> Message {
        TrueSkill::weighted_pass(
            &messages
                .iter()
                .map(|message| (1.0, *message))
                .collect::<Vec<(f64, Message)>>(),
        )
    }

    fn to_difference(left: Message, right: Message) -> Message {
        let difference_messages = [(1.0, left), (-1.0, right)];
        TrueSkill::weighted_pass(&difference_messages)
    }

    fn norm_pdf(x: f64) -> f64 {
        (-0.5 * x.powi(2)).exp() / (2.0 * f64::consts::PI).sqrt()
    }

    fn norm_cdf(x: f64) -> f64 {
        0.5 * (1.0 + libm::erf(x / 2.0_f64.sqrt()))
    }

    fn difference_marginal_won(&self, message: &Message) -> Message {
        fn v(t: f64, eps: f64) -> f64 {
            TrueSkill::norm_pdf(t - eps) / TrueSkill::norm_cdf(t - eps)
        }

        fn w(t: f64, eps: f64) -> f64 {
            let v_value = v(t, eps);
            v_value * (v_value + t - eps)
        }

        self.difference_marginal(v, w, message)
    }

    fn difference_marginal_draw(&self, message: &Message) -> Message {
        fn v(t: f64, eps: f64) -> f64 {
            (TrueSkill::norm_pdf(-eps - t) - TrueSkill::norm_pdf(eps - t))
                / (TrueSkill::norm_cdf(eps - t) - TrueSkill::norm_cdf(-eps - t))
        }

        fn w(t: f64, eps: f64) -> f64 {
            let v_value = v(t, eps);
            v_value.powi(2)
                + ((eps - t) * TrueSkill::norm_pdf(eps - t)
                    + (eps + t) * TrueSkill::norm_pdf(eps + t))
                    / (TrueSkill::norm_cdf(eps - t) - TrueSkill::norm_cdf(-eps - t))
        }

        self.difference_marginal(v, w, message)
    }

    fn difference_marginal(
        &self,
        v: fn(f64, f64) -> f64,
        w: fn(f64, f64) -> f64,
        message: &Message,
    ) -> Message {
        let c = message.pi;
        let d = message.tau;

        let sqrt_c = c.sqrt();

        let v_value = v(d / sqrt_c, self.eps * sqrt_c);
        let w_value = 1.0 - w(d / sqrt_c, self.eps * sqrt_c);

        Message {
            pi: c / w_value,
            tau: (d + sqrt_c * v_value) / w_value,
        }
    }

    fn from_difference(
        left_message: Message,
        right_message: Message,
        to_difference_message: Message,
    ) -> (Message, Message) {
        let left_messages = [(1.0, right_message), (1.0, to_difference_message)];
        let right_messages = [(1.0, left_message), (-1.0, to_difference_message)];
        (
            TrueSkill::weighted_pass(&left_messages),
            TrueSkill::weighted_pass(&right_messages),
        )
    }

    fn to_performance(
        from_performance_messages: &[Message],
        update_message: &Message,
    ) -> Vec<Message> {
        let mut weighted_messages = from_performance_messages
            .iter()
            .map(|message| (-1.0, *message))
            .collect::<Vec<(f64, Message)>>();
        let mut out_messages: Vec<Message> =
            vec![Message { pi: 0.0, tau: 0.0 }; from_performance_messages.len()];
        for i in 0..weighted_messages.len() {
            weighted_messages[i].0 = 1.0;
            weighted_messages[i].1 = *update_message;
            out_messages[i] = TrueSkill::weighted_pass(&weighted_messages);
            weighted_messages[i].0 = -1.0;
            weighted_messages[i].1 = from_performance_messages[i];
        }
        return out_messages;
    }

    fn to_skill(&self, message: &Message) -> Message {
        self.from_skill(message)
    }

    /// Passes all input team messages down the message tree and returns the
    /// message update for each player.
    pub fn tree_pass(
        &self,
        left_team: &[Message],
        right_team: &[Message],
        result: GameResult,
    ) -> (Vec<Message>, Vec<Message>) {
        if let GameResult::Lost = result {
            let result = self.tree_pass(right_team, left_team, GameResult::Won);
            return (result.1, result.0);
        }

        let left_performances = left_team
            .iter()
            .map(|message| self.from_skill(message))
            .collect::<Vec<_>>();

        let right_performances = right_team
            .iter()
            .map(|message| self.from_skill(message))
            .collect::<Vec<_>>();

        let left_performance = TrueSkill::from_performance(&left_performances);
        let right_performance = TrueSkill::from_performance(&right_performances);

        let to_difference_message =
            TrueSkill::to_difference(left_performance, right_performance);
        let marginal = match result {
            GameResult::Won => self.difference_marginal_won(&to_difference_message),
            GameResult::Draw => self.difference_marginal_draw(&to_difference_message),
            _ => panic!("cannot have Lost here"),
        };

        let from_difference_message = TrueSkill::from_difference(
            left_performance,
            right_performance,
            marginal.exclude(&to_difference_message),
        );

        let left_skills = TrueSkill::to_performance(&left_performances, &from_difference_message.0)
            .iter()
            .map(|message| self.to_skill(message))
            .collect::<Vec<_>>();
        let right_skills =
            TrueSkill::to_performance(&right_performances, &from_difference_message.1)
                .iter()
                .map(|message| self.to_skill(message))
                .collect::<Vec<_>>();
        (left_skills, right_skills)
    }
}
