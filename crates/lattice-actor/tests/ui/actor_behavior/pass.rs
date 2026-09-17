use lattice_actor::{Message, actor_behavior};

#[derive(Default)]
enum State {
    #[default]
    Idle,
    Paused,
}

#[derive(Message)]
struct Ping;

actor_behavior! {
    State {
        State::Idle | State::Paused => [Ping];
    }
}

fn main() {}
