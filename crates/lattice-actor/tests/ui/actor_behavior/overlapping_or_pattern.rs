use lattice_actor::actor_behavior;

enum State {
    Idle,
    Paused,
}

struct Ping;

actor_behavior! {
    State {
        State::Idle | State::Paused => [Ping];
        State::Idle => [Ping];
    }
}

fn main() {}
