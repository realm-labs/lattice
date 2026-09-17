use lattice_actor::actor_behavior;

enum State {
    Idle,
}

struct Ping;

actor_behavior! {
    State {
        State::Idle => [Ping];
        State::Idle => [Ping];
    }
}

fn main() {}
