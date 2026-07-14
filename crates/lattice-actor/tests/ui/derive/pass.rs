use lattice_actor::traits::{Message, Request};

#[derive(lattice_actor::Message)]
struct GenericTell<T>(T)
where
    T: Clone;

#[derive(lattice_actor::Message)]
enum Event {
    Started,
}

#[derive(lattice_actor::Request)]
#[request(response = Option<T>)]
struct GenericAsk<T>(T);

#[derive(lattice_actor::Request)]
#[request(response = std::result::Result<Vec<u8>, std::io::Error>)]
struct QualifiedAsk;

#[derive(Clone, PartialEq, prost::Message, lattice_actor::Message)]
struct WireTell {
    #[prost(uint64, tag = "1")]
    value: u64,
}

fn assert_message<T: Message>() {}

fn assert_request<T, R>()
where
    T: Request<Response = R>,
    R: Send + 'static,
{
}

fn main() {
    assert_message::<GenericTell<String>>();
    assert_message::<Event>();
    assert_message::<WireTell>();
    assert_request::<GenericAsk<String>, Option<String>>();
    assert_request::<QualifiedAsk, Result<Vec<u8>, std::io::Error>>();
}
