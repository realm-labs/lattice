use std::{
    collections::BTreeSet,
    sync::{Mutex, OnceLock},
};

use lattice_core::actor_ref::NodeAddress;
use tokio::{
    net::TcpListener,
    sync::{Mutex as AsyncMutex, MutexGuard},
};

static NETWORK_TEST: OnceLock<AsyncMutex<()>> = OnceLock::new();
static ALLOCATED_PORTS: OnceLock<Mutex<BTreeSet<u16>>> = OnceLock::new();

pub async fn network_test_guard() -> MutexGuard<'static, ()> {
    NETWORK_TEST
        .get_or_init(|| AsyncMutex::new(()))
        .lock()
        .await
}

pub async fn unused_address() -> NodeAddress {
    loop {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let allocated = ALLOCATED_PORTS.get_or_init(|| Mutex::new(BTreeSet::new()));
        let unique = allocated
            .lock()
            .expect("test port registry poisoned")
            .insert(port);
        drop(listener);
        if unique {
            return NodeAddress::new("127.0.0.1", port).unwrap();
        }
    }
}
