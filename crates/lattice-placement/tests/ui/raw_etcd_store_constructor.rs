use lattice_placement::storage::PlacementPrefix;
use lattice_placement::storage::etcd::EtcdPlacementStore;
use lattice_placement::storage::etcd::client::RealEtcdClient;

fn retain_raw_client(client: RealEtcdClient) {
    let retained = client.clone();
    let _store = EtcdPlacementStore::new(PlacementPrefix::new("/lattice/test"), client);
    let _ = retained;
}

fn main() {}
