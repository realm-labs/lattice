use lattice_placement::storage::etcd::client::RealEtcdClient;

fn delete_floor(client: &RealEtcdClient) {
    let _ = client.delete("/lattice/test/authority/epoch_floors/v1/actors/World/World/u64:7");
}

fn main() {}
