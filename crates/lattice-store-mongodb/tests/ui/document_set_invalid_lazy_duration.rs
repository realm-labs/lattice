use lattice_store_mongodb::MongoDocumentSet;

#[derive(MongoDocumentSet)]
#[mongo(id = u64)]
struct InvalidDocuments {
    #[mongo(lazy_unload = "ten minutes")]
    value: String,
}

fn main() {}
