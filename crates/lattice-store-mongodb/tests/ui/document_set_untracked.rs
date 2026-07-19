use lattice_store_mongodb::MongoDocumentSet;

#[derive(MongoDocumentSet)]
#[mongo(id = u64)]
struct InvalidDocuments {
    value: String,
}

fn main() {}
