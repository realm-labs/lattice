//! Derives for typed Mongo documents and actor-local persistence scans.

use proc_macro::TokenStream;
use syn::{DeriveInput, parse_macro_input};

mod common;
mod document;
mod document_set;
mod scan;

#[proc_macro_derive(MongoDocument, attributes(mongo, serde))]
pub fn derive_mongo_document(input: TokenStream) -> TokenStream {
    document::expand(&parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(MongoScan, attributes(mongo, serde))]
pub fn derive_mongo_scan(input: TokenStream) -> TokenStream {
    scan::expand(&parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

#[proc_macro_derive(MongoDocumentSet, attributes(mongo))]
pub fn derive_mongo_document_set(input: TokenStream) -> TokenStream {
    document_set::expand(&parse_macro_input!(input as DeriveInput))
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}
