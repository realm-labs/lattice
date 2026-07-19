use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident};

pub(crate) fn store_crate_path() -> syn::Result<proc_macro2::TokenStream> {
    match crate_name("lattice-store-mongodb") {
        Ok(FoundCrate::Itself) => Ok(quote!(::lattice_store_mongodb)),
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            Ok(quote!(::#ident))
        }
        Err(error) => Err(syn::Error::new(
            Span::call_site(),
            format!("could not resolve the `lattice-store-mongodb` crate: {error}"),
        )),
    }
}

pub(crate) fn require_named_struct(input: &DeriveInput) -> syn::Result<&syn::FieldsNamed> {
    let Data::Struct(data) = &input.data else {
        return Err(syn::Error::new_spanned(
            input,
            "Mongo persistence derives support structs only",
        ));
    };
    let Fields::Named(fields) = &data.fields else {
        return Err(syn::Error::new_spanned(
            input,
            "Mongo persistence derives require named fields",
        ));
    };
    Ok(fields)
}
