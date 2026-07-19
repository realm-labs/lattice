use quote::quote;
use syn::{DeriveInput, Ident, LitStr, Type};

use crate::common::{require_named_struct, store_crate_path};

struct DocumentOptions {
    collection: LitStr,
    id_field: Ident,
    id_type: Type,
    serialized_id_field: String,
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let options = document_options(input)?;
    let store = store_crate_path()?;
    let ident = &input.ident;
    let collection = options.collection;
    let id_field = options.id_field;
    let id_type = options.id_type;
    let serialized_id_field = options.serialized_id_field;
    Ok(quote! {
        impl #store::document::MongoDocument for #ident {
            type Id = #id_type;
            const COLLECTION: &'static str = #collection;
            const ID_FIELD: &'static str = #serialized_id_field;

            fn id(&self) -> &Self::Id {
                &self.#id_field
            }
        }
    })
}

fn document_options(input: &DeriveInput) -> syn::Result<DocumentOptions> {
    let mut collection = None;
    for attribute in &input.attrs {
        if !attribute.path().is_ident("mongo") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("collection") {
                collection = Some(meta.value()?.parse::<LitStr>()?);
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo document option"))
            }
        })?;
    }
    let fields = require_named_struct(input)?;
    let mut identity = None;
    for field in &fields.named {
        if !is_document_id_field(field)? {
            continue;
        }
        if identity.is_some() {
            return Err(syn::Error::new_spanned(
                field,
                "MongoDocument requires exactly one #[mongo(id)] field",
            ));
        }
        let ident = field.ident.clone().expect("named field");
        let serialized = serialized_field_name(field)?;
        identity = Some((ident, field.ty.clone(), serialized));
    }
    let (id_field, id_type, serialized_id_field) = identity.ok_or_else(|| {
        syn::Error::new_spanned(
            input,
            "MongoDocument requires exactly one field annotated with #[mongo(id)]",
        )
    })?;
    if matches!(
        serialized_id_field.as_str(),
        "_id" | "version" | "updated_at_ms"
    ) {
        return Err(syn::Error::new_spanned(
            &id_field,
            "Mongo identity field cannot serialize as a reserved storage field",
        ));
    }
    Ok(DocumentOptions {
        collection: collection.ok_or_else(|| {
            syn::Error::new_spanned(input, "missing #[mongo(collection = \"...\")] option")
        })?,
        id_field,
        id_type,
        serialized_id_field,
    })
}

fn is_document_id_field(field: &syn::Field) -> syn::Result<bool> {
    let mut identity = false;
    for attribute in &field.attrs {
        if !attribute.path().is_ident("mongo") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                if !meta.input.is_empty() {
                    return Err(meta.error("#[mongo(id)] does not accept a value"));
                }
                identity = true;
                Ok(())
            } else if meta.path.is_ident("scan_ignore") || meta.path.is_ident("scan") {
                if !meta.input.is_empty() {
                    let _ = meta.value()?.parse::<syn::Expr>()?;
                }
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo document field option"))
            }
        })?;
    }
    Ok(identity)
}

fn serialized_field_name(field: &syn::Field) -> syn::Result<String> {
    let ident = field.ident.as_ref().expect("named field");
    let mut name = ident.to_string();
    for attribute in &field.attrs {
        if !attribute.path().is_ident("serde") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                name = meta.value()?.parse::<LitStr>()?.value();
                Ok(())
            } else if meta.path.is_ident("skip")
                || meta.path.is_ident("skip_serializing")
                || meta.path.is_ident("skip_deserializing")
            {
                Err(meta.error("Mongo identity field cannot be skipped by serde"))
            } else if !meta.input.is_empty() {
                let _ = meta.value()?.parse::<syn::Expr>()?;
                Ok(())
            } else {
                Ok(())
            }
        })?;
    }
    Ok(name)
}
