use quote::quote;
use syn::{DeriveInput, Ident, LitStr, Type};

use crate::common::{
    require_named_struct, serde_field_shape, serde_serialize_rename_all, store_crate_path,
};

struct DocumentOptions {
    collection: LitStr,
    conflict_policy: ConflictPolicyOption,
    id_field: Ident,
    id_type: Type,
    serialized_id_field: String,
}

enum ConflictPolicyOption {
    Block,
    Quarantine,
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let options = document_options(input)?;
    let store = store_crate_path()?;
    let ident = &input.ident;
    let collection = options.collection;
    let conflict_policy = match options.conflict_policy {
        ConflictPolicyOption::Block => quote! {
            #store::persistence::coordinator::ConflictPolicy::BlockCoordinator
        },
        ConflictPolicyOption::Quarantine => quote! {
            #store::persistence::coordinator::ConflictPolicy::QuarantineDocument
        },
    };
    let id_field = options.id_field;
    let id_type = options.id_type;
    let serialized_id_field = options.serialized_id_field;
    Ok(quote! {
        impl #store::document::MongoDocument for #ident {
            type Id = #id_type;
            const COLLECTION: &'static str = #collection;
            const ID_FIELD: &'static str = #serialized_id_field;
            const CONFLICT_POLICY: #store::persistence::coordinator::ConflictPolicy =
                #conflict_policy;

            fn id(&self) -> &Self::Id {
                &self.#id_field
            }
        }
    })
}

fn document_options(input: &DeriveInput) -> syn::Result<DocumentOptions> {
    let mut collection = None;
    let mut conflict_policy = None;
    for attribute in &input.attrs {
        if !attribute.path().is_ident("mongo") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("collection") {
                collection = Some(meta.value()?.parse::<LitStr>()?);
                Ok(())
            } else if meta.path.is_ident("conflict") {
                let value = meta.value()?.parse::<LitStr>()?;
                conflict_policy = Some(match value.value().as_str() {
                    "block" => ConflictPolicyOption::Block,
                    "quarantine" => ConflictPolicyOption::Quarantine,
                    _ => {
                        return Err(syn::Error::new_spanned(
                            value,
                            "Mongo conflict policy must be `block` or `quarantine`",
                        ));
                    }
                });
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo document option"))
            }
        })?;
    }
    let fields = require_named_struct(input)?;
    let rename_all = serde_serialize_rename_all(input)?;
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
        let serde = serde_field_shape(field, rename_all)?;
        if serde.skipped || serde.flattened {
            return Err(syn::Error::new_spanned(
                field,
                "Mongo identity field cannot be skipped or flattened by serde",
            ));
        }
        let serialized = serde.serialized_name;
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
        conflict_policy: conflict_policy.unwrap_or(ConflictPolicyOption::Block),
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
            } else if meta.path.is_ident("adapter") {
                let _ = meta.value()?.parse::<syn::Path>()?;
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo document field option"))
            }
        })?;
    }
    Ok(identity)
}
