use quote::quote;
use syn::{DeriveInput, LitStr};

use crate::common::{
    require_named_struct, serde_field_shape, serde_serialize_rename_all, store_crate_path,
};

#[derive(Clone, Copy)]
enum Strategy {
    Whole,
    Map,
    Ignore,
}

struct ScanPolicy {
    path: String,
    strategy: Strategy,
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let store = store_crate_path()?;
    let fields = require_named_struct(input)?;
    let rename_all = serde_serialize_rename_all(input)?;
    let policies = fields
        .named
        .iter()
        .filter_map(|field| match scan_policy(field, rename_all) {
            Ok(value) => value.map(Ok),
            Err(error) => Some(Err(error)),
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let ident = &input.ident;
    let policies = policies
        .iter()
        .map(|policy| {
            let path = &policy.path;
            match policy.strategy {
                Strategy::Map => quote! { #store::scan::ScanFieldPolicy::map(#path) },
                Strategy::Ignore => quote! { #store::scan::ScanFieldPolicy::ignore(#path) },
                Strategy::Whole => unreachable!("whole fields use the BSON-level default"),
            }
        })
        .collect::<Vec<_>>();

    Ok(quote! {
        impl #store::scan::MongoScan for #ident {
            fn capture(&self) -> Result<#store::scan::ScanSnapshot, #store::scan::ScanError> {
                #store::scan::ScanSnapshot::empty().capture_document(
                    self,
                    &[#(#policies),*],
                )
            }

            fn capture_bson(
                document: &#store::scan::BsonDocument,
            ) -> Result<#store::scan::ScanSnapshot, #store::scan::ScanError> {
                #store::scan::ScanSnapshot::empty().capture_bson_document(
                    document,
                    &[#(#policies),*],
                )
            }

            fn diff(
                &self,
                baseline: &#store::scan::ScanSnapshot,
                cursor: #store::scan::ScanCursor,
                budget: &mut #store::scan::ScanBudget,
            ) -> Result<#store::scan::ScanDelta, #store::scan::ScanError> {
                let mut scan = #store::scan::ScanBuilder::new(baseline, cursor, budget);
                scan.document(self, &[#(#policies),*])?;
                Ok(scan.finish())
            }
        }
    })
}

fn scan_policy(
    field: &syn::Field,
    rename_all: Option<crate::common::RenameRule>,
) -> syn::Result<Option<ScanPolicy>> {
    let mut identity = false;
    let mut scan_ignore = false;
    let mut override_strategy = None;
    for attribute in &field.attrs {
        if !attribute.path().is_ident("mongo") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                identity = true;
                Ok(())
            } else if meta.path.is_ident("scan_ignore") {
                scan_ignore = true;
                Ok(())
            } else if meta.path.is_ident("scan") {
                let value = meta.value()?.parse::<LitStr>()?;
                override_strategy = Some(match value.value().as_str() {
                    "whole" => Strategy::Whole,
                    "map" => Strategy::Map,
                    _ => return Err(meta.error("scan must be `whole` or `map`")),
                });
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo scan option"))
            }
        })?;
    }
    if identity {
        return Ok(None);
    }

    let serde = serde_field_shape(field, rename_all)?;
    if serde.skipped {
        return Ok(None);
    }
    if serde.flattened {
        if scan_ignore || override_strategy.is_some() {
            return Err(syn::Error::new_spanned(
                field,
                "flattened fields cannot use Mongo scan overrides because they do not have one BSON field path",
            ));
        }
        return Ok(None);
    }

    let strategy = if scan_ignore {
        Strategy::Ignore
    } else {
        override_strategy.unwrap_or(Strategy::Whole)
    };
    if matches!(strategy, Strategy::Whole) {
        return Ok(None);
    }
    Ok(Some(ScanPolicy {
        path: serde.serialized_name,
        strategy,
    }))
}
