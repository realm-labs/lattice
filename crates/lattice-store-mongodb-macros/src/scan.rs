use quote::quote;
use syn::{DeriveInput, LitStr, Type};

use crate::common::{require_named_struct, store_crate_path};

#[derive(Clone, Copy)]
enum Strategy {
    Whole,
    Map,
}

struct ScanField<'a> {
    ident: &'a syn::Ident,
    path: String,
    strategy: Strategy,
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let store = store_crate_path()?;
    let fields = require_named_struct(input)?;
    let fields = fields
        .named
        .iter()
        .filter_map(|field| match scan_field(field) {
            Ok(value) => value.map(Ok),
            Err(error) => Some(Err(error)),
        })
        .collect::<syn::Result<Vec<_>>>()?;
    let ident = &input.ident;

    let captures = fields.iter().map(|field| {
        let rust = field.ident;
        let path = &field.path;
        match field.strategy {
            Strategy::Whole => quote! {
                snapshot = snapshot.capture_value(#path, &self.#rust)?;
            },
            Strategy::Map => quote! {
                snapshot = snapshot.capture_map_entries(#path, self.#rust.iter())?;
            },
        }
    });
    let scans = fields.iter().enumerate().map(|(index, field)| {
        let rust = field.ident;
        let path = &field.path;
        match field.strategy {
            Strategy::Whole => quote! {
                scan.whole_value(#index, #path, &self.#rust)?;
            },
            Strategy::Map => quote! {
                scan.map_entries(#index, #path, self.#rust.iter())?;
            },
        }
    });

    Ok(quote! {
        impl #store::scan::MongoScan for #ident {
            fn capture(&self) -> Result<#store::scan::ScanSnapshot, #store::scan::ScanError> {
                let mut snapshot = #store::scan::ScanSnapshot::empty();
                #(#captures)*
                Ok(snapshot)
            }

            fn diff(
                &self,
                baseline: &#store::scan::ScanSnapshot,
                cursor: #store::scan::ScanCursor,
                budget: &mut #store::scan::ScanBudget,
            ) -> Result<#store::scan::ScanDelta, #store::scan::ScanError> {
                let mut scan = #store::scan::ScanBuilder::new(baseline, cursor, budget);
                #(#scans)*
                Ok(scan.finish())
            }
        }
    })
}

fn scan_field(field: &syn::Field) -> syn::Result<Option<ScanField<'_>>> {
    let ident = field.ident.as_ref().expect("named field");
    let mut skipped = false;
    let mut rename = None;
    let mut override_strategy = None;
    for attribute in &field.attrs {
        if attribute.path().is_ident("serde") {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("skip") {
                    skipped = true;
                    Ok(())
                } else if meta.path.is_ident("rename") {
                    rename = Some(meta.value()?.parse::<LitStr>()?.value());
                    Ok(())
                } else if meta.path.is_ident("default") && !meta.input.is_empty() {
                    let _ = meta.value()?.parse::<LitStr>()?;
                    Ok(())
                } else if !meta.input.is_empty() {
                    let _ = meta.value()?.parse::<syn::Expr>()?;
                    Ok(())
                } else {
                    Ok(())
                }
            })?;
        } else if attribute.path().is_ident("mongo") {
            attribute.parse_nested_meta(|meta| {
                if meta.path.is_ident("id") || meta.path.is_ident("scan_ignore") {
                    skipped = true;
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
    }
    if skipped {
        return Ok(None);
    }
    Ok(Some(ScanField {
        ident,
        path: rename.unwrap_or_else(|| ident.to_string()),
        strategy: override_strategy.unwrap_or_else(|| infer_strategy(&field.ty)),
    }))
}

fn infer_strategy(ty: &Type) -> Strategy {
    let Type::Path(path) = ty else {
        return Strategy::Whole;
    };
    match path
        .path
        .segments
        .last()
        .map(|segment| segment.ident.to_string())
    {
        Some(name) if name == "BTreeMap" || name == "HashMap" => Strategy::Map,
        _ => Strategy::Whole,
    }
}
