use proc_macro2::TokenStream;
use quote::{format_ident, quote};
use syn::{DeriveInput, Ident, LitStr, Type};

use crate::common::{
    SerdeFieldShape, require_named_struct, serde_crate_path, serde_field_shape,
    serde_serialize_rename_all, store_crate_path,
};

#[derive(Clone, Copy, PartialEq, Eq)]
enum Strategy {
    Whole,
    Map,
    Flatten,
    Ignore,
}

struct ScanField {
    ident: Ident,
    ty: Type,
    path: String,
    strategy: Strategy,
    adapter: Option<syn::Path>,
    field_index: Option<usize>,
    serde: SerdeFieldShape,
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<TokenStream> {
    let store = store_crate_path()?;
    let serde_crate = serde_crate_path()?;
    let fields = require_named_struct(input)?;
    let rename_all = serde_serialize_rename_all(input)?;
    let mut scan_fields = Vec::new();
    let mut next_field_index = 0;
    let mut flatten_field = None;
    for field in &fields.named {
        let Some(mut field) = scan_field(field, rename_all)? else {
            continue;
        };
        if field.strategy == Strategy::Flatten {
            if flatten_field.is_some() {
                return Err(syn::Error::new_spanned(
                    &field.ident,
                    "MongoScan supports at most one #[serde(flatten)] field",
                ));
            }
            flatten_field = Some(field.ident.clone());
        }
        if field.strategy != Strategy::Ignore {
            field.field_index = Some(next_field_index);
            next_field_index += 1;
        }
        validate_map_serializer(&field)?;
        scan_fields.push(field);
    }

    let descriptors = scan_fields
        .iter()
        .map(|field| descriptor(&store, field))
        .collect::<Vec<_>>();
    let capture_fields = scan_fields
        .iter()
        .filter(|field| field.strategy != Strategy::Ignore)
        .map(|field| capture_statement(&serde_crate, field))
        .collect::<Vec<_>>();
    let diff_fields = scan_fields
        .iter()
        .filter(|field| field.strategy != Strategy::Ignore)
        .map(|field| diff_statement(&serde_crate, field))
        .collect::<Vec<_>>();
    let ident = &input.ident;
    let field_count = next_field_index;

    Ok(quote! {
        impl #store::scan::MongoScan for #ident {
            fn capture(&self) -> Result<#store::scan::ScanSnapshot, #store::scan::ScanError> {
                let mut snapshot = #store::scan::ScanSnapshot::empty();
                #(#capture_fields)*
                Ok(snapshot)
            }

            fn capture_bson(
                document: &#store::scan::BsonDocument,
            ) -> Result<#store::scan::ScanSnapshot, #store::scan::ScanError> {
                #store::scan::ScanSnapshot::empty().capture_bson_document(
                    document,
                    &[#(#descriptors),*],
                    #field_count,
                )
            }

            fn diff(
                &self,
                baseline: &#store::scan::ScanSnapshot,
                cursor: #store::scan::ScanCursor,
                budget: &mut #store::scan::ScanBudget,
            ) -> Result<#store::scan::ScanDelta, #store::scan::ScanError> {
                let mut scan = #store::scan::ScanBuilder::new(baseline, cursor, budget);
                #(#diff_fields)*
                Ok(scan.finish())
            }
        }
    })
}

fn descriptor(store: &TokenStream, field: &ScanField) -> TokenStream {
    let path = &field.path;
    match field.strategy {
        Strategy::Whole => {
            let index = field.field_index.expect("whole scan index");
            quote! { #store::scan::ScanFieldPolicy::whole(#index, #path) }
        }
        Strategy::Map => {
            let index = field.field_index.expect("map scan index");
            quote! { #store::scan::ScanFieldPolicy::map(#index, #path) }
        }
        Strategy::Flatten => {
            let index = field.field_index.expect("flatten scan index");
            quote! { #store::scan::ScanFieldPolicy::flatten(#index) }
        }
        Strategy::Ignore => quote! { #store::scan::ScanFieldPolicy::ignore(#path) },
    }
}

fn capture_statement(serde_crate: &TokenStream, field: &ScanField) -> TokenStream {
    let index = field.field_index.expect("captured field index");
    let absent = quote! { snapshot.capture_absent_field(#index); };
    let present = field_call(serde_crate, field, true);
    wrap_skip_predicate(field, absent, present)
}

fn diff_statement(serde_crate: &TokenStream, field: &ScanField) -> TokenStream {
    let index = field.field_index.expect("diff field index");
    let path = &field.path;
    let absent = if field.strategy == Strategy::Flatten {
        quote! { scan.flattened_absent(#index)?; }
    } else {
        quote! { scan.absent(#index, #path)?; }
    };
    let present = field_call(serde_crate, field, false);
    wrap_skip_predicate(field, absent, present)
}

fn wrap_skip_predicate(
    field: &ScanField,
    absent: TokenStream,
    present: TokenStream,
) -> TokenStream {
    let ident = &field.ident;
    if let Some(predicate) = &field.serde.skip_serializing_if {
        quote! {
            if #predicate(&self.#ident) {
                #absent
            } else {
                #present
            }
        }
    } else {
        present
    }
}

fn field_call(serde_crate: &TokenStream, field: &ScanField, capture: bool) -> TokenStream {
    let ident = &field.ident;
    let index = field.field_index.expect("scan field index");
    let path = &field.path;
    match (&field.adapter, field.strategy) {
        (Some(adapter), Strategy::Map) => {
            if capture {
                quote! {
                    snapshot.capture_map_entries_with_adapter_field::<#adapter, _, _>(
                        #index,
                        #path,
                        &self.#ident,
                    )?;
                }
            } else {
                quote! {
                    scan.map_entries_with_adapter::<#adapter, _, _>(
                        #index,
                        #path,
                        &self.#ident,
                    )?;
                }
            }
        }
        (None, Strategy::Map) if is_path_key_map(field) => {
            if capture {
                quote! {
                    snapshot.capture_path_key_map_entries_field(
                        #index,
                        #path,
                        &self.#ident,
                    )?;
                }
            } else {
                quote! { scan.path_key_map_entries(#index, #path, &self.#ident)?; }
            }
        }
        (None, Strategy::Map) => {
            if capture {
                quote! {
                    snapshot.capture_map_entries_field(#index, #path, &self.#ident)?;
                }
            } else {
                quote! { scan.map_entries(#index, #path, &self.#ident)?; }
            }
        }
        (None, Strategy::Whole | Strategy::Flatten) => {
            serialized_field_call(serde_crate, field, capture)
        }
        (None, Strategy::Ignore) => TokenStream::new(),
        (Some(_), _) => unreachable!("adapter validation requires a Map strategy"),
    }
}

fn serialized_field_call(
    serde_crate: &TokenStream,
    field: &ScanField,
    capture: bool,
) -> TokenStream {
    let ident = &field.ident;
    let ty = &field.ty;
    let index = field.field_index.expect("serialized field index");
    let path = &field.path;
    let wrapper = format_ident!("LatticeMongoScanSerializeField{}", index);
    let (definition, value) = if let Some(serializer) = &field.serde.serialize_with {
        (
            quote! {
                struct #wrapper<'scan>(&'scan #ty);
                impl<'scan> #serde_crate::Serialize for #wrapper<'scan> {
                    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
                    where
                        S: #serde_crate::Serializer,
                    {
                        #serializer(self.0, serializer)
                    }
                }
            },
            quote! { #wrapper(&self.#ident) },
        )
    } else {
        (TokenStream::new(), quote! { &self.#ident })
    };
    let call = match (field.strategy, capture) {
        (Strategy::Whole, true) => {
            quote! { snapshot.capture_value_field(#index, #path, &value)?; }
        }
        (Strategy::Whole, false) => quote! { scan.whole_value(#index, #path, &value)?; },
        (Strategy::Flatten, true) => {
            quote! { snapshot.capture_flattened_value_field(#index, &value)?; }
        }
        (Strategy::Flatten, false) => quote! { scan.flattened_value(#index, &value)?; },
        _ => unreachable!("serialized field strategy"),
    };
    quote! {{
        #definition
        let value = #value;
        #call
    }}
}

fn validate_map_serializer(field: &ScanField) -> syn::Result<()> {
    if field.adapter.is_some() && field.strategy != Strategy::Map {
        return Err(syn::Error::new_spanned(
            &field.ident,
            "#[mongo(adapter = ...)] requires #[mongo(scan = \"map\")]",
        ));
    }
    if field.adapter.is_some() {
        return Ok(());
    }
    if field.strategy != Strategy::Map || field.serde.serialize_with.is_none() {
        return Ok(());
    }
    if is_path_key_map(field) {
        return Ok(());
    }
    Err(syn::Error::new_spanned(
        &field.ident,
        "custom Map serializers require #[mongo(scan = \"map\", adapter = YourAdapter)] so entries can be scanned independently",
    ))
}

fn is_path_key_map(field: &ScanField) -> bool {
    field
        .serde
        .serialize_with_module
        .as_ref()
        .and_then(|path| path.segments.last())
        .is_some_and(|segment| segment.ident == "path_key_map")
}

fn scan_field(
    field: &syn::Field,
    rename_all: Option<crate::common::RenameRule>,
) -> syn::Result<Option<ScanField>> {
    let mut identity = false;
    let mut scan_ignore = false;
    let mut override_strategy = None;
    let mut adapter = None;
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
            } else if meta.path.is_ident("adapter") {
                if adapter.is_some() {
                    return Err(meta.error("Mongo scan adapter can only be declared once"));
                }
                adapter = Some(meta.value()?.parse::<syn::Path>()?);
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo scan option"))
            }
        })?;
    }
    if identity {
        if adapter.is_some() {
            return Err(syn::Error::new_spanned(
                field,
                "Mongo identity fields cannot declare a scan adapter",
            ));
        }
        return Ok(None);
    }

    let serde = serde_field_shape(field, rename_all)?;
    if serde.skipped {
        return Ok(None);
    }
    if serde.flattened && (scan_ignore || override_strategy.is_some()) {
        return Err(syn::Error::new_spanned(
            field,
            "flattened fields cannot use Mongo scan overrides because they do not have one BSON field path",
        ));
    }
    let strategy = if serde.flattened {
        Strategy::Flatten
    } else if scan_ignore {
        Strategy::Ignore
    } else {
        override_strategy.unwrap_or(Strategy::Whole)
    };
    Ok(Some(ScanField {
        ident: field.ident.clone().expect("named field"),
        ty: field.ty.clone(),
        path: serde.serialized_name.clone(),
        strategy,
        adapter,
        field_index: None,
        serde,
    }))
}
