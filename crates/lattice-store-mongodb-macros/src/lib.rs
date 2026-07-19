//! Derives for typed Mongo documents and actor-local persistence scans.

use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::{format_ident, quote};
use syn::{
    Data, DeriveInput, Fields, GenericArgument, Ident, LitStr, PathArguments, Type,
    parse_macro_input,
};

#[proc_macro_derive(MongoDocument, attributes(mongo, serde))]
pub fn derive_mongo_document(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match document_options(&input).and_then(|options| expand_document(&input, options)) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_derive(MongoScan, attributes(mongo, serde))]
pub fn derive_mongo_scan(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_scan(&input) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

#[proc_macro_derive(MongoDocumentSet, attributes(mongo))]
pub fn derive_mongo_document_set(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    match expand_document_set(&input) {
        Ok(output) => output.into(),
        Err(error) => error.into_compile_error().into(),
    }
}

struct DocumentOptions {
    collection: LitStr,
    id_field: Ident,
    id_type: Type,
    serialized_id_field: String,
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

fn expand_document(
    input: &DeriveInput,
    options: DocumentOptions,
) -> syn::Result<proc_macro2::TokenStream> {
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

fn expand_scan(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
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

struct DocumentSetOptions {
    id: Type,
    loaded: Ident,
}

struct DocumentSetField<'a> {
    ident: &'a Ident,
    visibility: &'a syn::Visibility,
    kind: DocumentSetFieldKind,
}

enum DocumentSetFieldKind {
    One(Type),
    Many(Type),
    Lazy(Type),
    Unloadable(Type, u64),
}

fn expand_document_set(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    if !input.generics.params.is_empty() {
        return Err(syn::Error::new_spanned(
            &input.generics,
            "MongoDocumentSet does not support generic structs",
        ));
    }
    let store = store_crate_path()?;
    let fields = require_named_struct(input)?;
    let options = document_set_options(input)?;
    let persistent = fields
        .named
        .iter()
        .filter_map(|field| match document_set_field(field) {
            Ok(value) => value.map(Ok),
            Err(error) => Some(Err(error)),
        })
        .collect::<syn::Result<Vec<_>>>()?;
    if persistent.is_empty() {
        return Err(syn::Error::new_spanned(
            input,
            "MongoDocumentSet requires at least one persistent field",
        ));
    }

    let ident = &input.ident;
    let visibility = &input.vis;
    let loaded = options.loaded;
    let id = options.id;
    let document_count = persistent.len();
    let eager = persistent
        .iter()
        .filter(|field| {
            matches!(
                field.kind,
                DocumentSetFieldKind::One(_) | DocumentSetFieldKind::Many(_)
            )
        })
        .collect::<Vec<_>>();
    let loaded_fields = eager.iter().map(|field| {
        let ident = field.ident;
        let visibility = field.visibility;
        match &field.kind {
            DocumentSetFieldKind::One(document) => {
                quote! { #visibility #ident: #store::document::LoadedDocument<#document> }
            }
            DocumentSetFieldKind::Many(collection) => quote! {
                #visibility #ident: ::std::vec::Vec<#store::document::LoadedDocument<
                    <#collection as #store::document_set::MongoDocumentCollection<#id>>::Document
                >>
            },
            DocumentSetFieldKind::Lazy(_) | DocumentSetFieldKind::Unloadable(_, _) => {
                unreachable!("lazy fields are not startup-loaded")
            }
        }
    });
    let validations = eager.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(document) => quote! {
                if loaded.#field_ident.id() != id {
                    return Err(#store::coordinator::PersistenceError::DocumentIdMismatch {
                        collection: <#document as #store::document::MongoDocument>::COLLECTION,
                        expected: ::std::format!("{:?}", id),
                        actual: ::std::format!("{:?}", loaded.#field_ident.id()),
                    });
                }
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                for document in &loaded.#field_ident {
                    let actual = <#collection as #store::document_set::MongoDocumentCollection<#id>>::owner_id(
                        &document.value,
                    );
                    if actual != id {
                        return Err(#store::coordinator::PersistenceError::DocumentIdMismatch {
                            collection: <<#collection as #store::document_set::MongoDocumentCollection<#id>>::Document as #store::document::MongoDocument>::COLLECTION,
                            expected: ::std::format!("{:?}", id),
                            actual: ::std::format!("{:?}", actual),
                        });
                    }
                }
            },
            DocumentSetFieldKind::Lazy(_) | DocumentSetFieldKind::Unloadable(_, _) => {
                unreachable!("lazy fields are not startup-loaded")
            }
        }
    });
    let eager_idents = eager.iter().map(|field| field.ident).collect::<Vec<_>>();
    let splits_and_attaches = eager.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(_) => quote! {
                let #field_ident = coordinator.track_loaded(#field_ident)?;
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                let #field_ident = coordinator.track_loaded_many(#field_ident)?;
                let #field_ident = <#collection as #store::document_set::MongoDocumentCollection<#id>>::from_documents(
                    #field_ident,
                )?;
            },
            DocumentSetFieldKind::Lazy(_) | DocumentSetFieldKind::Unloadable(_, _) => {
                unreachable!("lazy fields are not startup-loaded")
            }
        }
    });
    let initializers = fields.named.iter().map(|field| {
        let field_ident = field.ident.as_ref().expect("named field");
        match document_set_field(field) {
            Ok(Some(DocumentSetField {
                kind: DocumentSetFieldKind::One(_) | DocumentSetFieldKind::Many(_),
                ..
            })) => quote! { #field_ident },
            Ok(Some(DocumentSetField {
                kind: DocumentSetFieldKind::Lazy(field_type),
                ..
            })) => quote! {
                #field_ident: <#field_type as #store::lazy::MongoLazyField<#id>>::new_lazy(id.clone())
            },
            Ok(Some(DocumentSetField {
                kind: DocumentSetFieldKind::Unloadable(field_type, idle_millis),
                ..
            })) => quote! {
                #field_ident: <#field_type as #store::lazy::MongoUnloadableField<#id>>::new_unloadable(
                    id.clone(),
                    ::std::time::Duration::from_millis(#idle_millis),
                )
            },
            Ok(None) => quote! { #field_ident: ::core::default::Default::default() },
            Err(_) => unreachable!("document set fields were validated above"),
        }
    });
    let scans = persistent.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(_) => quote! {
                preparation.scan_tracked(&self.#field_ident)?;
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                for document in <#collection as #store::document_set::MongoDocumentCollection<#id>>::documents(
                    &self.#field_ident,
                ) {
                    preparation.scan_tracked(document)?;
                }
            },
            DocumentSetFieldKind::Lazy(field_type) => quote! {
                <#field_type as #store::lazy::MongoLazyField<#id>>::scan_loaded(
                    &self.#field_ident,
                    preparation,
                )?;
            },
            DocumentSetFieldKind::Unloadable(field_type, _) => quote! {
                <#field_type as #store::lazy::MongoUnloadableField<#id>>::scan_loaded(
                    &self.#field_ident,
                    preparation,
                )?;
            },
        }
    });
    let loads = eager.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(document) => quote! {
                let #field_ident = store
                    .find_one::<#document>(id.clone())
                    .await?
                    .ok_or_else(|| #store::coordinator::PersistenceError::RequiredDocumentMissing {
                        collection: <#document as #store::document::MongoDocument>::COLLECTION,
                        id: ::std::format!("{:?}", id),
                    })?;
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                let #field_ident = store
                    .find_many::<<#collection as #store::document_set::MongoDocumentCollection<#id>>::Document>(
                        <#collection as #store::document_set::MongoDocumentCollection<#id>>::load_filter(&id)?,
                    )
                    .await?;
            },
            DocumentSetFieldKind::Lazy(_) | DocumentSetFieldKind::Unloadable(_, _) => {
                unreachable!("lazy fields are not startup-loaded")
            }
        }
    });

    Ok(quote! {
        #visibility struct #loaded {
            #(#loaded_fields,)*
        }

        impl #store::document_set::MongoDocumentSet for #ident {
            type Id = #id;
            type Loaded = #loaded;

            const DOCUMENT_COUNT: usize = #document_count;

            fn from_loaded(
                id: &Self::Id,
                loaded: Self::Loaded,
                coordinator: &mut #store::coordinator::MongoPersistenceCoordinator,
            ) -> ::core::result::Result<Self, #store::coordinator::PersistenceError> {
                #(#validations)*
                let #loaded { #(#eager_idents,)* } = loaded;
                #(#splits_and_attaches)*
                Ok(Self { #(#initializers,)* })
            }

            fn load<'a>(
                store: &'a #store::mongo_store::MongoStore,
                id: &Self::Id,
                coordinator: &'a mut #store::coordinator::MongoPersistenceCoordinator,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<Self, #store::coordinator::PersistenceError>,
            > + Send + 'a {
                let id = id.clone();
                async move {
                    #(#loads)*
                    Self::from_loaded(
                        &id,
                        #loaded { #(#eager_idents,)* },
                        coordinator,
                    )
                }
            }

            fn scan_all(
                &self,
                preparation: &mut #store::coordinator::MongoPreparation<'_>,
            ) -> ::core::result::Result<(), #store::coordinator::PersistenceError> {
                #(#scans)*
                Ok(())
            }
        }
    })
}

fn document_set_options(input: &DeriveInput) -> syn::Result<DocumentSetOptions> {
    let mut id = None;
    let mut loaded = None;
    for attribute in &input.attrs {
        if !attribute.path().is_ident("mongo") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("id") {
                id = Some(meta.value()?.parse::<Type>()?);
                Ok(())
            } else if meta.path.is_ident("loaded") {
                loaded = Some(meta.value()?.parse::<Ident>()?);
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo document-set option"))
            }
        })?;
    }
    Ok(DocumentSetOptions {
        id: id.ok_or_else(|| {
            syn::Error::new_spanned(input, "missing #[mongo(id = IdType)] option")
        })?,
        loaded: loaded.unwrap_or_else(|| format_ident!("Loaded{}", input.ident)),
    })
}

fn document_set_field(field: &syn::Field) -> syn::Result<Option<DocumentSetField<'_>>> {
    let mut skipped = false;
    let mut many = false;
    let mut lazy = false;
    let mut lazy_unload = None;
    for attribute in &field.attrs {
        if !attribute.path().is_ident("mongo") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("skip") {
                if meta.input.peek(syn::Token![=]) {
                    return Err(meta.error("#[mongo(skip)] does not accept a value"));
                }
                skipped = true;
                Ok(())
            } else if meta.path.is_ident("many") {
                if meta.input.peek(syn::Token![=]) {
                    return Err(meta.error("#[mongo(many)] does not accept a value"));
                }
                many = true;
                Ok(())
            } else if meta.path.is_ident("lazy") {
                if meta.input.peek(syn::Token![=]) {
                    return Err(meta.error("#[mongo(lazy)] does not accept a value"));
                }
                lazy = true;
                Ok(())
            } else if meta.path.is_ident("lazy_unload") {
                let duration = meta.value()?.parse::<LitStr>()?;
                lazy_unload = Some(parse_duration_millis(&duration)?);
                Ok(())
            } else {
                Err(meta.error("unsupported Mongo document-set field option"))
            }
        })?;
    }
    let strategy_count = usize::from(skipped)
        + usize::from(many)
        + usize::from(lazy)
        + usize::from(lazy_unload.is_some());
    if strategy_count > 1 {
        return Err(syn::Error::new_spanned(
            field,
            "MongoDocumentSet field accepts only one of #[mongo(skip)], #[mongo(many)], #[mongo(lazy)], or #[mongo(lazy_unload = \"...\")]",
        ));
    }
    if skipped {
        return Ok(None);
    }
    let ident = field.ident.as_ref().expect("named field");
    if lazy {
        return Ok(Some(DocumentSetField {
            ident,
            visibility: &field.vis,
            kind: DocumentSetFieldKind::Lazy(field.ty.clone()),
        }));
    }
    if let Some(idle_millis) = lazy_unload {
        return Ok(Some(DocumentSetField {
            ident,
            visibility: &field.vis,
            kind: DocumentSetFieldKind::Unloadable(field.ty.clone(), idle_millis),
        }));
    }
    if many {
        return Ok(Some(DocumentSetField {
            ident,
            visibility: &field.vis,
            kind: DocumentSetFieldKind::Many(field.ty.clone()),
        }));
    }
    let Type::Path(path) = &field.ty else {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "MongoDocumentSet fields must have type Tracked<T> or use #[mongo(skip)]",
        ));
    };
    let Some(segment) = path.path.segments.last() else {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "MongoDocumentSet fields must have type Tracked<T> or use #[mongo(skip)]",
        ));
    };
    if segment.ident != "Tracked" {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "MongoDocumentSet fields must have type Tracked<T> or use #[mongo(skip)]",
        ));
    }
    let PathArguments::AngleBracketed(arguments) = &segment.arguments else {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "Tracked must contain exactly one document type",
        ));
    };
    let mut types = arguments.args.iter().filter_map(|argument| match argument {
        GenericArgument::Type(ty) => Some(ty.clone()),
        _ => None,
    });
    let Some(document) = types.next() else {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "Tracked must contain exactly one document type",
        ));
    };
    if types.next().is_some() || arguments.args.len() != 1 {
        return Err(syn::Error::new_spanned(
            &field.ty,
            "Tracked must contain exactly one document type",
        ));
    }
    Ok(Some(DocumentSetField {
        ident,
        visibility: &field.vis,
        kind: DocumentSetFieldKind::One(document),
    }))
}

fn parse_duration_millis(value: &LitStr) -> syn::Result<u64> {
    let raw = value.value();
    let split = raw
        .find(|character: char| !character.is_ascii_digit())
        .ok_or_else(|| {
            syn::Error::new_spanned(value, "duration requires a unit: ms, s, m, h, or d")
        })?;
    let (amount, unit) = raw.split_at(split);
    if amount.is_empty() || amount.starts_with('0') {
        return Err(syn::Error::new_spanned(
            value,
            "duration must be a positive integer without leading zeroes",
        ));
    }
    let amount = amount.parse::<u64>().map_err(|_| {
        syn::Error::new_spanned(
            value,
            "duration amount does not fit in an unsigned 64-bit integer",
        )
    })?;
    let multiplier = match unit {
        "ms" => 1,
        "s" => 1_000,
        "m" => 60_000,
        "h" => 3_600_000,
        "d" => 86_400_000,
        _ => {
            return Err(syn::Error::new_spanned(
                value,
                "unsupported duration unit; use ms, s, m, h, or d",
            ));
        }
    };
    amount
        .checked_mul(multiplier)
        .ok_or_else(|| syn::Error::new_spanned(value, "duration is too large"))
}

fn store_crate_path() -> syn::Result<proc_macro2::TokenStream> {
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

fn require_named_struct(input: &DeriveInput) -> syn::Result<&syn::FieldsNamed> {
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
                    // Serde owns representation attributes such as `with`,
                    // `serialize_with`, and `deserialize_with`. Scan strategy
                    // inference only needs to leave them intact, but must
                    // still consume their value while parsing the attribute.
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
