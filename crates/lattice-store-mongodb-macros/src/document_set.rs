use quote::{format_ident, quote};
use syn::{DeriveInput, GenericArgument, Ident, LitStr, PathArguments, Type};

use crate::common::{require_named_struct, store_crate_path};

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
    Default(Type),
    Many(Type),
    Lazy(Type),
    Unloadable(Type, u64),
}

pub(crate) fn expand(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
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
                DocumentSetFieldKind::One(_)
                    | DocumentSetFieldKind::Default(_)
                    | DocumentSetFieldKind::Many(_)
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
            DocumentSetFieldKind::Default(document) => quote! {
                #visibility #ident: ::core::option::Option<#store::document::LoadedDocument<#document>>
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                #visibility #ident: ::std::vec::Vec<#store::document::LoadedDocument<
                    <#collection as #store::document::set::MongoDocumentCollection<#id>>::Document
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
                    return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                        collection: <#document as #store::document::MongoDocument>::COLLECTION,
                        expected: ::std::format!("{:?}", id),
                        actual: ::std::format!("{:?}", loaded.#field_ident.id()),
                    });
                }
            },
            DocumentSetFieldKind::Default(document) => quote! {
                if let ::core::option::Option::Some(document) = &loaded.#field_ident {
                    if document.id() != id {
                        return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                            collection: <#document as #store::document::MongoDocument>::COLLECTION,
                            expected: ::std::format!("{:?}", id),
                            actual: ::std::format!("{:?}", document.id()),
                        });
                    }
                }
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                for document in &loaded.#field_ident {
                    let actual = <#collection as #store::document::set::MongoDocumentCollection<#id>>::owner_id(
                        &document.value,
                    );
                    if actual != id {
                        return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                            collection: <<#collection as #store::document::set::MongoDocumentCollection<#id>>::Document as #store::document::MongoDocument>::COLLECTION,
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
    let loaded_defaults = eager.iter().filter_map(|field| {
        let field_ident = field.ident;
        let DocumentSetFieldKind::Default(document) = &field.kind else {
            return None;
        };
        let absent_ident = format_ident!("__mongo_absent_{}", field_ident);
        Some(quote! {
            let #absent_ident = if loaded.#field_ident.is_none() {
                let document = <#document as #store::document::set::MongoDefaultDocument<#id>>::default_for(id);
                if <#document as #store::document::MongoDocument>::id(&document) != id {
                    return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                        collection: <#document as #store::document::MongoDocument>::COLLECTION,
                        expected: ::std::format!("{:?}", id),
                        actual: ::std::format!("{:?}", <#document as #store::document::MongoDocument>::id(&document)),
                    });
                }
                ::core::option::Option::Some(document)
            } else {
                ::core::option::Option::None
            };
        })
    });
    let eager_idents = eager.iter().map(|field| field.ident).collect::<Vec<_>>();
    let splits_and_attaches = eager.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(_) => quote! {
                let #field_ident = coordinator.track_loaded(#field_ident)?;
            },
            DocumentSetFieldKind::Default(_) => {
                let absent_ident = format_ident!("__mongo_absent_{}", field_ident);
                quote! {
                    let #field_ident = match #field_ident {
                        ::core::option::Option::Some(document) => coordinator.track_loaded(document)?,
                        ::core::option::Option::None => coordinator.track_absent(
                            #absent_ident.expect("missing default document was constructed before registration"),
                        )?,
                    };
                }
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                let #field_ident = coordinator.track_loaded_many(#field_ident)?;
                let #field_ident = <#collection as #store::document::set::MongoDocumentCollection<#id>>::from_documents(
                    #field_ident,
                )?;
            },
            DocumentSetFieldKind::Lazy(_) | DocumentSetFieldKind::Unloadable(_, _) => {
                unreachable!("lazy fields are not startup-loaded")
            }
        }
    });
    let initializers = fields
        .named
        .iter()
        .map(|field| {
            let field_ident = field.ident.as_ref().expect("named field");
            match document_set_field(field) {
            Ok(Some(DocumentSetField {
                kind: DocumentSetFieldKind::One(_)
                    | DocumentSetFieldKind::Default(_)
                    | DocumentSetFieldKind::Many(_),
                ..
            })) => quote! { #field_ident },
            Ok(Some(DocumentSetField {
                kind: DocumentSetFieldKind::Lazy(field_type),
                ..
            })) => quote! {
                #field_ident: <#field_type as #store::loading::policy::MongoLazyField<#id>>::new_lazy(id.clone())
            },
            Ok(Some(DocumentSetField {
                kind: DocumentSetFieldKind::Unloadable(field_type, idle_millis),
                ..
            })) => quote! {
                #field_ident: <#field_type as #store::loading::policy::MongoUnloadableField<#id>>::new_unloadable(
                    id.clone(),
                    ::std::time::Duration::from_millis(#idle_millis),
                )
            },
            Ok(None) => quote! { #field_ident: ::core::default::Default::default() },
                Err(_) => unreachable!("document set fields were validated above"),
            }
        })
        .collect::<Vec<_>>();
    let scans = persistent.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(_) => quote! {
                preparation.scan_tracked(&self.#field_ident)?;
            },
            DocumentSetFieldKind::Default(_) => quote! {
                preparation.scan_tracked(&self.#field_ident)?;
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                for document in <#collection as #store::document::set::MongoDocumentCollection<#id>>::documents(
                    &self.#field_ident,
                ) {
                    preparation.scan_tracked(document)?;
                }
            },
            DocumentSetFieldKind::Lazy(field_type) => quote! {
                <#field_type as #store::loading::policy::MongoLazyField<#id>>::scan_loaded(
                    &self.#field_ident,
                    preparation,
                )?;
            },
            DocumentSetFieldKind::Unloadable(field_type, _) => quote! {
                <#field_type as #store::loading::policy::MongoUnloadableField<#id>>::scan_loaded(
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
                    .find_one_scanned::<#document>(id.clone())
                    .await?
                    .ok_or_else(|| #store::persistence::coordinator::PersistenceError::RequiredDocumentMissing {
                        collection: <#document as #store::document::MongoDocument>::COLLECTION,
                        id: ::std::format!("{:?}", id),
                    })?;
            },
            DocumentSetFieldKind::Default(document) => quote! {
                let #field_ident = store
                    .find_one_scanned::<#document>(id.clone())
                    .await?;
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                let #field_ident = store
                    .find_many_scanned::<<#collection as #store::document::set::MongoDocumentCollection<#id>>::Document>(
                        <#collection as #store::document::set::MongoDocumentCollection<#id>>::load_filter(&id)?,
                    )
                    .await?;
            },
            DocumentSetFieldKind::Lazy(_) | DocumentSetFieldKind::Unloadable(_, _) => {
                unreachable!("lazy fields are not startup-loaded")
            }
        }
    });
    let scanned_validations = eager.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(document) => quote! {
                if #field_ident.id() != &id {
                    return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                        collection: <#document as #store::document::MongoDocument>::COLLECTION,
                        expected: ::std::format!("{:?}", id),
                        actual: ::std::format!("{:?}", #field_ident.id()),
                    });
                }
            },
            DocumentSetFieldKind::Default(document) => quote! {
                if let ::core::option::Option::Some(document) = &#field_ident {
                    if document.id() != &id {
                        return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                            collection: <#document as #store::document::MongoDocument>::COLLECTION,
                            expected: ::std::format!("{:?}", id),
                            actual: ::std::format!("{:?}", document.id()),
                        });
                    }
                }
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                for document in &#field_ident {
                    let actual = <#collection as #store::document::set::MongoDocumentCollection<#id>>::owner_id(
                        document.value(),
                    );
                    if actual != &id {
                        return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                            collection: <<#collection as #store::document::set::MongoDocumentCollection<#id>>::Document as #store::document::MongoDocument>::COLLECTION,
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
    let scanned_defaults = eager.iter().filter_map(|field| {
        let field_ident = field.ident;
        let DocumentSetFieldKind::Default(document) = &field.kind else {
            return None;
        };
        let absent_ident = format_ident!("__mongo_absent_{}", field_ident);
        Some(quote! {
            let #absent_ident = if #field_ident.is_none() {
                let document = <#document as #store::document::set::MongoDefaultDocument<#id>>::default_for(&id);
                if <#document as #store::document::MongoDocument>::id(&document) != &id {
                    return Err(#store::persistence::coordinator::PersistenceError::DocumentIdMismatch {
                        collection: <#document as #store::document::MongoDocument>::COLLECTION,
                        expected: ::std::format!("{:?}", id),
                        actual: ::std::format!("{:?}", <#document as #store::document::MongoDocument>::id(&document)),
                    });
                }
                ::core::option::Option::Some(document)
            } else {
                ::core::option::Option::None
            };
        })
    });
    let scanned_attaches = eager.iter().map(|field| {
        let field_ident = field.ident;
        match &field.kind {
            DocumentSetFieldKind::One(_) => quote! {
                let #field_ident = coordinator.track_loaded_scanned(#field_ident)?;
            },
            DocumentSetFieldKind::Default(_) => {
                let absent_ident = format_ident!("__mongo_absent_{}", field_ident);
                quote! {
                    let #field_ident = match #field_ident {
                        ::core::option::Option::Some(document) => coordinator.track_loaded_scanned(document)?,
                        ::core::option::Option::None => coordinator.track_absent(
                            #absent_ident.expect("missing default document was constructed before registration"),
                        )?,
                    };
                }
            },
            DocumentSetFieldKind::Many(collection) => quote! {
                let #field_ident = coordinator.track_loaded_scanned_many(#field_ident)?;
                let #field_ident = <#collection as #store::document::set::MongoDocumentCollection<#id>>::from_documents(
                    #field_ident,
                )?;
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

        impl #store::document::set::MongoDocumentSet for #ident {
            type Id = #id;
            type Loaded = #loaded;

            const DOCUMENT_COUNT: usize = #document_count;

            fn from_loaded(
                id: &Self::Id,
                loaded: Self::Loaded,
                coordinator: &mut #store::persistence::coordinator::MongoPersistenceCoordinator,
            ) -> ::core::result::Result<Self, #store::persistence::coordinator::PersistenceError> {
                #(#validations)*
                #(#loaded_defaults)*
                let #loaded { #(#eager_idents,)* } = loaded;
                #(#splits_and_attaches)*
                Ok(Self { #(#initializers,)* })
            }

            fn load<'a>(
                store: &'a #store::store::MongoStore,
                id: &Self::Id,
                coordinator: &'a mut #store::persistence::coordinator::MongoPersistenceCoordinator,
            ) -> impl ::core::future::Future<
                Output = ::core::result::Result<Self, #store::persistence::coordinator::PersistenceError>,
            > + Send + 'a {
                let id = id.clone();
                async move {
                    #(#loads)*
                    #(#scanned_validations)*
                    #(#scanned_defaults)*
                    #(#scanned_attaches)*
                    Ok(Self { #(#initializers,)* })
                }
            }

            fn scan_all(
                &self,
                preparation: &mut #store::persistence::coordinator::MongoPreparation<'_>,
            ) -> ::core::result::Result<(), #store::persistence::coordinator::PersistenceError> {
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
    let mut default_on_missing = false;
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
            } else if meta.path.is_ident("default") {
                if meta.input.peek(syn::Token![=]) {
                    return Err(meta.error("#[mongo(default)] does not accept a value"));
                }
                default_on_missing = true;
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
        + usize::from(default_on_missing)
        + usize::from(many)
        + usize::from(lazy)
        + usize::from(lazy_unload.is_some());
    if strategy_count > 1 {
        return Err(syn::Error::new_spanned(
            field,
            "MongoDocumentSet field accepts only one of #[mongo(skip)], #[mongo(default)], #[mongo(many)], #[mongo(lazy)], or #[mongo(lazy_unload = \"...\")]",
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
        kind: if default_on_missing {
            DocumentSetFieldKind::Default(document)
        } else {
            DocumentSetFieldKind::One(document)
        },
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
