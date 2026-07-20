use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::{ToTokens, quote};
use std::collections::BTreeMap;
use syn::parse::{Parse, ParseStream};
use syn::{
    Attribute, DeriveInput, Ident, Pat, Token, Type, braced, bracketed, parse_macro_input,
    parse_quote, punctuated::Punctuated,
};

/// Implements `lattice_actor::traits::Message` for a type.
#[proc_macro_derive(Message)]
pub fn derive_message(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_message(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Implements `lattice_actor::traits::Request` for a type.
///
/// The response type must be supplied with `#[request(response = Type)]`.
#[proc_macro_derive(Request, attributes(request))]
pub fn derive_request(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as DeriveInput);
    expand_request(&input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

/// Declares the messages accepted by each variant of an actor behavior.
#[proc_macro]
pub fn actor_behavior(input: TokenStream) -> TokenStream {
    let input = parse_macro_input!(input as BehaviorInput);
    expand_actor_behavior(input)
        .unwrap_or_else(syn::Error::into_compile_error)
        .into()
}

struct BehaviorInput {
    behavior: Type,
    entries: Vec<BehaviorEntry>,
}

struct BehaviorEntry {
    pattern: Option<Pat>,
    messages: Vec<Type>,
}

impl Parse for BehaviorInput {
    fn parse(input: ParseStream<'_>) -> syn::Result<Self> {
        let behavior = input.parse()?;
        let body;
        braced!(body in input);
        let mut entries = Vec::new();
        while !body.is_empty() {
            let pattern = if body.peek(Ident) && body.peek2(Token![=>]) {
                let ident: Ident = body.parse()?;
                if ident != "always" {
                    return Err(syn::Error::new_spanned(
                        ident,
                        "expected a state pattern or `always`",
                    ));
                }
                None
            } else {
                Some(body.call(Pat::parse_multi)?)
            };
            body.parse::<Token![=>]>()?;
            let messages;
            bracketed!(messages in body);
            let messages = Punctuated::<Type, Token![,]>::parse_terminated(&messages)?
                .into_iter()
                .collect();
            entries.push(BehaviorEntry { pattern, messages });
            if body.peek(Token![;]) {
                body.parse::<Token![;]>()?;
            } else if !body.is_empty() {
                return Err(body.error("expected `;` after behavior state entry"));
            }
        }
        Ok(Self { behavior, entries })
    }
}

fn expand_actor_behavior(input: BehaviorInput) -> syn::Result<proc_macro2::TokenStream> {
    let actor = actor_crate_path()?;
    let behavior = input.behavior;
    let mut messages: BTreeMap<String, (Type, bool, Vec<Pat>)> = BTreeMap::new();
    for entry in input.entries {
        for message in entry.messages {
            let key = message.to_token_stream().to_string();
            let definition = messages
                .entry(key)
                .or_insert_with(|| (message, false, Vec::new()));
            match &entry.pattern {
                None if definition.1 || !definition.2.is_empty() => {
                    return Err(syn::Error::new_spanned(
                        &definition.0,
                        "message may be declared either `always` or in individual states, not both",
                    ));
                }
                None => definition.1 = true,
                Some(pattern) if definition.1 => {
                    return Err(syn::Error::new_spanned(
                        &definition.0,
                        "message may be declared either `always` or in individual states, not both",
                    ));
                }
                Some(pattern) => definition.2.push(pattern.clone()),
            }
        }
    }

    let filters = messages.into_values().map(|(message, always, patterns)| {
        if always {
            quote! {
                impl #actor::state_machine::Accepts<#message> for #behavior {
                    const ALWAYS: bool = true;

                    #[inline(always)]
                    fn accepts(&self) -> bool { true }
                }
            }
        } else {
            quote! {
                impl #actor::state_machine::Accepts<#message> for #behavior {
                    #[inline]
                    fn accepts(&self) -> bool {
                        ::core::matches!(self, #(#patterns)|*)
                    }
                }
            }
        }
    });

    Ok(quote! {
        impl #actor::state_machine::Behavior for #behavior {}
        #(#filters)*
    })
}

fn expand_message(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let actor = actor_crate_path()?;
    let ident = &input.ident;
    let self_type = self_type(input);
    let mut generics = input.generics.clone();
    generics
        .make_where_clause()
        .predicates
        .push(parse_quote!(#self_type: ::core::marker::Send + 'static));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #actor::traits::Message for #ident #type_generics #where_clause {}
    })
}

fn expand_request(input: &DeriveInput) -> syn::Result<proc_macro2::TokenStream> {
    let actor = actor_crate_path()?;
    let response = request_response(&input.attrs, input.ident.span())?;
    let ident = &input.ident;
    let self_type = self_type(input);
    let mut generics = input.generics.clone();
    let where_clause = generics.make_where_clause();
    where_clause
        .predicates
        .push(parse_quote!(#self_type: ::core::marker::Send + 'static));
    where_clause
        .predicates
        .push(parse_quote!(#response: ::core::marker::Send + 'static));
    let (impl_generics, type_generics, where_clause) = generics.split_for_impl();

    Ok(quote! {
        impl #impl_generics #actor::traits::Request for #ident #type_generics #where_clause {
            type Response = #response;
        }
    })
}

fn self_type(input: &DeriveInput) -> Type {
    let ident = &input.ident;
    let (_, type_generics, _) = input.generics.split_for_impl();
    parse_quote!(#ident #type_generics)
}

fn request_response(attrs: &[Attribute], item_span: Span) -> syn::Result<Type> {
    let request_attrs: Vec<_> = attrs
        .iter()
        .filter(|attr| attr.path().is_ident("request"))
        .collect();

    if let Some(duplicate) = request_attrs.get(1) {
        return Err(syn::Error::new_spanned(
            duplicate,
            "duplicate `request` attribute; expected exactly one `#[request(response = Type)]`",
        ));
    }

    let Some(attr) = request_attrs.first() else {
        return Err(syn::Error::new(
            item_span,
            "`Request` requires `#[request(response = Type)]`",
        ));
    };

    let mut response = None;
    attr.parse_nested_meta(|meta| {
        if !meta.path.is_ident("response") {
            return Err(meta.error("unsupported request option; expected `response`"));
        }
        if response.is_some() {
            return Err(meta.error("duplicate `response` option"));
        }
        response = Some(meta.value()?.parse::<Type>()?);
        Ok(())
    })?;

    response.ok_or_else(|| syn::Error::new_spanned(attr, "`request` requires `response = Type`"))
}

fn actor_crate_path() -> syn::Result<proc_macro2::TokenStream> {
    match crate_name("lattice-actor") {
        Ok(FoundCrate::Itself) => Ok(quote!(::lattice_actor)),
        Ok(FoundCrate::Name(name)) => {
            let ident = Ident::new(&name, Span::call_site());
            Ok(quote!(::#ident))
        }
        Err(error) => Err(syn::Error::new(
            Span::call_site(),
            format!("could not resolve the `lattice-actor` crate: {error}"),
        )),
    }
}
