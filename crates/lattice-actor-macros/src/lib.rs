use proc_macro::TokenStream;
use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{Attribute, DeriveInput, Ident, Type, parse_macro_input, parse_quote};

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
