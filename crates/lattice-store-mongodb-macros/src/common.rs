use proc_macro_crate::{FoundCrate, crate_name};
use proc_macro2::Span;
use quote::quote;
use syn::{Data, DeriveInput, Fields, Ident, LitStr};

#[derive(Debug, Clone, Copy)]
pub(crate) enum RenameRule {
    Lower,
    Upper,
    Pascal,
    Camel,
    Snake,
    ScreamingSnake,
    Kebab,
    ScreamingKebab,
}

impl RenameRule {
    fn parse(value: &LitStr) -> syn::Result<Self> {
        match value.value().as_str() {
            "lowercase" => Ok(Self::Lower),
            "UPPERCASE" => Ok(Self::Upper),
            "PascalCase" => Ok(Self::Pascal),
            "camelCase" => Ok(Self::Camel),
            "snake_case" => Ok(Self::Snake),
            "SCREAMING_SNAKE_CASE" => Ok(Self::ScreamingSnake),
            "kebab-case" => Ok(Self::Kebab),
            "SCREAMING-KEBAB-CASE" => Ok(Self::ScreamingKebab),
            _ => Err(syn::Error::new_spanned(
                value,
                "unsupported serde rename_all rule",
            )),
        }
    }

    pub(crate) fn apply_to_field(self, field: &str) -> String {
        match self {
            Self::Lower | Self::Snake => field.to_owned(),
            Self::Upper => field.to_ascii_uppercase(),
            Self::Pascal => {
                let mut result = String::new();
                let mut capitalize = true;
                for character in field.chars() {
                    if character == '_' {
                        capitalize = true;
                    } else if capitalize {
                        result.push(character.to_ascii_uppercase());
                        capitalize = false;
                    } else {
                        result.push(character);
                    }
                }
                result
            }
            Self::Camel => {
                let pascal = Self::Pascal.apply_to_field(field);
                let mut characters = pascal.chars();
                characters
                    .next()
                    .map(|first| first.to_ascii_lowercase().to_string() + characters.as_str())
                    .unwrap_or_default()
            }
            Self::ScreamingSnake => field.to_ascii_uppercase(),
            Self::Kebab => field.replace('_', "-"),
            Self::ScreamingKebab => field.to_ascii_uppercase().replace('_', "-"),
        }
    }
}

pub(crate) fn serde_serialize_rename_all(input: &DeriveInput) -> syn::Result<Option<RenameRule>> {
    let mut rename_all = None;
    for attribute in &input.attrs {
        if !attribute.path().is_ident("serde") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename_all") {
                if meta.input.peek(syn::Token![=]) {
                    rename_all = Some(RenameRule::parse(&meta.value()?.parse::<LitStr>()?)?);
                    return Ok(());
                }
                return meta.parse_nested_meta(|nested| {
                    if nested.path.is_ident("serialize") {
                        rename_all = Some(RenameRule::parse(&nested.value()?.parse::<LitStr>()?)?);
                    } else if nested.input.peek(syn::Token![=]) {
                        let _ = nested.value()?.parse::<syn::Expr>()?;
                    }
                    Ok(())
                });
            }
            consume_meta(meta)
        })?;
    }
    Ok(rename_all)
}

pub(crate) struct SerdeFieldShape {
    pub(crate) serialized_name: String,
    pub(crate) skipped: bool,
    pub(crate) flattened: bool,
}

pub(crate) fn serde_field_shape(
    field: &syn::Field,
    rename_all: Option<RenameRule>,
) -> syn::Result<SerdeFieldShape> {
    let ident = field.ident.as_ref().expect("named field");
    let mut serialized_name = rename_all
        .map(|rule| rule.apply_to_field(&ident.to_string()))
        .unwrap_or_else(|| ident.to_string());
    let mut skipped = false;
    let mut flattened = false;
    for attribute in &field.attrs {
        if !attribute.path().is_ident("serde") {
            continue;
        }
        attribute.parse_nested_meta(|meta| {
            if meta.path.is_ident("rename") {
                if meta.input.peek(syn::Token![=]) {
                    serialized_name = meta.value()?.parse::<LitStr>()?.value();
                    return Ok(());
                }
                return meta.parse_nested_meta(|nested| {
                    if nested.path.is_ident("serialize") {
                        serialized_name = nested.value()?.parse::<LitStr>()?.value();
                    } else if nested.input.peek(syn::Token![=]) {
                        let _ = nested.value()?.parse::<syn::Expr>()?;
                    }
                    Ok(())
                });
            }
            if meta.path.is_ident("skip") || meta.path.is_ident("skip_serializing") {
                skipped = true;
                return Ok(());
            }
            if meta.path.is_ident("flatten") {
                flattened = true;
                return Ok(());
            }
            consume_meta(meta)
        })?;
    }
    Ok(SerdeFieldShape {
        serialized_name,
        skipped,
        flattened,
    })
}

fn consume_meta(meta: syn::meta::ParseNestedMeta<'_>) -> syn::Result<()> {
    if meta.input.peek(syn::Token![=]) {
        let _ = meta.value()?.parse::<syn::Expr>()?;
    } else if meta.input.peek(syn::token::Paren) {
        meta.parse_nested_meta(consume_meta)?;
    }
    Ok(())
}

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
