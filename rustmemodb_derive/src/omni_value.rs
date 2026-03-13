use proc_macro2::TokenStream as TokenStream2;
use quote::quote;
use syn::{DeriveInput, Error};

pub fn expand_omni_value(input: DeriveInput) -> syn::Result<TokenStream2> {
    let name = &input.ident;

    // For now, assume this is a Newtype struct: `struct MyId(String);`
    // Wait, the specification says: "Обеспечивает маппинг в базовые типы БД".
    // A robust macro would inspect the inner type of the struct or the enum.
    // We will generate a basic wrapper that assumes a single unnamed field implementation.

    let syn::Data::Struct(ref data) = input.data else {
        return Err(Error::new_spanned(
            name,
            "OmniValue derive MVP only supports Newtype Structs",
        ));
    };

    let syn::Fields::Unnamed(ref fields) = data.fields else {
        return Err(Error::new_spanned(
            name,
            "OmniValue requires an unnamed field (tuple struct)",
        ));
    };

    if fields.unnamed.len() != 1 {
        return Err(Error::new_spanned(
            name,
            "OmniValue requires exactly 1 unnamed field",
        ));
    }

    let expanded = quote! {
        impl rustmemodb::core::omni_entity::OmniValue for #name {
            fn into_db_value(self) -> rustmemodb::core::Value {
                rustmemodb::core::omni_entity::OmniValue::into_db_value(self.0)
            }

            fn from_db_value(val: rustmemodb::core::Value) -> Result<Self, String> {
                let inner = rustmemodb::core::omni_entity::OmniValue::from_db_value(val)?;
                Ok(Self(inner))
            }
        }
    };

    Ok(expanded)
}
