use serde::{Serialize, Deserialize};
use proc_macro::TokenStream;
use quote::quote;
use syn::*;
use protolith_core::api::prost_wkt_types;
use protolith_core::collection::Wrapper;
use prost_wkt_types::Struct;

#[proc_macro_derive(Collection)]
pub fn collection_derive(input: TokenStream) -> TokenStream {
    // Parse the input tokens into a syntax tree
    let ast = parse_macro_input!(input as DeriveInput);

    // Build the implementation
    impl_collection(&ast)
}

fn impl_collection(ast: &syn::DeriveInput) -> TokenStream {
    let name = &ast.ident;
    let name_str = name.to_string(); // Convert the ident to a string

    let gen = quote! {
        impl #name {

            pub fn serialize(&self) -> Wrapper {
                let inner = serde_json::to_string(&self).expect("Serialization failed");
                Wrapper {
                    id: 0, // You need to define how to generate or provide the ID
                    collection: #name_str.to_string(),
                    inner,
                }
            }

            pub fn deserialize(wrapper: Wrapper) -> Self {
                assert_eq!(wrapper.collection, #name_str, "Mismatched struct types");
                serde_json::from_str(&wrapper.inner).expect("Deserialization failed")
            }

            pub fn into_struct(wrapper: Wrapper) -> Struct {
                serde_json::from_str(&wrapper.inner).expect("Deserialization failed")
            }

        }
    };

    gen.into()
}