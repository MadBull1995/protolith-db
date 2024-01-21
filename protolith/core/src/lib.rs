use std::{marker::PhantomData, any::{Any, TypeId}};

use api::pbjson_types::Value;
use prost_reflect::Kind;
pub use protolith_api as api;
pub use protolith_tracing as trace;
pub use protolith_error as error;
pub mod meta_store;
pub mod db;
pub mod schema;
use serde::{Serialize, Deserialize}; // Make sure to add serde traits

// Define a struct for your key wrapper, now generic over T
pub struct Key<T> {
    key: T,
    raw_key: Vec<u8>,
    expected_type: Kind,
    _marker: PhantomData<T>, // This is to make Key generic over T without storing T
}

impl<T: Serialize + Any> Key<T> {
    // Create a new Key, automatically determining the expected type and serializing T
    pub fn new(key_data: T) -> Self {
        let raw_key = serde_json::to_vec(&key_data).expect("Serialization failed");
        let expected_type = Self::determine_expected_type();
        
        Self {
            key: key_data,
            raw_key,
            expected_type,
            _marker: PhantomData,
        }
    }
    
    // Internal method to determine the expected Protobuf type based on T
    pub fn determine_expected_type() -> Kind {
        if TypeId::of::<T>() == TypeId::of::<i32>() {
            Kind::Int32
        } else if TypeId::of::<T>() == TypeId::of::<i64>() {
            Kind::Int64
        } else if TypeId::of::<T>() == TypeId::of::<u32>() {
            Kind::Uint32
        } else if TypeId::of::<T>() == TypeId::of::<u64>() {
            Kind::Uint64
        } else if TypeId::of::<T>() == TypeId::of::<String>() {
            Kind::String
        } else if TypeId::of::<T>() == TypeId::of::<f64>() {
            Kind::Double
        } else if TypeId::of::<T>() == TypeId::of::<f32>() {
            Kind::Float
        } else if TypeId::of::<T>() == TypeId::of::<&str>() {
            Kind::String
        } // ... handle other types
        else {
            panic!("Unsupported type for Key");
        }
    }

    pub fn validate(&self) -> bool {
        match self.expected_type {
            Kind::Int32 
            | Kind::Int64 
            | Kind::Uint32 
            | Kind::Uint64 => {
                // Validate that the key is an integer (works for both signed and unsigned integers)
                self.raw_key.iter().all(|&byte| byte.is_ascii_digit())
            },
            Kind::Double | Kind::Float => {
                serde_json::from_slice::<f64>(&self.raw_key).is_ok()
            },
            Kind::String => {
                std::str::from_utf8(&self.raw_key).is_ok()
            },
            // Add validation for other types as needed
            _ => false,
        }
    }

    pub fn as_value(&self) -> Value {
        match self.expected_type {
            Kind::Int32 
            | Kind::Int64 
            | Kind::Uint32 
            | Kind::Uint64
            | Kind::Float
            | Kind::Double => {
                Value {
                    kind: Some(
                        api::pbjson_types::value::Kind::NumberValue(
                            f64::from_be_bytes(self.raw_key.clone().try_into().unwrap())
                        )
                    )
                }                
            },
            Kind::String => {
                Value {
                    kind: Some(
                        api::pbjson_types::value::Kind::StringValue(
                            String::from_utf8(self.raw_key.clone().try_into().unwrap()).unwrap()
                        )
                    )
                }   
            },
            _ => panic!("Unsupported type for conversion to protobuf Value"),
        } 
    }
}