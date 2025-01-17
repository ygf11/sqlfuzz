// Copyright 2022 Andy Grove
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use datafusion::arrow::array::{Array, Int16Builder, Int32Builder, Int8Builder, StringBuilder};
use datafusion::arrow::datatypes::{DataType, Schema};
use datafusion::arrow::error::{ArrowError, Result};
use datafusion::arrow::record_batch::RecordBatch;
use rand::rngs::ThreadRng;
use rand::Rng;
use std::sync::Arc;

/// Generate an Arrow RecordBatch for the provided schema and row count
pub fn generate_batch(
    rng: &mut ThreadRng,
    schema: &Schema,
    row_count: usize,
) -> Result<RecordBatch> {
    let gen: Vec<Arc<dyn ArrayGenerator>> = schema
        .fields()
        .iter()
        .map(|f| match f.data_type() {
            DataType::Utf8 => Ok(Arc::new(StringGenerator {}) as Arc<dyn ArrayGenerator>),
            DataType::Int8 => Ok(Arc::new(Int8Generator {}) as Arc<dyn ArrayGenerator>),
            DataType::Int16 => Ok(Arc::new(Int16Generator {}) as Arc<dyn ArrayGenerator>),
            DataType::Int32 => Ok(Arc::new(Int32Generator {}) as Arc<dyn ArrayGenerator>),
            _ => Err(ArrowError::SchemaError("Unsupported data type".to_string())),
        })
        .collect::<Result<Vec<_>>>()?;
    let arrays = gen
        .iter()
        .map(|g| g.generate(rng, row_count))
        .collect::<Result<Vec<_>>>()?;
    RecordBatch::try_new(Arc::new(schema.clone()), arrays)
}

trait ArrayGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>>;
}

struct StringGenerator {}

impl ArrayGenerator for StringGenerator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = StringBuilder::new();
        for i in 0..n {
            if i % 7 == 0 {
                builder.append_null();
            } else {
                let mut str = String::new();
                for _ in 0..8 {
                    let ch = rng.gen_range(32..127); // printable ASCII chars
                    str.push(char::from_u32(ch).unwrap());
                }
                builder.append_value(str);
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

// TODO use generics to implement all primitive types

struct Int8Generator {}

impl ArrayGenerator for Int8Generator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = Int8Builder::new();
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null();
            } else {
                builder.append_value(rng.gen::<i8>());
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct Int16Generator {}

impl ArrayGenerator for Int16Generator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = Int16Builder::new();
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null();
            } else {
                builder.append_value(rng.gen::<i16>());
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}

struct Int32Generator {}

impl ArrayGenerator for Int32Generator {
    fn generate(&self, rng: &mut ThreadRng, n: usize) -> Result<Arc<dyn Array>> {
        let mut builder = Int32Builder::new();
        for i in 0..n {
            if i % 5 == 0 {
                builder.append_null();
            } else {
                builder.append_value(rng.gen::<i32>());
            }
        }
        Ok(Arc::new(builder.finish()))
    }
}
