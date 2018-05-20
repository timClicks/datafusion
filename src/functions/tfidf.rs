// Copyright 2018 Grove Enterprises LLC
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

//! TFIDF aggregate function

use std::rc::Rc;
use std::str;
use std::collections::HashMap;

use super::super::errors::*;
use super::super::types::*;

use arrow::datatypes::{Schema, Field, DataType};

pub struct TfidfFunction {
    /// map of word to index
    dictionary: HashMap<String,usize>,
    /// array of word counts
    word_counts: Vec<usize>
}

impl TfidfFunction {
    pub fn new() -> Self {
        TfidfFunction {
            dictionary: HashMap::new(),
            word_counts: Vec::with_capacity(1024*1024)
        }

    }
}

impl AggregateFunction for TfidfFunction {

    fn name(&self) -> String {
        "count_words".to_string()
    }

    fn args(&self) -> Vec<Field> {
        vec![Field::new("words", DataType::Utf8, true)]
    }

    fn return_type(&self) -> DataType {
        //TODO: need to return an array of (word, count) pairs
        DataType::Utf8
    }

    fn execute(&mut self, args: &[Value]) -> Result<()> {
        println!("execute()");
        assert_eq!(1, args.len());
        match args[0] {
            Value::Column(ref array) => match array.data() {
                ArrayData::Utf8(str) => {
                    println!("Column contains {} strings", str.len());


                    // for each row
                    for i in 0..str.len() {
                        // seems expensive to do all this conversion ?
                        let row = str::from_utf8(str.get(i)).unwrap().to_string();

                        // crude parsing into words
                        let words = row.split_whitespace();
                        words.for_each(|word| {
                            let word_as_string = word.to_string();
                            let index = self.dictionary.entry(word_as_string).or_insert(self.word_counts.len());
                            if *index == self.word_counts.len() {
                                self.word_counts.push(1);
                            } else {
                                self.word_counts[ * index] += 1;
                            }
                        });
                    }
                }
                other => panic!("Unsupported data type for TFIDF")
            }
            _ => panic!("TFIDF does not support scalar values")
        }
        Ok(())
    }

    fn finish(&self) -> Result<Value> {
        let mut s = "".to_string();
        for (word, i) in &self.dictionary {
            s += &format!("{}={} ", word, i);
        }
        Ok(Value::Scalar(Rc::new(ScalarValue::Utf8(Rc::new(s)))))
    }
}
