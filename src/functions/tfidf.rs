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
use arrow::array::ListArray;
use arrow::list_builder::ListBuilder;

pub struct TfidfFunction {
    /// map of word to index
    dictionary: HashMap<String,usize>,
    /// array of word counts
    word_counts: Vec<usize>,
    /// per-row word counts (each row has a vec of (word_index, word_count)
    per_row_word_counts: Vec<Vec<(String,usize)>> //TODO: dictionary encode here too
}

impl TfidfFunction {
    pub fn new() -> Self {
        TfidfFunction {
            dictionary: HashMap::new(),
            word_counts: Vec::with_capacity(1024*1024),
            per_row_word_counts: Vec::with_capacity(1024*1024)
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
                    //println!("Column contains {} strings", str.len());
                    // for each row
                    for i in 0..str.len() {
                        // seems expensive to do all this conversion ?
                        let row = str::from_utf8(str.get(i)).unwrap().to_string();

                        let mut row_word_counts: HashMap<String,usize> = HashMap::new();

                        // crude parsing into words
                        let words = row.split_whitespace();
                        words.for_each(|word| {
                            let word_as_string = word.to_string();
                            // get index into word counts
                            let word_index = self.dictionary.entry(word_as_string).or_insert(self.word_counts.len());
                            // increase total word counts
                            if *word_index == self.word_counts.len() {
                                println!("New word: {}", word);
                                self.word_counts.push(1);
                            } else {
                                println!("Repeated word: {}", word);
                                self.word_counts[ * word_index] += 1;
                            }
                            // record this row word counts
                            let c = match row_word_counts.get(&word.to_string()) {
                                Some(c) => c + 1,
                                None => 1
                            };
                            row_word_counts.insert(word.to_string(), c);

                        });

                        let row_summary: Vec<(String,usize)> = row_word_counts.iter()
                            .map(|(a,b)| (a.clone(),*b))
                            .collect();
                        self.per_row_word_counts.push(row_summary);
                    }
                }
                other => panic!("Unsupported data type for TFIDF")
            }
            _ => panic!("TFIDF does not support scalar values")
        }
        Ok(())
    }

    fn finish(&self) -> Result<Value> {
        // now do the aggregation part
        // for now return results in string format instead of structured
        let mut b: ListBuilder<u8> = ListBuilder::new();
        for row_summary in &self.per_row_word_counts {

            // build aggregate data for this row
            let mut s = "".to_string();
            for (word,count) in row_summary {
                let word_index = self.dictionary.get(word).unwrap();
                let total_word_count = self.word_counts[*word_index];
                s += &format!("{}={}/{} ", word, count, total_word_count);
            }

            println!("ROW SUMMARY: {}", s);
            b.push(s.as_bytes());
        }

        Ok(Value::Column(Rc::new(Array::new(self.per_row_word_counts.len(),
                                    ArrayData::Utf8(ListArray::from(b.finish()))))))
    }
}
