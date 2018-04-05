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

use std::io::prelude::*;
use std::io::{BufReader, BufWriter};
use std::fs::File;
use std::mem;

use arrow::datatypes::*;
use arrow::array::*;

extern crate byteorder;
use self::byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};


fn write_array(a: &Array, w: &mut BufWriter<File>) {
    let len : usize = a.len();
    w.write_i32::<LittleEndian>(a.len() as i32).unwrap();
    match a.data() {
        &ArrayData::Int32(ref b) => {
            w.write_u8(1).unwrap();
            b.iter().for_each(|v| w.write_i32::<LittleEndian>(v).unwrap() )
        },
        &ArrayData::Int64(ref b) => {
            w.write_u8(2).unwrap();
            b.iter().for_each(|v| w.write_i64::<LittleEndian>(v).unwrap() )
        },
        _ => panic!()
    }
}

fn read_array(r: &mut BufReader<File>) -> Array {
    let len = r.read_i32::<LittleEndian>().unwrap() as usize;
    let arr_type = r.read_u8().unwrap();
    match arr_type {
        1 => {
            let mut v : Vec<i32> = Vec::with_capacity(len);
            for _ in 0..len {
                v.push(r.read_i32::<LittleEndian>().unwrap());
            }
            println!("read {:?}", v);
            Array::from(v)
        },
        _ => panic!()
    }

}

//fn write_column(col: &ArrayData) {
//
//    let mut stream = BufWriter::new(TcpStream::connect("127.0.0.1:34254").unwrap());
//
//    use std::slice;
//    use std::mem;
//
//    //stream.write_u32(col.len() as u32).unwrap();
//    let slice_u8: &[u8] = match col {
//        &ArrayData::Float32(ref v) => {
//            let slice : &[f32] = &v;
//            unsafe {
//                slice::from_raw_parts(
//                    slice.as_ptr() as *const u8,
//                    slice.len() * mem::size_of::<f32>(),
//                )
//            }
//        },
//        &ArrayData::Float64(ref v) => {
//            let slice : &[f64] = &v;
//            unsafe {
//                slice::from_raw_parts(
//                    slice.as_ptr() as *const u8,
//                    slice.len() * mem::size_of::<f64>(),
//                )
//            }
//        },
//        _ => unimplemented!()
//    };
//
//    let mut wtr = vec![];
//    wtr.write_u32::<LittleEndian>(col.len() as u32).unwrap();
//    stream.write(&wtr).unwrap();
//    stream.write(slice_u8).unwrap();
//
//
//}


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_read_write_array() {
        let a =  Array::from(vec![1,2,3,4,5]);

        {
            let mut writer= BufWriter::new(File::create("array.df").unwrap());
            write_array(&a, &mut writer);
        }

        let mut reader = BufReader::new(File::open("array.df").unwrap());
        let b = read_array(&mut reader);

//        println!("a = {:?}", a);
//        println!("b = {:?}", b);

    }
}
