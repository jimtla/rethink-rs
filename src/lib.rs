//#![feature(custom_derive, plugin)]
//#![plugin(serde_macros)]

extern crate serde_json;
extern crate byteorder;
use byteorder::{ReadBytesExt, WriteBytesExt, LittleEndian};
use std::io::prelude::*;
use std::net::TcpStream;
use std::str::{Utf8Error};
use std::string::{FromUtf8Error};
use std::fmt;
use std::io;
use std::convert::From;

use std::collections::BTreeMap;
use serde_json::Value;

#[derive(Debug)]
pub struct UnknownError {
  description: String
}

impl UnknownError {
  fn new(description : String) -> UnknownError {
    UnknownError{ description: description }
  }
}

impl fmt::Display for UnknownError {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    fmt::Display::fmt(&self.description, f)
  }
}

#[macro_use] extern crate wrapped_enum;

wrapped_enum!{
  #[derive(Debug)]
  /// More Docs
  pub enum Error {
    /// Converting bytes to utf8 string
    FromUtf8Error(FromUtf8Error),
    /// Utf8Error
    Utf8Error(Utf8Error),
    /// IO
    Io(io::Error),
    /// Byteorder
    Byteorder(byteorder::Error),
    /// Connection Error
    ConnectionError(String),
    /// Unknown Error
    UnknownError(UnknownError),
    /// serde json parsing error
    JsonParse(serde_json::Error),
  }
}

impl fmt::Display for Error {
  fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
    match *self {
      Error::FromUtf8Error(ref error) => fmt::Display::fmt(error, f),
      Error::Utf8Error(ref error) => fmt::Display::fmt(error, f),
      Error::Io(ref error) => fmt::Display::fmt(error, f),
      Error::Byteorder(ref error) => fmt::Display::fmt(error, f),
      Error::ConnectionError(ref s) => fmt::Display::fmt(s, f),
      Error::UnknownError(ref error) => fmt::Display::fmt(error, f),
      Error::JsonParse(ref error) => fmt::Display::fmt(error, f),
    }
  }
}


fn read_string<T: Read>(r : &mut T) -> Result<String, Error> {
  let bytes = try!(r.bytes().take_while(|b| {
    match *b {
      Err(_) => false,
      Ok(x) => x != 0,
    }
  }).collect::<Result<Vec<u8>,_>>());
  let s = try!(String::from_utf8(bytes));
  Ok(s)
}

pub struct Connection {
  stream: TcpStream,
  query_count: u64,
}

impl Connection {
  pub fn connect() -> Result<Connection, Error> {
    let stream = try!(TcpStream::connect("127.0.0.1:28015"));
    let mut conn = Connection{
      stream: stream,
      query_count: 0,
    };
    try!(conn.handshake());
    Ok(conn)
  }

  fn handshake(&mut self) -> Result<(), Error> {
    let v4 = 0x400c2d20u32;
    let json = 0x7e6970c7u32;
    try!(self.stream.write_u32::<LittleEndian>(v4));
    try!(self.stream.write_u32::<LittleEndian>(0));
    try!(self.stream.write_u32::<LittleEndian>(json));
    let res = try!(read_string(&mut self.stream));
    if res != "SUCCESS" {
      Err(Error::ConnectionError(res))
    } else {
      Ok(())
    }
  }

  pub fn exec<Q:Reql>(&mut self, query: Q) -> Result<serde_json::Value, Error> {
    let start = Value::Array(vec![Value::U64(1),
                                  query.as_json(),
                                  Value::Object(BTreeMap::new())]);
    let raw_query = try!(serde_json::ser::to_string(&start));
    println!("{:?}", raw_query);
    self.send(&raw_query)
  }

  fn send(&mut self, raw_string : &str) -> Result<serde_json::Value, Error> {
    self.query_count += 1;
    try!(self.stream.write_u64::<LittleEndian>(self.query_count));

    let bytes = raw_string.as_bytes();
    try!(self.stream.write_u32::<LittleEndian>(bytes.len() as u32));
    try!(self.stream.write_all(bytes));


    let query_token_resp = try!(self.stream.read_u64::<LittleEndian>());
    if query_token_resp != self.query_count {
      return Err(UnknownError::new(
        format!("Query token ({}) does not match {}",
        query_token_resp, self.query_count)
        ).into()
      );
    }

    let resp_len = self.stream.read_u32::<LittleEndian>().unwrap();
    let mut resp_bytes = Read::by_ref(&mut self.stream).take(resp_len as u64);
    serde_json::de::from_reader(&mut resp_bytes).map_err(Error::from)
  }
}



// 14
pub struct Database {
  name: String,
}

impl Database {
  pub fn new<S: Into<String>>(database: S) -> Self {
    Database{
      name: database.into(),
    }
  }

  pub fn table<S: Into<String>>(self, table: S) -> Table {
    Table{
      name: table.into(),
      db: self,
    }
  }
}

impl Reql for Database {
  fn as_json(&self) -> Value {
    Value::Array(vec![Value::U64(14),
                      Value::Array(vec![Value::String(self.name.clone())])])
  }

}


// 15
pub struct Table {
  name: String,
  db: Database,
}

impl Table {
  pub fn insert<O:Object>(self, object: O) -> Insert<O> {
    Insert{
      table: self,
      obj: object,
    }
  }
}

impl Reql for Table {
  fn as_json(&self) -> Value {
    Value::Array(vec![Value::U64(15),
                      Value::Array(vec![
                        self.db.as_json(),
                        Value::String(self.name.clone()),
                      ]),
                ])
  }
}

impl Sequence for Table {}

// 39 
pub struct Filter<S:Sequence,O:Object> {
  seq: S,
  obj: O,
}

impl<S:Sequence, O:Object> Reql for Filter<S,O> {
  fn as_json(&self) -> Value {
    Value::Array(vec![Value::U64(39),
                      Value::Array(vec![
                        self.seq.as_json(),
                        self.obj.as_json(),
                      ]),
                ])
  }
}

// 56 
pub struct Insert<O:Object> {
  table: Table,
  obj: O,
}

impl<O:Object> Reql for Insert<O> {
  fn as_json(&self) -> Value {
    Value::Array(vec![Value::U64(56),
                      Value::Array(vec![
                        self.table.as_json(),
                        self.obj.as_json(),
                      ]),
                ])
  }
}

pub trait Reql {
  fn as_json(&self) -> Value;
}

pub trait Sequence : Reql {
  fn filter<O:Object>(self, obj: O) -> Filter<Self,O> where Self: Sized {
    Filter{
      seq: self,
      obj: obj,
    }
  }
}

pub trait Object : Reql {}

impl Reql for Value {
  fn as_json(&self) -> Value {
    self.clone()
  }
}
impl Object for Value {}


#[test]
fn it_works() {
  let mut filter : BTreeMap<String, Value> = BTreeMap::new();
  filter.insert("name".into(), Value::String("Michael".into()));
  let query = Database::new("test").table("table").filter(Value::Object(filter));

  let mut conn = Connection::connect().unwrap();
  println!("{:?}", conn.exec(query).unwrap());
  panic!("ASDF");
}
