
3
4
5
6
7
8
9
10
11
12
13
14
15
16
17
extern crate syntex;
extern crate serde_codegen;

use std::env;
use std::path::Path;

fn main() {
    let out_dir = env::var_os("OUT_DIR").unwrap();

    let src = Path::new("src/lib.rs.in");
    let dst = Path::new(&out_dir).join("lib.rs");

    let mut registry = syntex::Registry::new();

    serde_codegen::register(&mut registry);
    registry.expand("", &src, &dst).unwrap();
}