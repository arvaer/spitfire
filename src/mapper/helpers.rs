use std::collections::HashMap;
use lazy_static::lazy_static;

lazy_static! {
    pub static ref PARTITION_MAP: HashMap<i32, Vec<char>> = {
        let mut m = HashMap::new();
        m.insert(1, vec!['a', 'b', 'c', 'd', 'e', 'f']);
        m.insert(2, vec!['g', 'h', 'i', 'j', 'k', 'l']);
        m.insert(3, vec!['m', 'n', 'o', 'p', 'q', 'r']);
        m.insert(4, vec!['s', 't', 'u', 'v', 'w', 'x']);
        m.insert(5, vec!['y', 'z']);
        m
    };
}

