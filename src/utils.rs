use avro_rs::types::Value;
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};

pub fn load_file(prefix: &Path, schema_name: &str) -> String {
    let path = prefix.join(schema_name);
    fs::read_to_string(&path).unwrap_or_else(|e| {
        panic!(
            "File {} cannot be loaded. Error is {:?}",
            &path.to_str().unwrap(),
            e
        )
    })
}

pub fn gen_hash_map(s: &HashMap<String, String>) -> Value {
    Value::Map(
        s.iter()
            .map(|(k, v)| (k.clone(), Value::String(v.clone())))
            .collect(),
    )
}

pub fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

pub fn fill_byte_array(buf: &mut [u8], from: &Vec<u8>) {
    let len = std::cmp::min(buf.len(), from.len());
    buf[..len].clone_from_slice(from.as_slice());
}

pub fn get_avro_path() -> String {
    let mut base_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    base_dir.push("API/avro/protocol");
    return String::from(base_dir.to_str().unwrap());
}
