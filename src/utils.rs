use std::collections::HashMap;
use std::path::Path;
use std::fs;
use avro_rs::types::Value;


pub fn load_file(prefix: &Path, schema_name: &str) -> String {
    let path = prefix.join(schema_name);
    dbg!(&path);
    fs::read_to_string(&path).expect(format!("File {} cannot be loaded", &path.to_str().unwrap()).as_str())
}

pub fn gen_hash_map(s: &HashMap<String, String>) -> Value {
    Value::Map(s.iter().map(|(k, v)| (k.clone(), Value::String(v.clone()))).collect())
}
