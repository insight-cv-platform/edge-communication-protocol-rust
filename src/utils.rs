use std::path::Path;
use std::fs;


pub fn load_file(prefix: &Path, schema_name: &str) -> String {
    let path = format!("insight.edge.{}.avsc", schema_name);
    fs::read_to_string(prefix.join(path)).unwrap()
}
