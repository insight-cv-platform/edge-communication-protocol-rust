use std::path::Path;
use std::fs;


pub fn load_file(prefix: &Path, schema_name: &str) -> String {
    fs::read_to_string(prefix.join(schema_name)).unwrap()
}
