use std::collections::HashMap;
use std::path::Path;
use std::str;

use avro_rs::{from_avro_datum, Schema, to_avro_datum};
use avro_rs::schema::Name;
use avro_rs::types::{Record, Value};

use crate::utils;

pub mod media_store;
pub mod services;

type SchemaDirectory = HashMap<String, Schema>;

pub const STORAGE_SCHEMAS: &str = "storage";
pub const TRACK_TYPE_SCHEMA: &str = "insight.storage.TrackType.avsc";
pub const TRACK_INFO_SCHEMA: &str = "insight.storage.TrackInfo.avsc";
pub const UNIT_SCHEMA: &str = "insight.storage.Unit.avsc";
pub const UNIT_ELEMENT_MESSAGE_SCHEMA: &str = "insight.storage.UnitElementMessage.avsc";
pub const UNIT_ELEMENT_VALUE_SCHEMA: &str = "insight.storage.UnitElementValue.avsc";

pub const TRANSPORT_SCHEMAS: &str = "transport";
pub const NOTIFY_MESSAGE_SCHEMA: &str = "insight.transport.NotifyMessage.avsc";
pub const STREAM_TRACKS_REQUEST_SCHEMA: &str = "insight.transport.StreamTracksRequest.avsc";
pub const STREAM_TRACKS_RESPONSE_SCHEMA: &str = "insight.transport.StreamTracksResponse.avsc";
pub const STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA: &str = "insight.transport.StreamTrackUnitElementsRequest.avsc";
pub const STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA: &str = "insight.transport.StreamTrackUnitElementsResponse.avsc";
pub const STREAM_TRACK_UNITS_REQUEST_SCHEMA: &str = "insight.transport.StreamTrackUnitsRequest.avsc";
pub const STREAM_TRACK_UNITS_RESPONSE_SCHEMA: &str = "insight.transport.StreamTrackUnitsResponse.avsc";
pub const MESSAGE_ENVELOPE_SCHEMA: &str = "insight.transport.MessageEnvelope.avsc";
pub const PING_REQUEST_RESPONSE_SCHEMA: &str = "insight.transport.PingRequestResponse.avsc";

pub const SERVICE_FFPROBE_SCHEMAS: &str = "services/ffprobe";
pub const SERVICES_FFPROBE_REQUEST_SCHEMA: &str = "insight.ffprobe.Request.avsc";
pub const SERVICES_FFPROBE_RESPONSE_SCHEMA: &str = "insight.ffprobe.Response.avsc";


pub struct MessageBuilder {
    pub directory: SchemaDirectory,
}

impl MessageBuilder {
    pub fn schema_files() -> Vec<(&'static str, &'static str)> {
        vec![
            (STORAGE_SCHEMAS, TRACK_TYPE_SCHEMA),
            (STORAGE_SCHEMAS, TRACK_INFO_SCHEMA),
            (STORAGE_SCHEMAS, UNIT_SCHEMA),
            (STORAGE_SCHEMAS, UNIT_ELEMENT_VALUE_SCHEMA),
            (STORAGE_SCHEMAS, UNIT_ELEMENT_MESSAGE_SCHEMA),
            (TRANSPORT_SCHEMAS, NOTIFY_MESSAGE_SCHEMA),
            (TRANSPORT_SCHEMAS, STREAM_TRACKS_REQUEST_SCHEMA),
            (TRANSPORT_SCHEMAS, STREAM_TRACKS_RESPONSE_SCHEMA),
            (TRANSPORT_SCHEMAS, STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA),
            (TRANSPORT_SCHEMAS, STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA),
            (TRANSPORT_SCHEMAS, STREAM_TRACK_UNITS_REQUEST_SCHEMA),
            (TRANSPORT_SCHEMAS, STREAM_TRACK_UNITS_RESPONSE_SCHEMA),
            (TRANSPORT_SCHEMAS, PING_REQUEST_RESPONSE_SCHEMA),
            (TRANSPORT_SCHEMAS, MESSAGE_ENVELOPE_SCHEMA),
            (SERVICE_FFPROBE_SCHEMAS, SERVICES_FFPROBE_REQUEST_SCHEMA),
            (SERVICE_FFPROBE_SCHEMAS, SERVICES_FFPROBE_RESPONSE_SCHEMA),
        ]
    }

    pub fn new(path_prefix: &str) -> MessageBuilder {
        let schemas_raw: Vec<String> = Self::schema_files()
            .iter()
            .map(|schema| utils::load_file(Path::new(path_prefix).join(Path::new(schema.0)).as_path(), schema.1))
            .collect();
        let schemas_raw_str: Vec<&str> = schemas_raw.iter().map(|s| s.as_str()).collect();

        let schemas = Schema::parse_list(&schemas_raw_str).unwrap();
        let mut named_schemas = HashMap::default();

        for s in &schemas {
            match s {
                Schema::Enum {
                    name:
                    Name {
                        name,
                        namespace,
                        aliases: _,
                    },
                    doc: _,
                    symbols: _,
                } => {
                    let mut full_name = namespace.clone().unwrap_or(String::from("insight.transport"));
                    full_name.push_str(".");
                    full_name.push_str(name);
                    full_name.push_str(".avsc");
                    named_schemas.insert(full_name, s.clone());
                }
                Schema::Record {
                    name:
                    Name {
                        name,
                        namespace,
                        aliases: _,
                    },
                    doc: _,
                    fields: _,
                    lookup: _,
                } => {
                    let mut full_name = namespace.clone().unwrap_or(String::from("insight.transport"));
                    full_name.push_str(".");
                    full_name.push_str(name);
                    full_name.push_str(".avsc");
                    named_schemas.insert(full_name, s.clone());
                }
                _ => {
                    dbg!(s);
                }
            };
        }

        MessageBuilder {
            directory: named_schemas,
        }
    }

    #[inline]
    pub fn get_schema(&self, schema_name: &str) -> Option<&Schema> {
        self.directory.get(&String::from(schema_name))
    }

    #[inline]
    fn get_record(&self, schema_name: &str) -> Record {
        let record = Record::new(self.get_schema(schema_name).unwrap()).unwrap();
        record
    }

    fn pack_message_into_envelope(&self, schema_name: &str, payload: Record) -> Vec<u8> {
        let mut envelope = self.get_record(MESSAGE_ENVELOPE_SCHEMA);
        let inner = to_avro_datum(self.get_schema(schema_name).unwrap(), payload).unwrap();
        envelope.put("schema", Value::Bytes(schema_name.into()));
        envelope.put("payload", Value::Bytes(inner));
        to_avro_datum(self.get_schema(MESSAGE_ENVELOPE_SCHEMA).unwrap(), envelope).unwrap()
    }

    pub fn read_protocol_message(&self, from: &Vec<u8>) -> Result<(String, Value), String> {
        let envelope_schema = self.get_schema(MESSAGE_ENVELOPE_SCHEMA).unwrap();
        let envelope = from_avro_datum(&envelope_schema, &mut from.as_slice(), None);

        match envelope {
            Ok(envelope) => match envelope {
                Value::Record(fields) => match fields.as_slice() {
                    [(s_field_name, Value::Bytes(schema)), (p_field_name, Value::Bytes(payload))]
                    if s_field_name == "schema" && p_field_name == "payload" =>
                        {
                            let schema = str::from_utf8(schema.as_slice());
                            match schema {
                                Ok(schema_name) => {
                                    let inner_schema = self.get_schema(schema_name);

                                    match inner_schema {
                                        Some(inner_schema) => {
                                            let inner = from_avro_datum(inner_schema,
                                                                        &mut payload.clone().as_slice(), None);

                                            match inner {
                                                Ok(inner) => Ok((String::from(schema_name), inner)),
                                                _ => Err(String::from("Failed to parse inner AVRO serialized record"))
                                            }
                                        }
                                        _ => Err(format!("No valid schema found in schema catalog for the schema ({}) in serialized record", schema_name))
                                    }
                                }
                                _ => Err(String::from(
                                    "Failed to parse schema name, not a valid UTF-8",
                                )),
                            }
                        }
                    _ => Err(String::from(
                        "No outer AVRO record (MessageEnvelope) matched",
                    )),
                },
                _ => Err(String::from("Failed to parse/match outer AVRO Record")),
            },
            _ => Err(String::from("Failed to deserialize the outer message")),
        }
    }
}



