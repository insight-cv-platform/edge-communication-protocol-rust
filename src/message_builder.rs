use std::collections::HashMap;
use std::path::Path;
use std::str;

use avro_rs::schema::Name;
use avro_rs::types::{Record, Value};
use avro_rs::{from_avro_datum, to_avro_datum, Schema};
use crate::{ElementType, Payload, StreamName, TrackName, TrackType, utils};

type SchemaDirectory = HashMap<String, Schema>;

pub const TRACK_TYPE_SCHEMA: &str = "insight.transport.TrackType.avsc";
pub const TRACK_INFO_SCHEMA: &str = "insight.transport.TrackInfo.avsc";
pub const UNIT_SCHEMA: &str = "insight.transport.Unit.avsc";
pub const UNIT_ELEMENT_MESSAGE_SCHEMA: &str = "insight.transport.UnitElementMessage.avsc";
pub const NOTIFY_MESSAGE_SCHEMA: &str = "insight.transport.NotifyMessage.avsc";
pub const STREAM_TRACKS_REQUEST_SCHEMA: &str = "insight.transport.StreamTracksRequest.avsc";
pub const STREAM_TRACKS_RESPONSE_SCHEMA: &str = "insight.transport.StreamTracksResponse.avsc";
pub const STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA: &str = "insight.transport.StreamTrackUnitElementsRequest.avsc";
pub const STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA: &str = "insight.transport.StreamTrackUnitElementsResponse.avsc";
pub const STREAM_TRACK_UNITS_REQUEST_SCHEMA: &str = "insight.transport.StreamTrackUnitsRequest.avsc";
pub const STREAM_TRACK_UNITS_RESPONSE_SCHEMA: &str = "insight.transport.StreamTrackUnitsResponse.avsc";
pub const MESSAGE_ENVELOPE_SCHEMA: &str = "insight.transport.MessageEnvelope.avsc";
pub const PING_REQUEST_RESPONSE_SCHEMA: &str = "insight.transport.PingRequestResponse.avsc";
pub const UNIT_ELEMENT_VALUE_SCHEMA: &str = "insight.transport.UnitElementValue.avsc";

pub struct MessageBuilder {
    pub directory: SchemaDirectory,
}

impl MessageBuilder {
    pub fn schema_files() -> Vec<String> {
        vec![
            String::from(TRACK_TYPE_SCHEMA),
            String::from(TRACK_INFO_SCHEMA),
            String::from(UNIT_SCHEMA),
            String::from(UNIT_ELEMENT_VALUE_SCHEMA),
            String::from(UNIT_ELEMENT_MESSAGE_SCHEMA),
            String::from(NOTIFY_MESSAGE_SCHEMA),
            String::from(STREAM_TRACKS_REQUEST_SCHEMA),
            String::from(STREAM_TRACKS_RESPONSE_SCHEMA),
            String::from(STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA),
            String::from(STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA),
            String::from(STREAM_TRACK_UNITS_REQUEST_SCHEMA),
            String::from(STREAM_TRACK_UNITS_RESPONSE_SCHEMA),
            String::from(PING_REQUEST_RESPONSE_SCHEMA),
            String::from(MESSAGE_ENVELOPE_SCHEMA),
        ]
    }

    pub fn new(path_prefix: &str) -> MessageBuilder {
        let schemas_raw: Vec<String> = Self::schema_files()
            .iter()
            .map(|schema_name| utils::load_file(Path::new(path_prefix), schema_name))
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

    fn get_track_type_enum(track_type: &TrackType) -> Value {
        match track_type {
            TrackType::Video => Value::Enum(0, "VIDEO".into()),
            TrackType::Meta => Value::Enum(1, "META".into()),
            TrackType::NotImplemented => panic!("Not implemented track type")
        }
    }

    fn get_stream_unit(
        stream_name: StreamName,
        track_type: &TrackType,
        track_name: TrackName,
        unit: i64,
    ) -> Value {
        Value::Record(vec![
            ("stream_name".into(), Value::Bytes(stream_name.to_vec())),
            ("track_name".into(), Value::Bytes(track_name.to_vec())),
            ("track_type".into(), Self::get_track_type_enum(track_type)),
            ("unit".into(), Value::Long(unit)),
        ])
    }

    fn pack_message_into_envelope(&self, schema_name: &str, payload: Record) -> Vec<u8> {
        let mut envelope = self.get_record(MESSAGE_ENVELOPE_SCHEMA);
        let inner = to_avro_datum(self.get_schema(schema_name).unwrap(), payload).unwrap();
        envelope.put("schema", Value::Bytes(schema_name.into()));
        envelope.put("payload", Value::Bytes(inner));
        to_avro_datum(self.get_schema(MESSAGE_ENVELOPE_SCHEMA).unwrap(), envelope).unwrap()
    }

    pub fn build_notify_message(
        &self,
        stream_name: StreamName,
        track_type: &TrackType,
        track_name: TrackName,
        unit: i64,
        saved_ms: u64,
        last_element: Option<ElementType>,
    ) -> Vec<u8> {
        let mut record = self.get_record(NOTIFY_MESSAGE_SCHEMA);
        record.put(
            "stream_unit",
            Self::get_stream_unit(stream_name, track_type, track_name, unit),
        );
        record.put("saved_ms", Value::Long(saved_ms as i64));
        match last_element {
            Some(elt) => {
                record.put("notify_type".into(), Value::Enum(0, "READY".into()));
                record.put("last_element".into(), Value::Int(elt.into()));
            }
            None => {
                record.put("notify_type".into(), Value::Enum(1, "NEW".into()));
                record.put("last_element".into(), Value::Int(-1));
            }
        }

        self.pack_message_into_envelope(NOTIFY_MESSAGE_SCHEMA, record)
    }

    pub fn build_ping_request_response(&self, request_id: i64, topic: String, is_response: bool) -> Vec<u8> {
        let mut record = self.get_record(PING_REQUEST_RESPONSE_SCHEMA);
        record.put("request_id", Value::Long(request_id));
        record.put("topic", Value::String(topic));
        if is_response {
            record.put("type".into(), Value::Enum(1, "RESPONSE".into()));
        } else {
            record.put("type".into(), Value::Enum(0, "REQUEST".into()));
        }
        self.pack_message_into_envelope(PING_REQUEST_RESPONSE_SCHEMA, record)
    }

    pub fn build_unit_element_message(
        &self,
        stream_name: StreamName,
        track_type: &TrackType,
        track_name: TrackName,
        unit: i64,
        element: ElementType,
        value: &Vec<u8>,
        attributes: &HashMap<String, String>,
        last: bool,
    ) -> Vec<u8> {
        let mut record = self.get_record(UNIT_ELEMENT_MESSAGE_SCHEMA);
        record.put(
            "stream_unit",
            Self::get_stream_unit(stream_name, track_type, track_name, unit),
        );
        record.put("element", Value::Long(element.into()));
        record.put("value", Value::Bytes(value.clone()));
        record.put("attributes", Value::Map(attributes.iter().map(|x| (x.0.clone(), Value::String(x.1.clone()))).collect()));
        record.put("last", Value::Boolean(last));
        self.pack_message_into_envelope(UNIT_ELEMENT_MESSAGE_SCHEMA, record)
    }

    pub fn build_stream_tracks_request(
        &self,
        request_id: i64,
        topic: String,
        stream_name: StreamName,
    ) -> Vec<u8> {
        let mut record = self.get_record(STREAM_TRACKS_REQUEST_SCHEMA);
        record.put("request_id", Value::Long(request_id));
        record.put("topic", Value::String(topic));
        record.put("stream_name", Value::Bytes(stream_name.to_vec()));
        self.pack_message_into_envelope(STREAM_TRACKS_REQUEST_SCHEMA, record)
    }

    pub fn build_stream_tracks_respose(
        &self,
        request_id: i64,
        stream_name: StreamName,
        tracks: &Vec<(TrackName, TrackType)>,
    ) -> Vec<u8> {
        let mut record = self.get_record(STREAM_TRACKS_RESPONSE_SCHEMA);
        record.put("request_id", Value::Long(request_id));
        record.put("stream_name", Value::Bytes(stream_name.to_vec()));
        let tracks: Vec<Value> = tracks
            .iter()
            .map(|(track_name, track_type)| {
                let mut r = self.get_record(TRACK_INFO_SCHEMA);
                r.put("name", Value::Bytes(track_name.to_vec()));
                r.put("type", Self::get_track_type_enum(track_type));
                r.into()
            })
            .collect();
        record.put("tracks", Value::Array(tracks));
        self.pack_message_into_envelope(STREAM_TRACKS_RESPONSE_SCHEMA, record)
    }

    pub fn build_stream_track_unit_elements_request(
        &self,
        request_id: i64,
        topic: String,
        stream_name: StreamName,
        track_type: &TrackType,
        track_name: TrackName,
        unit: i64,
        max_element: ElementType,
    ) -> Vec<u8> {
        let mut record = self.get_record(STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA);
        record.put("request_id", Value::Long(request_id));
        record.put("topic", Value::String(topic));
        record.put(
            "stream_unit",
            Self::get_stream_unit(stream_name, track_type, track_name, unit),
        );
        record.put("max_element", Value::Long(max_element.into()));
        self.pack_message_into_envelope(STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA, record)
    }

    pub fn build_stream_track_unit_elements_response(
        &self,
        request_id: i64,
        stream_name: StreamName,
        track_type: &TrackType,
        track_name: TrackName,
        unit: i64,
        values: &Vec<Payload>,
    ) -> Vec<u8> {
        let mut record = self.get_record(STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA);
        record.put("request_id", Value::Long(request_id));
        record.put(
            "stream_unit",
            Self::get_stream_unit(stream_name, track_type, track_name, unit),
        );

        fn payload_to_avro(p: &Payload) -> Value {
            Value::Record(vec![
                ("data".into(), Value::Bytes(p.data.clone())),
                ("attributes".into(), Value::Map(p.attributes.iter().map(|x| (x.0.clone(), Value::String(x.1.clone()))).collect())),
            ])
        }

        let values: Vec<Value> = values.iter().map(|x| payload_to_avro(x)).collect();
        record.put("values", Value::Array(values));
        self.pack_message_into_envelope(STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA, record)
    }

    pub fn build_stream_track_units_request(
        &self,
        request_id: i64,
        topic: String,
        stream_name: StreamName,
        track_type: &TrackType,
        track_name: TrackName,
        from_ms: i64,
        to_ms: i64,
    ) -> Vec<u8> {
        let mut record = self.get_record(STREAM_TRACK_UNITS_REQUEST_SCHEMA);
        record.put("request_id", Value::Long(request_id));
        record.put("topic", Value::String(topic));
        record.put(
            "stream_unit",
            Self::get_stream_unit(stream_name, track_type, track_name, 0),
        );
        record.put("from_ms", Value::Long(from_ms));
        record.put("to_ms", Value::Long(to_ms));
        self.pack_message_into_envelope(STREAM_TRACK_UNITS_REQUEST_SCHEMA, record)
    }

    pub fn build_stream_track_units_response(
        &self,
        request_id: i64,
        stream_name: StreamName,
        track_type: &TrackType,
        track_name: TrackName,
        from_ms: i64,
        to_ms: i64,
        units: &Vec<i64>,
    ) -> Vec<u8> {
        let mut record = self.get_record(STREAM_TRACK_UNITS_RESPONSE_SCHEMA);
        record.put("request_id", Value::Long(request_id));
        record.put(
            "stream_unit",
            Self::get_stream_unit(stream_name, track_type, track_name, 0),
        );
        record.put("from_ms", Value::Long(from_ms));
        record.put("to_ms", Value::Long(to_ms));
        let values: Vec<Value> = units.iter().map(|x| Value::Long(*x)).collect();
        record.put("units", Value::Array(values));
        self.pack_message_into_envelope(STREAM_TRACK_UNITS_RESPONSE_SCHEMA, record)
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


