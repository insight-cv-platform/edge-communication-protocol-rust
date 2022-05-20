use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::str;

use crate::objects::services::ffprobe::{ServicesFFProbeRequest, ServicesFFProbeResponse};
use crate::objects::services::keep_alive::KeepAliveMessage;
use crate::objects::services::ping::PingRequestResponse;
use crate::objects::services::storage::notify_message::NotifyMessage;
use crate::objects::services::storage::stream_track_unit_elements::{
    StreamTrackUnitElementsRequest, StreamTrackUnitElementsResponse,
};
use crate::objects::services::storage::stream_track_units::{
    StreamTrackUnitsRequest, StreamTrackUnitsResponse,
};
use crate::objects::services::storage::stream_tracks::{StreamTracksRequest, StreamTracksResponse};
use crate::objects::services::storage::unit_element_message::UnitElementMessage;
use crate::objects::{FromProtocolMessage, ToProtocolMessage};
use avro_rs::schema::Name;
use avro_rs::types::{Record, Value};
use avro_rs::{from_avro_datum, to_avro_datum, Schema};
use log::warn;
use pyo3::PyClass;

use crate::utils;

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
pub const STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA: &str =
    "insight.transport.StreamTrackUnitElementsRequest.avsc";
pub const STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA: &str =
    "insight.transport.StreamTrackUnitElementsResponse.avsc";
pub const STREAM_TRACK_UNITS_REQUEST_SCHEMA: &str =
    "insight.transport.StreamTrackUnitsRequest.avsc";
pub const STREAM_TRACK_UNITS_RESPONSE_SCHEMA: &str =
    "insight.transport.StreamTrackUnitsResponse.avsc";
pub const MESSAGE_ENVELOPE_SCHEMA: &str = "insight.transport.MessageEnvelope.avsc";
pub const PING_REQUEST_RESPONSE_SCHEMA: &str = "insight.transport.PingRequestResponse.avsc";
pub const KEEPALIVE_MESSAGE_SCHEMA: &str = "insight.transport.KeepAliveMessage.avsc";

pub const SERVICE_FFPROBE_SCHEMAS: &str = "services/ffprobe";
pub const SERVICES_FFPROBE_REQUEST_SCHEMA: &str = "insight.ffprobe.Request.avsc";
pub const SERVICES_FFPROBE_RESPONSE_SCHEMA: &str = "insight.ffprobe.Response.avsc";

pub struct BuilderImpl {
    pub directory: SchemaDirectory,
}

impl BuilderImpl {
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
            (
                TRANSPORT_SCHEMAS,
                STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA,
            ),
            (TRANSPORT_SCHEMAS, STREAM_TRACK_UNITS_REQUEST_SCHEMA),
            (TRANSPORT_SCHEMAS, STREAM_TRACK_UNITS_RESPONSE_SCHEMA),
            (TRANSPORT_SCHEMAS, PING_REQUEST_RESPONSE_SCHEMA),
            (TRANSPORT_SCHEMAS, KEEPALIVE_MESSAGE_SCHEMA),
            (TRANSPORT_SCHEMAS, MESSAGE_ENVELOPE_SCHEMA),
            (SERVICE_FFPROBE_SCHEMAS, SERVICES_FFPROBE_REQUEST_SCHEMA),
            (SERVICE_FFPROBE_SCHEMAS, SERVICES_FFPROBE_RESPONSE_SCHEMA),
        ]
    }

    pub fn new(path_prefix: &str) -> BuilderImpl {
        let schemas_raw: Vec<String> = Self::schema_files()
            .iter()
            .map(|schema| {
                utils::load_file(
                    Path::new(path_prefix).join(Path::new(schema.0)).as_path(),
                    schema.1,
                )
            })
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
                    let mut full_name = namespace
                        .clone()
                        .unwrap_or_else(|| String::from("insight.transport"));
                    full_name.push('.');
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
                    let mut full_name = namespace
                        .clone()
                        .unwrap_or_else(|| String::from("insight.transport"));
                    full_name.push('.');
                    full_name.push_str(name);
                    full_name.push_str(".avsc");
                    named_schemas.insert(full_name, s.clone());
                }
                _ => {
                    dbg!(s);
                }
            };
        }

        BuilderImpl {
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

    fn pack_message_into_envelope(&self, schema_name: &str, payload: Value) -> Vec<u8> {
        let mut envelope = self.get_record(MESSAGE_ENVELOPE_SCHEMA);
        let inner = to_avro_datum(self.get_schema(schema_name).unwrap(), payload).unwrap();
        envelope.put("schema", Value::Bytes(schema_name.into()));
        envelope.put("payload", Value::Bytes(inner));
        to_avro_datum(self.get_schema(MESSAGE_ENVELOPE_SCHEMA).unwrap(), envelope).unwrap()
    }

    pub fn read_protocol_message(&self, from: &Vec<u8>) -> Result<(String, Value), String> {
        let envelope_schema = self.get_schema(MESSAGE_ENVELOPE_SCHEMA).unwrap();
        let envelope = from_avro_datum(envelope_schema, &mut from.as_slice(), None);

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

#[pyclass]
pub struct Builder {
    builder: BuilderImpl,
}

#[derive(Clone)]
#[pyclass]
pub struct ProtocolMessage {
    pub schema: String,
    pub object: Value,
}

#[pymethods]
impl Builder {
    #[new]
    pub fn new(path_prefix: &str) -> Builder {
        Builder {
            builder: BuilderImpl::new(path_prefix),
        }
    }

    pub fn load_to_avro(&self, obj: Vec<u8>) -> Option<ProtocolMessage> {
        match self.builder.read_protocol_message(&obj) {
            Ok((schema, object)) => Some(ProtocolMessage { schema, object }),
            Err(m) => {
                warn!(
                    "Unable to decode the message from the envelope. Error is {}",
                    m
                );
                None
            }
        }
    }

    pub fn save_from_avro(&self, message: ProtocolMessage) -> Vec<u8> {
        self.builder
            .pack_message_into_envelope(message.schema.as_str(), message.object)
    }

    pub fn save(&self, obj: &PyAny) -> Option<Vec<u8>> {
        fn try_to<T: Clone + PyClass + ToProtocolMessage>(
            mb: &Builder,
            x: &PyAny,
        ) -> Option<Vec<u8>> {
            if x.is_instance_of::<T>().unwrap() {
                let ro: T = x.extract().unwrap();
                let protocol_message_res = ro.save(mb);

                protocol_message_res.map(|m| mb.save_from_avro(m))
            } else {
                None
            }
        }

        try_to::<UnitElementMessage>(self, obj)
            .or_else(|| try_to::<NotifyMessage>(self, obj))
            .or_else(|| try_to::<PingRequestResponse>(self, obj))
            .or_else(|| try_to::<ServicesFFProbeRequest>(self, obj))
            .or_else(|| try_to::<ServicesFFProbeResponse>(self, obj))
            .or_else(|| try_to::<StreamTrackUnitElementsRequest>(self, obj))
            .or_else(|| try_to::<StreamTrackUnitElementsResponse>(self, obj))
            .or_else(|| try_to::<StreamTracksRequest>(self, obj))
            .or_else(|| try_to::<StreamTracksResponse>(self, obj))
            .or_else(|| try_to::<StreamTrackUnitsRequest>(self, obj))
            .or_else(|| try_to::<StreamTrackUnitsResponse>(self, obj))
            .or_else(|| try_to::<KeepAliveMessage>(self, obj))
    }

    pub fn load(&self, message: Vec<u8>) -> Option<PyObject> {
        fn try_from<T: FromProtocolMessage + PyClass + Into<PyClassInitializer<T>>>(
            message: &ProtocolMessage,
        ) -> Option<PyObject> {
            let gil = Python::acquire_gil();
            let py = gil.python();

            T::load(message).map(|o| Py::new(py, o).unwrap().to_object(py))
        }

        match self.load_to_avro(message) {
            None => None,
            Some(obj) => try_from::<UnitElementMessage>(&obj)
                .or_else(|| try_from::<NotifyMessage>(&obj))
                .or_else(|| try_from::<PingRequestResponse>(&obj))
                .or_else(|| try_from::<ServicesFFProbeRequest>(&obj))
                .or_else(|| try_from::<ServicesFFProbeResponse>(&obj))
                .or_else(|| try_from::<StreamTrackUnitElementsRequest>(&obj))
                .or_else(|| try_from::<StreamTrackUnitElementsResponse>(&obj))
                .or_else(|| try_from::<StreamTracksRequest>(&obj))
                .or_else(|| try_from::<StreamTracksResponse>(&obj))
                .or_else(|| try_from::<StreamTrackUnitsRequest>(&obj))
                .or_else(|| try_from::<StreamTrackUnitsResponse>(&obj))
                .or_else(|| try_from::<KeepAliveMessage>(&obj)),
        }
    }
}

impl Builder {
    pub fn get_record(&self, schema_name: &str) -> Record {
        let record = Record::new(self.builder.get_schema(schema_name).unwrap()).unwrap();
        record
    }
}

#[cfg(test)]
mod tests {
    use crate::avro::{Builder, UNIT_ELEMENT_MESSAGE_SCHEMA};
    use crate::utils::get_avro_path;

    #[test]
    fn test_load_schemas() {
        let mb = Builder::new(get_avro_path().as_str());
        let _r = mb.get_record(UNIT_ELEMENT_MESSAGE_SCHEMA);
    }
}
