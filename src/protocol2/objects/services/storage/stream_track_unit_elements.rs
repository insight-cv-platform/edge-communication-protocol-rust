use pyo3::prelude::*;
use avro_rs::types::Value;
use log::warn;
use crate::protocol2::avro::{ProtocolMessage, STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA, STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA, Builder};

use crate::protocol2::primitives::{ElementType, Payload, StreamName, TrackName, TrackType, Unit};
use crate::protocol2::objects::{FromProtocolMessage, ToProtocolMessage};
use crate::utils::value_to_string;


#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct StreamTrackUnitElementsRequest {
    pub request_id: i64,
    pub topic: String,
    pub stream_unit: Unit,
    pub max_element: ElementType,
}

#[pymethods]
impl StreamTrackUnitElementsRequest {
    #[new]
    pub fn new(request_id: i64,
               topic: String,
               stream_unit: Unit,
               max_element: ElementType) -> Self {
        StreamTrackUnitElementsRequest {
            request_id,
            topic,
            stream_unit,
            max_element,
        }
    }
}

impl FromProtocolMessage for StreamTrackUnitElementsRequest {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        if message.schema != STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA {
            return None;
        }
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::String(topic)),
                (_, Value::Record(stream_unit_fields)),
                (_, Value::Long(max_element)),
                ] => match stream_unit_fields.as_slice() {
                    [
                    (_, Value::Bytes(stream_name)),
                    (_, Value::Bytes(track_name)),
                    (_, Value::Enum(_index, track_type)),
                    (_, Value::Long(unit))
                    ] => {
                        Some(StreamTrackUnitElementsRequest {
                            request_id: request_id.clone(),
                            topic: topic.clone(),
                            stream_unit: Unit::new(stream_name.clone(), track_name.clone(), track_type.clone(), *unit),
                            max_element: max_element.clone() as i16,
                        })
                    }
                    _ => {
                        warn!("Unable to match AVRO Record to Unit");
                        None
                    }
                }
                _ => {
                    warn!("Unable to match AVRO Record to to StreamTrackUnitElementsRequest");
                    None
                }
            }
            _ => {
                warn!("Unable to match AVRO Record.");
                None
            }
        }
    }
}

impl ToProtocolMessage for StreamTrackUnitElementsRequest {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA);
        obj.put("request_id", Value::Long(self.request_id));
        obj.put("topic", Value::String(self.topic.clone()));
        obj.put(
            "stream_unit",
            get_stream_unit(self.stream_unit.stream_name.clone(), &self.stream_unit.track_type,
                            self.stream_unit.track_name.clone(), self.stream_unit.unit),
        );
        obj.put("max_element", Value::Long(self.max_element.into()));

        Some(ProtocolMessage {
            schema: String::from(STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA),
            object: Value::from(obj),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct StreamTrackUnitElementsResponse {
    pub request_id: i64,
    pub stream_unit: Unit,
    pub values: Vec<Payload>,
}

#[pymethods]
impl StreamTrackUnitElementsResponse {
    #[new]
    pub fn new(request_id: i64,
               stream_unit: Unit,
               values: Vec<Payload>) -> Self {
        StreamTrackUnitElementsResponse {
            request_id,
            stream_unit,
            values,
        }
    }
}

fn get_track_type_enum(track_type: &TrackType) -> Value {
    match track_type {
        TrackType::Video => Value::Enum(0, "VIDEO".into()),
        TrackType::Meta => Value::Enum(1, "META".into()),
        TrackType::NotImplemented => panic!("Not supported track type")
    }
}

impl FromProtocolMessage for StreamTrackUnitElementsResponse {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        if message.schema != STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA {
            return None;
        }
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::Record(stream_unit_fields)),
                (_, Value::Array(values)),
                ] => match stream_unit_fields.as_slice() {
                    [
                    (_, Value::Bytes(stream_name)),
                    (_, Value::Bytes(track_name)),
                    (_, Value::Enum(_index, track_type)),
                    (_, Value::Long(unit))
                    ] => {
                        fn to_payload(v: &Value) -> Option<Payload> {
                            match v {
                                Value::Record(fields) => match fields.as_slice() {
                                    [
                                    (_, Value::Bytes(data)),
                                    (_, Value::Map(attributes))
                                    ] => {
                                        Some(Payload {
                                            data: data.clone(),
                                            attributes: attributes.iter()
                                                .map(|x| (x.0.clone(), value_to_string(x.1)
                                                    .or(Some(String::from(""))).unwrap())).collect(),
                                        })
                                    }
                                    _ => None
                                }
                                _ => None
                            }
                        }

                        let values_parsed: Vec<_> = values.iter()
                            .map(|x| to_payload(x))
                            .filter(|x| x.is_some())
                            .map(|x| x.unwrap()).collect();

                        if values_parsed.len() < values.len() {
                            warn!("Not all payload values were parsed correctly");
                            None
                        } else {
                            Some(StreamTrackUnitElementsResponse {
                                request_id: request_id.clone(),
                                stream_unit: Unit::new(stream_name.clone(), track_name.clone(), track_type.clone(), *unit),
                                values: values_parsed,
                            })
                        }
                    }
                    _ => {
                        warn!("Unable to match AVRO Record to Unit");
                        None
                    }
                }
                _ => {
                    warn!("Unable to match AVRO Record to to StreamTrackUnitElementsRequest");
                    None
                }
            }
            _ => {
                warn!("Unable to match AVRO Record.");
                None
            }
        }
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
        ("track_type".into(), get_track_type_enum(track_type)),
        ("unit".into(), Value::Long(unit)),
    ])
}

fn payload_to_avro(p: &Payload) -> Value {
    Value::Record(vec![
        ("data".into(), Value::Bytes(p.data.clone())),
        ("attributes".into(), Value::Map(p.attributes.iter().map(|x| (x.0.clone(), Value::String(x.1.clone()))).collect())),
    ])
}

impl ToProtocolMessage for StreamTrackUnitElementsResponse {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA);
        obj.put("request_id", Value::Long(self.request_id));
        obj.put(
            "stream_unit",
            get_stream_unit(self.stream_unit.stream_name.clone(), &self.stream_unit.track_type, self.stream_unit.track_name.clone(), self.stream_unit.unit),
        );

        let values: Vec<Value> = self.values.iter().map(|x| payload_to_avro(x)).collect();
        obj.put("values", Value::Array(values));
        Some(ProtocolMessage {
            schema: String::from(STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA),
            object: Value::from(obj),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use uuid::Uuid;
    use crate::protocol2::avro::Builder;
    use crate::protocol2::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::protocol2::objects::services::storage::stream_track_unit_elements::{StreamTrackUnitElementsRequest, StreamTrackUnitElementsResponse};
    use crate::protocol2::primitives::{pack_stream_name, pack_track_name, Payload, Unit};
    use crate::utils::get_avro_path;

    #[test]
    fn test_load_save_req() {
        let mb = Builder::new(get_avro_path().as_str());

        let track_name = pack_track_name(&String::from("test")).unwrap();
        let stream_uuid = Uuid::parse_str("fa807469-fbb3-4f63-b1a9-f63fbbf90f41").unwrap();
        let stream_name = pack_stream_name(&stream_uuid);

        let req = StreamTrackUnitElementsRequest::new(
            1,
            String::from("response"),
            Unit::new(stream_name.to_vec(), track_name.to_vec(), String::from("VIDEO"), 3),
            100);

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save(req_envelope);

        let req_envelope_opt = mb.load(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = StreamTrackUnitElementsRequest::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }

    #[test]
    fn test_load_save_rep() {
        let mb = Builder::new(get_avro_path().as_str());

        let track_name = pack_track_name(&String::from("test")).unwrap();
        let stream_uuid = Uuid::parse_str("fa807469-fbb3-4f63-b1a9-f63fbbf90f41").unwrap();
        let stream_name = pack_stream_name(&stream_uuid);

        let req = StreamTrackUnitElementsResponse::new(
            1,
            Unit::new(stream_name.to_vec(), track_name.to_vec(), String::from("VIDEO"), 3),
            vec![
                Payload {
                    data: vec![0, 1, 2],
                    attributes: HashMap::default(),
                },
                Payload {
                    data: vec![1, 2, 3],
                    attributes: HashMap::default(),
                },
            ]);

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save(req_envelope);

        let req_envelope_opt = mb.load(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = StreamTrackUnitElementsResponse::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }
}
