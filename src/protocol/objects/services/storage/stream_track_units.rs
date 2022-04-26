use crate::protocol::primitives::Unit;
use pyo3::prelude::*;
use avro_rs::types::Value;
use log::warn;
use crate::protocol::avro::{Builder, ProtocolMessage, STREAM_TRACK_UNITS_REQUEST_SCHEMA, STREAM_TRACK_UNITS_RESPONSE_SCHEMA};
use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct StreamTrackUnitsRequest {
    #[pyo3(get, set)]
    pub request_id: i64,
    #[pyo3(get, set)]
    pub topic: String,
    #[pyo3(get, set)]
    pub stream_unit: Unit,
    #[pyo3(get, set)]
    pub from_ms: u128,
    #[pyo3(get, set)]
    pub to_ms: u128,
}

#[pymethods]
impl StreamTrackUnitsRequest {
    #[new]
    pub fn new(request_id: i64,
               topic: String,
               stream_unit: Unit,
               from_ms: u128,
               to_ms: u128,
    ) -> Self {
        StreamTrackUnitsRequest {
            request_id,
            topic,
            stream_unit,
            from_ms,
            to_ms,
        }
    }
}

impl FromProtocolMessage for StreamTrackUnitsRequest {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::String(topic)),
                (_, Value::Record(stream_unit_fields)),
                (_, Value::Long(from_ms)),
                (_, Value::Long(to_ms))
                ] => match stream_unit_fields.as_slice() {
                    [
                    (_, Value::Bytes(stream_name)),
                    (_, Value::Bytes(track_name)),
                    (_, Value::Enum(_index, track_type)),
                    (_, Value::Long(unit))
                    ] => {
                        Some(StreamTrackUnitsRequest {
                            request_id: request_id.clone(),
                            topic: topic.clone(),
                            stream_unit: Unit::new(stream_name.clone(), track_name.clone(), track_type.clone(), *unit),
                            from_ms: from_ms.clone() as u128,
                            to_ms: to_ms.clone() as u128,
                        })
                    }
                    _ => {
                        warn!("Unable to match AVRO Record to Unit");
                        None
                    }
                }
                _ => {
                    warn!("Unable to match AVRO Record to to StreamTrackUnitsRequest");
                    None
                }
            }
            _ => {
                warn!("Unable to match AVRO Record");
                None
            }
        }
    }
}

impl ToProtocolMessage for StreamTrackUnitsRequest {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(STREAM_TRACK_UNITS_REQUEST_SCHEMA);
        obj.put("request_id", Value::Long(self.request_id));
        obj.put("topic", Value::String(self.topic.clone()));
        obj.put(
            "stream_unit",
            self.stream_unit.to_avro_record(),
        );
        obj.put("from_ms", Value::Long(i64::try_from(self.from_ms).unwrap()));
        obj.put("to_ms", Value::Long(i64::try_from(self.to_ms).unwrap()));
        Some(ProtocolMessage {
            schema: String::from(STREAM_TRACK_UNITS_REQUEST_SCHEMA),
            object: Value::from(obj),
        })
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct StreamTrackUnitsResponse {
    #[pyo3(get, set)]
    pub request_id: i64,
    #[pyo3(get, set)]
    pub stream_unit: Unit,
    #[pyo3(get, set)]
    pub from_ms: u128,
    #[pyo3(get, set)]
    pub to_ms: u128,
    #[pyo3(get, set)]
    pub units: Vec<i64>,
}

#[pymethods]
impl StreamTrackUnitsResponse {
    #[new]
    pub fn new(request_id: i64,
               stream_unit: Unit,
               from_ms: u128,
               to_ms: u128,
               units: Vec<i64>) -> Self {
        StreamTrackUnitsResponse {
            request_id,
            stream_unit,
            from_ms,
            to_ms,
            units,
        }
    }
}

impl FromProtocolMessage for StreamTrackUnitsResponse {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::Record(stream_unit_fields)),
                (_, Value::Long(from_ms)),
                (_, Value::Long(to_ms)),
                (_, Value::Array(units))
                ] => match stream_unit_fields.as_slice() {
                    [
                    (_, Value::Bytes(stream_name)),
                    (_, Value::Bytes(track_name)),
                    (_, Value::Enum(_index, track_type)),
                    (_, Value::Long(unit))
                    ] => {
                        let units_parsed: Vec<_> = units.iter()
                            .map(|x| match x {
                                Value::Long(value) => Some(value.clone()),
                                _ => None
                            })
                            .filter(|x| x.is_some())
                            .map(|x| x.unwrap()).collect();

                        if units_parsed.len() < units.len() {
                            warn!("Not all payload units were parsed correctly");
                            None
                        } else {
                            Some(StreamTrackUnitsResponse {
                                request_id: request_id.clone(),
                                stream_unit: Unit::new(stream_name.clone(), track_name.clone(), track_type.clone(), *unit),
                                from_ms: from_ms.clone() as u128,
                                to_ms: to_ms.clone() as u128,
                                units: units_parsed,
                            })
                        }
                    }
                    _ => {
                        warn!("Unable to match AVRO Record to Unit");
                        None
                    }
                }
                _ => {
                    warn!("Unable to match AVRO Record to to StreamTrackUnitsResponse");
                    None
                }
            }
            _ => {
                warn!("Unable to match AVRO Record");
                None
            }
        }
    }
}

impl ToProtocolMessage for StreamTrackUnitsResponse {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(STREAM_TRACK_UNITS_RESPONSE_SCHEMA);
        obj.put("request_id", Value::Long(self.request_id));
        obj.put(
            "stream_unit",
            self.stream_unit.to_avro_record(),
        );
        obj.put("from_ms", Value::Long(i64::try_from(self.from_ms).unwrap()));
        obj.put("to_ms", Value::Long(i64::try_from(self.to_ms).unwrap()));
        let values: Vec<Value> = self.units.iter().map(|x| Value::Long(*x)).collect();
        obj.put("units", Value::Array(values));
        Some(ProtocolMessage {
            schema: String::from(STREAM_TRACK_UNITS_RESPONSE_SCHEMA),
            object: Value::from(obj),
        })
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use crate::protocol::avro::Builder;
    use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::protocol::objects::services::storage::stream_track_units::{StreamTrackUnitsRequest, StreamTrackUnitsResponse};
    use crate::protocol::primitives::{pack_stream_name, pack_track_name, Unit};
    use crate::utils::get_avro_path;

    #[test]
    fn test_load_save_req() {
        let mb = Builder::new(get_avro_path().as_str());

        let track_name = pack_track_name(&String::from("test")).unwrap();
        let stream_uuid = Uuid::parse_str("fa807469-fbb3-4f63-b1a9-f63fbbf90f41").unwrap();
        let stream_name = pack_stream_name(&stream_uuid);

        let req = StreamTrackUnitsRequest::new(
            1,
            String::from("response"),
            Unit::new(stream_name.to_vec(), track_name.to_vec(), String::from("VIDEO"), 3),
            100,
            500);

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = StreamTrackUnitsRequest::load(&req_envelope);

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

        let req = StreamTrackUnitsResponse::new(
            1,
            Unit::new(stream_name.to_vec(), track_name.to_vec(), String::from("VIDEO"), 3),
            100, 500, vec![1, 2, 3]);

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = StreamTrackUnitsResponse::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }
}
