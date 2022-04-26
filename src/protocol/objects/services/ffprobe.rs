use std::collections::HashMap;
use avro_rs::types::Value;
use log::warn;
use pyo3::prelude::*;
use crate::protocol::avro::{Builder, ProtocolMessage, SERVICES_FFPROBE_REQUEST_SCHEMA, SERVICES_FFPROBE_RESPONSE_SCHEMA};
use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
use crate::utils::{gen_hash_map, value_to_string};

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub enum ServicesFFProbeResponseType {
    Accepted,
    Complete,
    Error,
    NotImplemented,
}

pub fn get_services_ffprobe_response_type_avro(response_type: &ServicesFFProbeResponseType) -> Value {
    match response_type {
        ServicesFFProbeResponseType::Accepted => Value::Enum(0, "ACCEPTED".into()),
        ServicesFFProbeResponseType::Complete => Value::Enum(1, "COMPLETE".into()),
        ServicesFFProbeResponseType::Error => Value::Enum(2, "ERROR".into()),
        ServicesFFProbeResponseType::NotImplemented => panic!("Not supported ffprobe response type")
    }
}


fn get_services_ffprobe_response_type_enum(response_type: &str) -> ServicesFFProbeResponseType {
    match response_type {
        "ACCEPTED" => ServicesFFProbeResponseType::Accepted,
        "COMPLETE" => ServicesFFProbeResponseType::Complete,
        "ERROR" => ServicesFFProbeResponseType::Error,
        _ => ServicesFFProbeResponseType::NotImplemented
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct ServicesFFProbeRequest {
    #[pyo3(get, set)]
    pub request_id: i64,
    #[pyo3(get, set)]
    pub topic: String,
    #[pyo3(get, set)]
    pub url: String,
    #[pyo3(get, set)]
    pub attributes: HashMap<String, String>,
}

#[pymethods]
impl ServicesFFProbeRequest {
    #[new]
    pub fn new(request_id: i64,
               topic: String,
               url: String,
               attributes: HashMap<String, String>,
    ) -> Self {
        ServicesFFProbeRequest {
            request_id,
            topic,
            url,
            attributes,
        }
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct ServicesFFProbeResponse {
    #[pyo3(get, set)]
    pub request_id: i64,
    #[pyo3(get, set)]
    pub response_type: ServicesFFProbeResponseType,
    #[pyo3(get, set)]
    pub time_spent: i64,
    #[pyo3(get, set)]
    pub streams: Vec<HashMap<String, String>>,
}

#[pymethods]
impl ServicesFFProbeResponse {
    #[new]
    pub fn new(request_id: i64,
               response_type: ServicesFFProbeResponseType,
               time_spent: i64,
               streams: Vec<HashMap<String, String>>) -> Self {
        ServicesFFProbeResponse {
            request_id,
            response_type,
            time_spent,
            streams,
        }
    }
}

impl FromProtocolMessage for ServicesFFProbeRequest {
    fn load(message: &ProtocolMessage) -> Option<ServicesFFProbeRequest> {
        if message.schema != SERVICES_FFPROBE_REQUEST_SCHEMA {
            return None;
        }

        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::String(topic)),
                (_, Value::String(url)),
                (_, Value::Map(attributes)),
                ] => {
                    Some(ServicesFFProbeRequest {
                        request_id: *request_id,
                        topic: topic.clone(),
                        url: url.clone(),
                        attributes: attributes.iter().map(|x| (x.0.clone(), value_to_string(x.1).or(Some(String::from(""))).unwrap())).collect(),
                    })
                }
                _ => {
                    warn!("Unable to match AVRO Record to to FFprobe Request");
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

impl FromProtocolMessage for ServicesFFProbeResponse {
    fn load(message: &ProtocolMessage) -> Option<ServicesFFProbeResponse> {
        if message.schema != SERVICES_FFPROBE_RESPONSE_SCHEMA {
            return None;
        }

        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::Enum(_, response_type)),
                (_, Value::Long(time_spent)),
                (_, Value::Array(streams)),
                ] => {
                    let mut response_streams: Vec<HashMap<String, String>> = Default::default();
                    for s in streams {
                        match s {
                            Value::Map(attributes) => {
                                let attributes: HashMap<String, String> = attributes.iter()
                                    .map(|kv| (kv.0.clone(), value_to_string(kv.1).unwrap_or(String::from("")))).collect();
                                response_streams.push(attributes);
                            }
                            _ => panic!("Unexpected structure found, stream attributes must be a `map`")
                        }
                    }

                    Some(ServicesFFProbeResponse {
                        request_id: *request_id,
                        response_type: get_services_ffprobe_response_type_enum(response_type.as_str()),
                        time_spent: *time_spent,
                        streams: response_streams,
                    })
                }
                _ => {
                    warn!("Unable to match AVRO Record to to FFprobe Response");
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

impl ToProtocolMessage for ServicesFFProbeRequest {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut object = mb.get_record(SERVICES_FFPROBE_REQUEST_SCHEMA);
        object.put("request_id", Value::Long(self.request_id.clone()));
        object.put("topic", Value::String(self.topic.clone()));
        object.put("url", Value::String(self.url.clone()));
        object.put("attributes", gen_hash_map(&self.attributes));
        Some(ProtocolMessage {
            schema: String::from(SERVICES_FFPROBE_REQUEST_SCHEMA),
            object: Value::from(object),
        })
    }
}

impl ToProtocolMessage for ServicesFFProbeResponse {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut object = mb.get_record(SERVICES_FFPROBE_RESPONSE_SCHEMA);
        object.put("request_id", Value::Long(self.request_id.clone()));
        object.put("response_type", get_services_ffprobe_response_type_avro(&self.response_type));
        object.put("time_spent", Value::Long(self.time_spent.clone()));
        let streams_array: Vec<Value> = self.streams.iter().map(|s| gen_hash_map(s)).collect();
        object.put("streams", Value::Array(streams_array));
        Some(ProtocolMessage {
            schema: String::from(SERVICES_FFPROBE_RESPONSE_SCHEMA),
            object: Value::from(object),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use crate::protocol::avro::Builder;
    use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::protocol::objects::services::ffprobe::{ServicesFFProbeRequest, ServicesFFProbeResponse, ServicesFFProbeResponseType};
    use crate::utils::get_avro_path;

    #[test]
    fn test_load_save_req() {
        let mb = Builder::new(get_avro_path().as_str());
        let req = ServicesFFProbeRequest::new(
            0,
            String::from("test"),
            String::from("/dev/video0"),
            HashMap::from([
                ("attribute".into(), "value".into())
            ]));
        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = ServicesFFProbeRequest::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }

    #[test]
    fn test_load_save_resp() {
        let mb = Builder::new(get_avro_path().as_str());
        let res = ServicesFFProbeResponse::new(1,
                                               ServicesFFProbeResponseType::Accepted,
                                               100, vec![
                HashMap::from([("a".to_string(), "b".to_string())]),
                HashMap::from([("x".to_string(), "y".to_string())]),
            ]);
        let res_envelope_opt = res.save(&mb);
        assert!(res_envelope_opt.is_some());

        let res_envelope = res_envelope_opt.unwrap();
        let res_serialized = mb.save_from_avro(res_envelope);

        let res_envelope_opt = mb.load_to_avro(res_serialized);
        assert!(res_envelope_opt.is_some());

        let res_envelope = res_envelope_opt.unwrap();

        let new_res_opt = ServicesFFProbeResponse::load(&res_envelope);

        assert!(new_res_opt.is_some());

        let new_res = new_res_opt.unwrap();

        assert_eq!(res, new_res);
    }
}
