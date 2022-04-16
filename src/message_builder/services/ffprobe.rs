use std::collections::HashMap;
use avro_rs::types::Value;
use crate::message_builder::{MessageBuilder, SERVICES_FFPROBE_REQUEST_SCHEMA, SERVICES_FFPROBE_RESPONSE_SCHEMA};
use crate::protocol::Message;
use crate::utils::{gen_hash_map, value_to_string};

#[derive(Debug)]
pub enum ServicesFFProbeResponseType {
    Accepted,
    Complete,
    NotImplemented,
}

pub fn get_services_ffprobe_response_type_avro(response_type: &ServicesFFProbeResponseType) -> Value {
    match response_type {
        ServicesFFProbeResponseType::Accepted => Value::Enum(0, "ACCEPTED".into()),
        ServicesFFProbeResponseType::Complete => Value::Enum(1, "COMPLETE".into()),
        ServicesFFProbeResponseType::NotImplemented => panic!("Not supported ffprobe response type")
    }
}

pub fn get_services_ffprobe_response_type_enum(response_type: &str) -> ServicesFFProbeResponseType {
    match response_type {
        "ACCEPTED" => ServicesFFProbeResponseType::Accepted,
        "COMPLETE" => ServicesFFProbeResponseType::Complete,
        _ => ServicesFFProbeResponseType::NotImplemented
    }
}

pub fn build_services_ffprobe_request(mb: &MessageBuilder, request_id: i64, topic: String, url: String, attributes: HashMap<String, String>) -> Vec<u8> {
    let mut record = mb.get_record(SERVICES_FFPROBE_REQUEST_SCHEMA);
    record.put("request_id", Value::Long(request_id));
    record.put("topic", Value::String(topic));
    record.put("url", Value::String(url));
    record.put("attributes", gen_hash_map(&attributes));
    mb.pack_message_into_envelope(SERVICES_FFPROBE_REQUEST_SCHEMA, record)
}

pub fn build_services_ffprobe_response(mb: &MessageBuilder, request_id: i64, response_type: &ServicesFFProbeResponseType, streams: Vec<HashMap<String, String>>) -> Vec<u8> {
    let mut record = mb.get_record(SERVICES_FFPROBE_RESPONSE_SCHEMA);
    record.put("request_id", Value::Long(request_id));
    record.put("response_type", get_services_ffprobe_response_type_avro(response_type));
    let streams_array: Vec<Value> = streams.iter().map(|s| gen_hash_map(s)).collect();
    record.put("streams", Value::Array(streams_array));
    mb.pack_message_into_envelope(SERVICES_FFPROBE_RESPONSE_SCHEMA, record)
}

pub fn load_services_ffprobe_request(value: Value) -> Message {
    match value {
        Value::Record(fields) => match fields.as_slice() {
            [
            (_, Value::Long(request_id)),
            (_, Value::String(topic)),
            (_, Value::String(url)),
            (_, Value::Map(attributes)),
            ] => {
                Message::ServicesFFProbeRequest {
                    request_id: *request_id,
                    topic: topic.clone(),
                    url: url.clone(),
                    attributes: attributes.iter().map(|x| (x.0.clone(), value_to_string(x.1).or(Some(String::from(""))).unwrap())).collect(),
                }
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record to to FFprobe Request"))
        }
        _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
    }
}

pub fn load_services_ffprobe_response(value: Value) -> Message {
    match value {
        Value::Record(fields) => match fields.as_slice() {
            [
            (_, Value::Long(request_id)),
            (_, Value::Enum(_, response_type)),
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

                Message::ServicesFFProbeResponse {
                    request_id: *request_id,
                    response_type: get_services_ffprobe_response_type_enum(response_type.as_str()),
                    streams: response_streams,
                }
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record to to FFprobe Response"))
        }
        _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
    }
}

