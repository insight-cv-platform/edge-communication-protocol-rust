use std::collections::HashMap;
use avro_rs::types::Value;

use crate::{ElementType, Payload, STREAM_NAME_MAX_LENGTH, StreamName, TRACK_NAME_MAX_LENGTH, TrackInfo, TrackName, TrackType};

use crate::message_builder::*;

fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        _ => None
    }
}

#[derive(Default, Debug)]
pub struct Unit {
    pub stream_name: StreamName,
    pub track_name: TrackName,
    pub track_type: TrackType,
    pub unit: i64,
}

fn fill_byte_array(buf: &mut [u8], from: &Vec<u8>) {
    let len = std::cmp::min(buf.len(), from.len());
    buf[..len].clone_from_slice(from.as_slice());
}

fn track_type_literal_to_track_type(literal: &str) -> TrackType {
    match literal {
        "VIDEO" => TrackType::Video,
        "META" => TrackType::Meta,
        _ => TrackType::NotImplemented
    }
}

impl Unit {
    pub fn new(stream_name: &Vec<u8>, track_name: &Vec<u8>, track_type: &String, unit: i64) -> Unit {
        let mut u = Unit::default();
        fill_byte_array(&mut u.stream_name, stream_name);
        fill_byte_array(&mut u.track_name, track_name);
        u.track_type = track_type_literal_to_track_type(track_type.as_str());
        u.unit = unit;
        u
    }
}

#[derive(Debug)]
pub enum NotifyType {
    Ready(ElementType),
    New,
    NotImplemented,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum PingRequestResponseType {
    REQUEST,
    RESPONSE,
}

#[derive(Debug)]
pub enum Message {
    StreamTracksResponse {
        request_id: i64,
        stream_name: StreamName,
        tracks: Vec<TrackInfo>,
    },
    StreamTracksRequest {
        request_id: i64,
        topic: String,
        stream_name: StreamName,
    },
    UnitElementMessage {
        stream_unit: Unit,
        element: ElementType,
        value: Vec<u8>,
        attributes: HashMap<String, String>,
        last: bool,
    },
    NotifyMessage {
        stream_unit: Unit,
        saved_ms: u64,
        notify_type: NotifyType,
    },
    StreamTrackUnitElementsRequest {
        request_id: i64,
        topic: String,
        stream_unit: Unit,
        max_element: ElementType,
    },
    StreamTrackUnitElementsResponse {
        request_id: i64,
        stream_unit: Unit,
        values: Vec<Payload>,
    },
    StreamTrackUnitsRequest {
        request_id: i64,
        topic: String,
        stream_unit: Unit,
        from_ms: u128,
        to_ms: u128,
    },
    StreamTrackUnitsResponse {
        request_id: i64,
        stream_unit: Unit,
        from_ms: u128,
        to_ms: u128,
        units: Vec<i64>,
    },
    PingRequestResponse {
        request_id: i64,
        topic: String,
        mtype: PingRequestResponseType,
    },
    ServicesFFprobeRequest {
        request_id: i64,
        topic: String,
        url: String,
        attributes: HashMap<String, String>,
    },
    ServicesFFprobeResponse {
        request_id: i64,
        streams: Vec<HashMap<String, String>>,
    },
    ParsingError(String),
}

impl Message {
    fn load_unit_element_message(value: Value) -> Message {
        match value {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Record(stream_unit_fields)),
                (_, Value::Long(element)),
                (_, Value::Bytes(value)),
                (_, Value::Map(attributes)),
                (_, Value::Boolean(last))
                ] => match stream_unit_fields.as_slice() {
                    [
                    (_, Value::Bytes(stream_name)),
                    (_, Value::Bytes(track_name)),
                    (_, Value::Enum(_index, track_type)),
                    (_, Value::Long(unit))
                    ] => {
                        Message::UnitElementMessage {
                            stream_unit: Unit::new(stream_name, track_name, track_type, unit.clone()),
                            element: element.clone() as i16,
                            value: value.clone(),
                            attributes: attributes.iter().map(|x| (x.0.clone(), value_to_string(x.1).or(Some(String::from(""))).unwrap())).collect(),
                            last: last.clone(),
                        }
                    }
                    _ => Message::ParsingError(String::from("Unable to match AVRO Record to Unit"))
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to UnitElementMessage"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_notify_message(value: Value) -> Message {
        match value {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Record(stream_unit_fields)),
                (_, Value::Int(last_element)),
                (_, Value::Long(saved_ms)),
                (_, Value::Enum(_index, notify_type)),
                ] => match stream_unit_fields.as_slice() {
                    [
                    (_, Value::Bytes(stream_name)),
                    (_, Value::Bytes(track_name)),
                    (_, Value::Enum(_index, track_type)),
                    (_, Value::Long(unit))
                    ] => {
                        Message::NotifyMessage {
                            stream_unit: Unit::new(stream_name, track_name, track_type, unit.clone()),
                            saved_ms: *saved_ms as u64,
                            notify_type: match notify_type.as_str() {
                                "READY" => NotifyType::Ready(last_element.clone() as i16),
                                "NEW" => NotifyType::New,
                                _ => NotifyType::NotImplemented
                            },
                        }
                    }
                    _ => Message::ParsingError(String::from("Unable to match AVRO Record to Unit"))
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to NotifyMessage"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_stream_tracks_request(value: Value) -> Message {
        match value {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::String(topic)),
                (_, Value::Bytes(stream_name))
                ] => {
                    let mut sn = [0u8; STREAM_NAME_MAX_LENGTH];
                    fill_byte_array(&mut sn, stream_name);
                    Message::StreamTracksRequest {
                        request_id: request_id.clone(),
                        topic: topic.clone(),
                        stream_name: sn,
                    }
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to StreamTracksRequest"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_ping_request_response(value: Value) -> Message {
        match value {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::String(topic)),
                (_, Value::Enum(_index, ping_m_type))
                ] => {
                    Message::PingRequestResponse {
                        request_id: request_id.clone(),
                        topic: topic.clone(),
                        mtype: if ping_m_type.as_str() == "REQUEST" { PingRequestResponseType::REQUEST } else { PingRequestResponseType::RESPONSE },
                    }
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to PingRequestResponse"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }


    fn load_stream_tracks_response(value: Value) -> Message {
        match value {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::Bytes(stream_name)),
                (_, Value::Array(tracks))
                ] => {
                    let mut sn = [0u8; STREAM_NAME_MAX_LENGTH];
                    fill_byte_array(&mut sn, stream_name);
                    let track_records: Vec<Option<TrackInfo>> = tracks.iter().map(|t| match t {
                        Value::Record(fields) => {
                            match fields.as_slice() {
                                [
                                (_, Value::Bytes(track_name)),
                                (_, Value::Enum(_, track_type))
                                ] => {
                                    let mut tn = [0u8; TRACK_NAME_MAX_LENGTH];
                                    fill_byte_array(&mut tn, track_name);
                                    Some(TrackInfo {
                                        track_name: tn,
                                        track_type: track_type_literal_to_track_type(track_type.as_str()),
                                    })
                                }
                                _ => None
                            }
                        }
                        _ => None
                    }).collect();

                    let valid_track_records: Vec<_> = track_records.iter()
                        .filter(|x| x.is_some())
                        .map(|x| x.unwrap().clone()).collect();

                    if valid_track_records.len() < track_records.len() {
                        Message::ParsingError(String::from("Not all track info records are parsed well."))
                    } else {
                        Message::StreamTracksResponse {
                            request_id: request_id.clone(),
                            stream_name: sn,
                            tracks: valid_track_records,
                        }
                    }
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to StreamTracksResponse"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_stream_track_unit_elements_request(value: Value) -> Message {
        match value {
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
                        Message::StreamTrackUnitElementsRequest {
                            request_id: request_id.clone(),
                            topic: topic.clone(),
                            stream_unit: Unit::new(stream_name, track_name, track_type, unit.clone()),
                            max_element: max_element.clone() as i16,
                        }
                    }
                    _ => Message::ParsingError(String::from("Unable to match AVRO Record to Unit"))
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to StreamTrackUnitElementsRequest"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_stream_track_unit_elements_response(value: Value) -> Message {
        match value {
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
                                            attributes: attributes.iter().map(|x| (x.0.clone(), value_to_string(x.1).or(Some(String::from(""))).unwrap())).collect(),
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
                            Message::ParsingError(String::from("Not all payload values were parsed correctly"))
                        } else {
                            Message::StreamTrackUnitElementsResponse {
                                request_id: request_id.clone(),
                                stream_unit: Unit::new(stream_name, track_name, track_type, unit.clone()),
                                values: values_parsed,
                            }
                        }
                    }
                    _ => Message::ParsingError(String::from("Unable to match AVRO Record to Unit"))
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to StreamTrackUnitElementsRequest"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_stream_track_units_request(value: Value) -> Message {
        match value {
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
                        Message::StreamTrackUnitsRequest {
                            request_id: request_id.clone(),
                            topic: topic.clone(),
                            stream_unit: Unit::new(stream_name, track_name, track_type, unit.clone()),
                            from_ms: from_ms.clone() as u128,
                            to_ms: to_ms.clone() as u128,
                        }
                    }
                    _ => Message::ParsingError(String::from("Unable to match AVRO Record to Unit"))
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to StreamTrackUnitsRequest"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_stream_track_units_response(value: Value) -> Message {
        match value {
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
                            Message::ParsingError(String::from("Not all payload units were parsed correctly"))
                        } else {
                            Message::StreamTrackUnitsResponse {
                                request_id: request_id.clone(),
                                stream_unit: Unit::new(stream_name, track_name, track_type, unit.clone()),
                                from_ms: from_ms.clone() as u128,
                                to_ms: to_ms.clone() as u128,
                                units: units_parsed,
                            }
                        }
                    }
                    _ => Message::ParsingError(String::from("Unable to match AVRO Record to Unit"))
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to StreamTrackUnitsResponse"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }

    fn load_services_ffprobe_request(value: Value) -> Message {
        match value {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::String(topic)),
                (_, Value::String(url)),
                (_, Value::Map(attributes)),
                ] => {
                    Message::ServicesFFprobeRequest {
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

    fn load_services_ffprobe_response(value: Value) -> Message {
        match value {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::Array(streams)),
                ] => {
                    let mut response_streams: Vec<HashMap<String, String>> = Default::default();
                    for s in streams {
                        match s {
                            Value::Map(attributes) => {
                                let attributes: HashMap<String, String>  = attributes.iter()
                                    .map(|kv| (kv.0.clone(), value_to_string(kv.1).unwrap_or(String::from("")))).collect();
                                response_streams.push(attributes);
                            }
                            _ => panic!("Unexpected structure found, stream attributes must be a `map`")
                        }
                    }

                    Message::ServicesFFprobeResponse {
                        request_id: *request_id,
                        streams: response_streams,
                    }
                }
                _ => Message::ParsingError(String::from("Unable to match AVRO Record to to FFprobe Response"))
            }
            _ => Message::ParsingError(String::from("Unable to match AVRO Record."))
        }
    }


    pub fn from(kind: &String, value: Value) -> Message {
        match kind.as_str() {
            UNIT_ELEMENT_MESSAGE_SCHEMA => Self::load_unit_element_message(value),
            NOTIFY_MESSAGE_SCHEMA => Self::load_notify_message(value),
            STREAM_TRACKS_REQUEST_SCHEMA => Self::load_stream_tracks_request(value),
            STREAM_TRACKS_RESPONSE_SCHEMA => Self::load_stream_tracks_response(value),
            STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA => Self::load_stream_track_unit_elements_request(value),
            STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA => Self::load_stream_track_unit_elements_response(value),
            STREAM_TRACK_UNITS_REQUEST_SCHEMA => Self::load_stream_track_units_request(value),
            STREAM_TRACK_UNITS_RESPONSE_SCHEMA => Self::load_stream_track_units_response(value),
            PING_REQUEST_RESPONSE_SCHEMA => Self::load_ping_request_response(value),
            SERVICES_FFPROBE_REQUEST_SCHEMA => Self::load_services_ffprobe_request(value),
            SERVICES_FFPROBE_RESPONSE_SCHEMA => Self::load_services_ffprobe_response(value),
            _ => Message::ParsingError(kind.clone())
        }
    }

    pub fn dump(&self, mb: &MessageBuilder) -> Result<Vec<u8>, String> {
        match self {
            Message::UnitElementMessage {
                stream_unit: Unit { stream_name, track_name, track_type, unit },
                element,
                value,
                attributes,
                last
            } => Ok(mb.build_unit_element_message(*stream_name, track_type, *track_name, *unit, *element, value, attributes, *last)),

            Message::NotifyMessage {
                stream_unit: Unit { stream_name, track_name, track_type, unit },
                saved_ms,
                notify_type
            } => {
                let last = match notify_type {
                    NotifyType::Ready(last_element) => Some(*last_element),
                    NotifyType::New => None,
                    _ => panic!("Not implemented")
                };
                Ok(mb.build_notify_message(*stream_name, track_type, *track_name, *unit, *saved_ms, last))
            }

            Message::StreamTracksRequest {
                request_id, topic, stream_name
            } => Ok(mb.build_stream_tracks_request(*request_id, topic.clone(), *stream_name)),

            Message::PingRequestResponse {
                request_id, topic, mtype
            } => {
                match mtype {
                    PingRequestResponseType::REQUEST => Ok(mb.build_ping_request_response(*request_id, topic.clone(), false)),
                    PingRequestResponseType::RESPONSE => Ok(mb.build_ping_request_response(*request_id, topic.clone(), true))
                }
            }

            Message::StreamTracksResponse {
                request_id, stream_name, tracks
            } => Ok(mb.build_stream_tracks_response(*request_id, *stream_name,
                                                    &tracks.iter().map(|x| (x.track_name, x.track_type)).collect())),

            Message::StreamTrackUnitElementsRequest {
                request_id,
                topic,
                stream_unit: Unit { stream_name, track_name, track_type, unit },
                max_element
            } => Ok(mb.build_stream_track_unit_elements_request(*request_id, topic.clone(), *stream_name,
                                                                track_type, *track_name, *unit, *max_element)),

            Message::StreamTrackUnitElementsResponse {
                request_id,
                stream_unit: Unit { stream_name, track_name, track_type, unit },
                values
            } => Ok(mb.build_stream_track_unit_elements_response(*request_id, *stream_name, track_type, *track_name, *unit, values)),

            Message::StreamTrackUnitsRequest {
                request_id,
                topic,
                stream_unit: Unit { stream_name, track_name, track_type, unit: _ },
                from_ms,
                to_ms
            } => {
                let from_ms = i64::try_from(*from_ms);
                let to_ms = i64::try_from(*to_ms);
                match (from_ms, to_ms) {
                    (Ok(from_ms), Ok(to_ms)) =>
                        Ok(mb.build_stream_track_units_request(*request_id, topic.clone(), *stream_name, track_type, *track_name, from_ms, to_ms)),
                    _ => Err(format!("Unable to serialize from_ms ({:?})/to_ms ({:?}) to AVRO Long field.", from_ms, to_ms))
                }
            }

            Message::StreamTrackUnitsResponse {
                request_id,
                stream_unit: Unit { stream_name, track_name, track_type, unit: _ },
                from_ms,
                to_ms,
                units
            } => {
                let from_ms = i64::try_from(*from_ms);
                let to_ms = i64::try_from(*to_ms);
                match (from_ms, to_ms) {
                    (Ok(from_ms), Ok(to_ms)) =>
                        Ok(mb.build_stream_track_units_response(*request_id, *stream_name, track_type, *track_name, from_ms, to_ms, units)),
                    _ => Err(format!("Unable to serialize from_ms ({:?})/to_ms ({:?}) to AVRO Long field.", from_ms, to_ms))
                }
            }

            Message::ServicesFFprobeRequest {
                request_id,
                topic,
                url,
                attributes
            } => {
                Ok(mb.build_services_ffprobe_request(*request_id, topic.clone(), url.clone(), attributes.clone()))
            }

            Message::ServicesFFprobeResponse { request_id, streams } => {
                Ok(mb.build_services_ffprobe_response(*request_id, streams.clone()))
            }

            _ => Err(format!("Message {:?} can not be serialized", self))
        }
    }
}

