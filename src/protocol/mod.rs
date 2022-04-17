use std::collections::HashMap;

use avro_rs::types::Value;

use crate::{ElementType, Payload, StreamName, TrackInfo, TrackName, TrackType};
use crate::message_builder::*;
use crate::message_builder::media_store::*;
use crate::message_builder::services::ffprobe::*;
use crate::utils::fill_byte_array;

pub mod media_store;

#[derive(Default, Debug)]
pub struct Unit {
    pub stream_name: StreamName,
    pub track_name: TrackName,
    pub track_type: TrackType,
    pub unit: i64,
}

pub fn track_type_literal_to_track_type(literal: &str) -> TrackType {
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
    ServicesFFProbeRequest {
        request_id: i64,
        topic: String,
        url: String,
        attributes: HashMap<String, String>,
    },
    ServicesFFProbeResponse {
        request_id: i64,
        response_type: ServicesFFProbeResponseType,
        time_spent: i64,
        streams: Vec<HashMap<String, String>>,
    },
    ParsingError(String),
}

impl Message {


    pub fn from(kind: &String, value: Value) -> Message {
        match kind.as_str() {
            UNIT_ELEMENT_MESSAGE_SCHEMA => load_unit_element_message(value),
            NOTIFY_MESSAGE_SCHEMA => load_notify_message(value),
            STREAM_TRACKS_REQUEST_SCHEMA => load_stream_tracks_request(value),
            STREAM_TRACKS_RESPONSE_SCHEMA => load_stream_tracks_response(value),
            STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA => load_stream_track_unit_elements_request(value),
            STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA => load_stream_track_unit_elements_response(value),
            STREAM_TRACK_UNITS_REQUEST_SCHEMA => load_stream_track_units_request(value),
            STREAM_TRACK_UNITS_RESPONSE_SCHEMA => load_stream_track_units_response(value),
            PING_REQUEST_RESPONSE_SCHEMA => load_ping_request_response(value),
            SERVICES_FFPROBE_REQUEST_SCHEMA => load_services_ffprobe_request(value),
            SERVICES_FFPROBE_RESPONSE_SCHEMA => load_services_ffprobe_response(value),
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
            } => Ok(build_unit_element_message(&mb, *stream_name, track_type, *track_name, *unit, *element, value, attributes, *last)),

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
                Ok(build_notify_message(mb, *stream_name, track_type, *track_name, *unit, *saved_ms, last))
            }

            Message::StreamTracksRequest {
                request_id, topic, stream_name
            } => Ok(build_stream_tracks_request(mb, *request_id, topic.clone(), *stream_name)),

            Message::PingRequestResponse {
                request_id, topic, mtype
            } => {
                match mtype {
                    PingRequestResponseType::REQUEST => Ok(build_ping_request_response(&mb, *request_id, topic.clone(), false)),
                    PingRequestResponseType::RESPONSE => Ok(build_ping_request_response(&mb, *request_id, topic.clone(), true))
                }
            }

            Message::StreamTracksResponse {
                request_id, stream_name, tracks
            } => Ok(build_stream_tracks_response(&mb, *request_id, *stream_name,
                                                    &tracks.iter().map(|x| (x.track_name, x.track_type)).collect())),

            Message::StreamTrackUnitElementsRequest {
                request_id,
                topic,
                stream_unit: Unit { stream_name, track_name, track_type, unit },
                max_element
            } => Ok(build_stream_track_unit_elements_request(&mb, *request_id, topic.clone(), *stream_name,
                                                                track_type, *track_name, *unit, *max_element)),

            Message::StreamTrackUnitElementsResponse {
                request_id,
                stream_unit: Unit { stream_name, track_name, track_type, unit },
                values
            } => Ok(build_stream_track_unit_elements_response(&mb, *request_id, *stream_name, track_type, *track_name, *unit, values)),

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
                        Ok(build_stream_track_units_request(&mb, *request_id, topic.clone(), *stream_name, track_type, *track_name, from_ms, to_ms)),
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
                        Ok(build_stream_track_units_response(&mb, *request_id, *stream_name, track_type, *track_name, from_ms, to_ms, units)),
                    _ => Err(format!("Unable to serialize from_ms ({:?})/to_ms ({:?}) to AVRO Long field.", from_ms, to_ms))
                }
            }

            Message::ServicesFFProbeRequest {
                request_id,
                topic,
                url,
                attributes
            } => {
                Ok(build_services_ffprobe_request(mb, *request_id, topic.clone(), url.clone(), attributes.clone()))
            }

            Message::ServicesFFProbeResponse {
                request_id,
                response_type,
                time_spent,
                streams
            } => {
                Ok(build_services_ffprobe_response(mb, *request_id, response_type, *time_spent, streams.clone()))
            }

            _ => Err(format!("Message {:?} can not be serialized", self))
        }
    }
}

