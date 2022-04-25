use avro_rs::types::Value;

use crate::{StreamName, TrackName, TrackType};
use crate::message_builder::*;
use crate::message_builder::media_store::*;
use crate::utils::fill_byte_array;


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
pub enum Message {
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
    ParsingError(String),
}

impl Message {


    pub fn from(kind: &String, value: Value) -> Message {
        match kind.as_str() {
            STREAM_TRACK_UNITS_REQUEST_SCHEMA => load_stream_track_units_request(value),
            STREAM_TRACK_UNITS_RESPONSE_SCHEMA => load_stream_track_units_response(value),
            _ => Message::ParsingError(kind.clone())
        }
    }

    pub fn dump(&self, mb: &MessageBuilder) -> Result<Vec<u8>, String> {
        match self {

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

            _ => Err(format!("Message {:?} can not be serialized", self))
        }
    }
}

