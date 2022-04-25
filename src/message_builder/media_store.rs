use avro_rs::types::Value;

use crate::{StreamName, TrackName, TrackType};
use crate::message_builder::{MessageBuilder, STREAM_TRACK_UNITS_REQUEST_SCHEMA,
                             STREAM_TRACK_UNITS_RESPONSE_SCHEMA};
use crate::protocol::{Message, Unit};



fn get_track_type_enum(track_type: &TrackType) -> Value {
    match track_type {
        TrackType::Video => Value::Enum(0, "VIDEO".into()),
        TrackType::Meta => Value::Enum(1, "META".into()),
        TrackType::NotImplemented => panic!("Not supported track type")
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



pub fn build_stream_track_units_request(
    mb: &MessageBuilder,
    request_id: i64,
    topic: String,
    stream_name: StreamName,
    track_type: &TrackType,
    track_name: TrackName,
    from_ms: i64,
    to_ms: i64,
) -> Vec<u8> {
    let mut record = mb.get_record(STREAM_TRACK_UNITS_REQUEST_SCHEMA);
    record.put("request_id", Value::Long(request_id));
    record.put("topic", Value::String(topic));
    record.put(
        "stream_unit",
        get_stream_unit(stream_name, track_type, track_name, 0),
    );
    record.put("from_ms", Value::Long(from_ms));
    record.put("to_ms", Value::Long(to_ms));
    mb.pack_message_into_envelope(STREAM_TRACK_UNITS_REQUEST_SCHEMA, record)
}

pub fn build_stream_track_units_response(
    mb: &MessageBuilder,
    request_id: i64,
    stream_name: StreamName,
    track_type: &TrackType,
    track_name: TrackName,
    from_ms: i64,
    to_ms: i64,
    units: &Vec<i64>,
) -> Vec<u8> {
    let mut record = mb.get_record(STREAM_TRACK_UNITS_RESPONSE_SCHEMA);
    record.put("request_id", Value::Long(request_id));
    record.put(
        "stream_unit",
        get_stream_unit(stream_name, track_type, track_name, 0),
    );
    record.put("from_ms", Value::Long(from_ms));
    record.put("to_ms", Value::Long(to_ms));
    let values: Vec<Value> = units.iter().map(|x| Value::Long(*x)).collect();
    record.put("units", Value::Array(values));
    mb.pack_message_into_envelope(STREAM_TRACK_UNITS_RESPONSE_SCHEMA, record)
}




pub fn load_stream_track_units_request(value: Value) -> Message {
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

pub fn load_stream_track_units_response(value: Value) -> Message {
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

