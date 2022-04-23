use avro_rs::types::Value;

use crate::{ElementType, Payload, StreamName, TrackName, TrackType};
use crate::{STREAM_NAME_MAX_LENGTH, TRACK_NAME_MAX_LENGTH, TrackInfo};
use crate::message_builder::{MessageBuilder, NOTIFY_MESSAGE_SCHEMA, STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA,
                             STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA, STREAM_TRACK_UNITS_REQUEST_SCHEMA,
                             STREAM_TRACK_UNITS_RESPONSE_SCHEMA, STREAM_TRACKS_REQUEST_SCHEMA, STREAM_TRACKS_RESPONSE_SCHEMA,
                             TRACK_INFO_SCHEMA};
use crate::protocol::{Message, NotifyType, track_type_literal_to_track_type, Unit};
use crate::utils::{fill_byte_array, value_to_string};



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

pub fn build_notify_message(
    mb: &MessageBuilder,
    stream_name: StreamName,
    track_type: &TrackType,
    track_name: TrackName,
    unit: i64,
    saved_ms: u64,
    last_element: Option<ElementType>,
) -> Vec<u8> {
    let mut record = mb.get_record(NOTIFY_MESSAGE_SCHEMA);
    record.put(
        "stream_unit",
        get_stream_unit(stream_name, track_type, track_name, unit),
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

    mb.pack_message_into_envelope(NOTIFY_MESSAGE_SCHEMA, record)
}

pub fn build_stream_track_unit_elements_request(
    mb: &MessageBuilder,
    request_id: i64,
    topic: String,
    stream_name: StreamName,
    track_type: &TrackType,
    track_name: TrackName,
    unit: i64,
    max_element: ElementType,
) -> Vec<u8> {
    let mut record = mb.get_record(STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA);
    record.put("request_id", Value::Long(request_id));
    record.put("topic", Value::String(topic));
    record.put(
        "stream_unit",
        get_stream_unit(stream_name, track_type, track_name, unit),
    );
    record.put("max_element", Value::Long(max_element.into()));
    mb.pack_message_into_envelope(STREAM_TRACK_UNIT_ELEMENTS_REQUEST_SCHEMA, record)
}

pub fn build_stream_track_unit_elements_response(
    mb: &MessageBuilder,
    request_id: i64,
    stream_name: StreamName,
    track_type: &TrackType,
    track_name: TrackName,
    unit: i64,
    values: &Vec<Payload>,
) -> Vec<u8> {
    let mut record = mb.get_record(STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA);
    record.put("request_id", Value::Long(request_id));
    record.put(
        "stream_unit",
        get_stream_unit(stream_name, track_type, track_name, unit),
    );

    fn payload_to_avro(p: &Payload) -> Value {
        Value::Record(vec![
            ("data".into(), Value::Bytes(p.data.clone())),
            ("attributes".into(), Value::Map(p.attributes.iter().map(|x| (x.0.clone(), Value::String(x.1.clone()))).collect())),
        ])
    }

    let values: Vec<Value> = values.iter().map(|x| payload_to_avro(x)).collect();
    record.put("values", Value::Array(values));
    mb.pack_message_into_envelope(STREAM_TRACK_UNIT_ELEMENTS_RESPONSE_SCHEMA, record)
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

pub fn load_notify_message(value: Value) -> Message {
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


pub fn load_stream_track_unit_elements_request(value: Value) -> Message {
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

pub fn load_stream_track_unit_elements_response(value: Value) -> Message {
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

