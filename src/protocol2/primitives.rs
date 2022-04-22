use std::collections::HashMap;
use avro_rs::types::Value;
use uuid::Uuid;
use pyo3::prelude::*;
use crate::utils::fill_byte_array;

pub const TRACK_NAME_MAX_LENGTH: usize = 16;
pub const STREAM_NAME_MAX_LENGTH: usize = 16;

pub type StreamName = [u8; STREAM_NAME_MAX_LENGTH];
pub type TrackName = [u8; TRACK_NAME_MAX_LENGTH];
pub type ElementType = i16;

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub enum TrackType {
    Video,
    Meta,
    NotImplemented,
}

impl Default for TrackType {
    fn default() -> Self {
        TrackType::Video
    }
}


#[derive(Debug, Default, Clone, PartialEq)]
#[pyclass]
pub struct Payload {
    pub data: Vec<u8>,
    pub attributes: HashMap<String, String>,
}

#[pymethods]
impl Payload {
    #[new]
    pub fn new(data: Vec<u8>, attributes: HashMap<String, String>) -> Self {
        Payload {
            data,
            attributes,
        }
    }
}

#[derive(Debug, Default, Clone)]
#[pyclass]
pub struct TrackInfo {
    pub track_type: TrackType,
    pub track_name: TrackName,
}

#[pymethods]
impl TrackInfo {
    #[new]
    pub fn new(track_type: TrackType, track_name: TrackName) -> Self {
        TrackInfo {
            track_type,
            track_name,
        }
    }
}

pub fn get_empty_track_name() -> TrackName {
    [0; TRACK_NAME_MAX_LENGTH]
}

pub fn pack_stream_name(stream_name: &Uuid) -> StreamName {
    stream_name.as_bytes().clone()
}

pub fn pack_track_name(track_name: &String) -> std::result::Result<TrackName, String> {
    let name_len = track_name.len();
    if name_len > TRACK_NAME_MAX_LENGTH {
        let error = format!(
            "Invalid track name length. Must me less than {} characters.",
            TRACK_NAME_MAX_LENGTH
        );
        Err(error)
    } else {
        let mut buf: TrackName = get_empty_track_name();
        buf[..name_len].clone_from_slice(track_name.as_bytes());
        Ok(buf)
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
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

#[pymethods]
impl Unit {
    #[new]
    pub fn new(stream_name: Vec<u8>, track_name: Vec<u8>, track_type: String, unit: i64) -> Unit {
        let mut b_stream_name: StreamName = StreamName::default();
        let mut b_track_name: TrackName = TrackName::default();
        fill_byte_array(&mut b_stream_name, &stream_name);
        fill_byte_array(&mut b_track_name, &track_name);
        let track_type = track_type_literal_to_track_type(track_type.as_str());

        Unit {
            stream_name: b_stream_name,
            track_name: b_track_name,
            track_type,
            unit,
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

impl Unit {
    pub fn to_avro_record(&self) -> Value {
        Value::Record(vec![
            ("stream_name".into(), Value::Bytes(self.stream_name.to_vec())),
            ("track_name".into(), Value::Bytes(self.track_name.to_vec())),
            ("track_type".into(), get_track_type_enum(&self.track_type)),
            ("unit".into(), Value::Long(self.unit)),
        ])
    }
}

#[derive(Debug, Clone)]
pub enum NotifyTypeImpl {
    Ready(ElementType),
    New,
}

#[derive(Debug, Clone)]
#[pyclass]
pub struct NotifyType {
    obj: NotifyTypeImpl,
}

#[pymethods]
impl NotifyType {
    #[staticmethod]
    pub fn ready(element: ElementType) -> Self {
        NotifyType {
            obj: NotifyTypeImpl::Ready(element)
        }
    }

    #[staticmethod]
    pub fn new() -> Self {
        NotifyType {
            obj: NotifyTypeImpl::New
        }
    }
}


