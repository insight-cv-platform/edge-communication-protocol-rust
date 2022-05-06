use crate::utils::fill_byte_array;
use avro_rs::types::Value;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::fmt::Debug;
use uuid::Uuid;

pub const TRACK_NAME_MAX_LENGTH: usize = 16;
pub const STREAM_NAME_MAX_LENGTH: usize = 16;

pub type StreamName = [u8; STREAM_NAME_MAX_LENGTH];
pub type TrackName = [u8; TRACK_NAME_MAX_LENGTH];
pub type ElementType = i16;

#[derive(Debug, Clone, PartialEq, Copy, Eq, Hash)]
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
    #[pyo3(get, set)]
    pub data: Vec<u8>,
    #[pyo3(get, set)]
    pub attributes: HashMap<String, String>,
}

#[pymethods]
impl Payload {
    #[new]
    pub fn new(data: Vec<u8>, attributes: HashMap<String, String>) -> Self {
        Payload { data, attributes }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;
}

#[derive(Debug, Default, Clone, PartialEq, Copy)]
#[pyclass]
pub struct TrackInfo {
    #[pyo3(get, set)]
    pub track_type: TrackType,
    #[pyo3(get, set)]
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

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;
}

pub fn get_empty_track_name() -> TrackName {
    [0; TRACK_NAME_MAX_LENGTH]
}

pub fn pack_stream_name(stream_name: &Uuid) -> StreamName {
    *stream_name.as_bytes()
}

pub fn pack_track_name(track_name: &str) -> std::result::Result<TrackName, String> {
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
    #[pyo3(get, set)]
    pub stream_name: StreamName,
    #[pyo3(get, set)]
    pub track_name: TrackName,
    #[pyo3(get, set)]
    pub track_type: TrackType,
    #[pyo3(get, set)]
    pub unit: i64,
}

pub fn track_type_literal_to_track_type(literal: &str) -> TrackType {
    match literal {
        "VIDEO" => TrackType::Video,
        "META" => TrackType::Meta,
        _ => TrackType::NotImplemented,
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

    fn __repr__(&self) -> String {
        format!("{:?}", self)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;
}

fn get_track_type_enum(track_type: &TrackType) -> Value {
    match track_type {
        TrackType::Video => Value::Enum(0, "VIDEO".into()),
        TrackType::Meta => Value::Enum(1, "META".into()),
        TrackType::NotImplemented => panic!("Not supported track type"),
    }
}

impl Unit {
    pub fn to_avro_record(&self) -> Value {
        Value::Record(vec![
            (
                "stream_name".into(),
                Value::Bytes(self.stream_name.to_vec()),
            ),
            ("track_name".into(), Value::Bytes(self.track_name.to_vec())),
            ("track_type".into(), get_track_type_enum(&self.track_type)),
            ("unit".into(), Value::Long(self.unit)),
        ])
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum NotifyTypeImpl {
    Ready(ElementType),
    New,
    NotImplemented,
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct NotifyType {
    pub obj: NotifyTypeImpl,
}

#[allow(clippy::new_without_default)]
#[pymethods]
impl NotifyType {
    #[staticmethod]
    pub fn ready(element: ElementType) -> Self {
        NotifyType {
            obj: NotifyTypeImpl::Ready(element),
        }
    }

    #[staticmethod]
    pub fn new() -> Self {
        NotifyType {
            obj: NotifyTypeImpl::New,
        }
    }

    fn __repr__(&self) -> String {
        format!("{:?}", self.obj)
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }

    #[classattr]
    const __hash__: Option<Py<PyAny>> = None;
}
