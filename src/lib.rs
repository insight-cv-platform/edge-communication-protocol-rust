use crate::avro::Builder;
use crate::primitives::{NotifyType, Payload, TrackInfo, TrackType, Unit};
use objects::services::ffprobe::{
    ServicesFFProbeRequest, ServicesFFProbeResponse, ServicesFFProbeResponseType,
};
use objects::services::ping::{PingRequestResponse, PingRequestResponseType};
use objects::services::storage::notify_message::NotifyMessage;
use objects::services::storage::stream_track_unit_elements::{
    StreamTrackUnitElementsRequest, StreamTrackUnitElementsResponse,
};
use objects::services::storage::stream_track_units::{
    StreamTrackUnitsRequest, StreamTrackUnitsResponse,
};
use objects::services::storage::stream_tracks::{StreamTracksRequest, StreamTracksResponse};
use objects::services::storage::unit_element_message::UnitElementMessage;
use pyo3::prelude::*;

pub mod avro;
pub mod objects;
pub mod primitives;
pub mod utils;

#[pymodule]
fn protocol(_py: Python, m: &PyModule) -> PyResult<()> {
    m.add_class::<Builder>()?;
    m.add_class::<UnitElementMessage>()?;
    m.add_class::<NotifyMessage>()?;
    m.add_class::<PingRequestResponse>()?;
    m.add_class::<ServicesFFProbeRequest>()?;
    m.add_class::<ServicesFFProbeResponse>()?;
    m.add_class::<StreamTrackUnitElementsRequest>()?;
    m.add_class::<StreamTrackUnitElementsResponse>()?;
    m.add_class::<StreamTracksRequest>()?;
    m.add_class::<StreamTracksResponse>()?;
    m.add_class::<StreamTrackUnitsRequest>()?;
    m.add_class::<StreamTrackUnitsResponse>()?;
    m.add_class::<PingRequestResponseType>()?;
    m.add_class::<ServicesFFProbeResponseType>()?;
    m.add_class::<Unit>()?;
    m.add_class::<TrackInfo>()?;
    m.add_class::<Payload>()?;
    m.add_class::<TrackType>()?;
    m.add_class::<NotifyType>()?;
    Ok(())
}
