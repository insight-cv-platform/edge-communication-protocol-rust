use crate::protocol::primitives::{StreamName, track_type_literal_to_track_type, TrackInfo, TrackName, TrackType};
use avro_rs::types::Value;
use log::warn;
use pyo3::prelude::*;
use crate::protocol::avro::{Builder, ProtocolMessage, STREAM_TRACKS_REQUEST_SCHEMA, STREAM_TRACKS_RESPONSE_SCHEMA, TRACK_INFO_SCHEMA};
use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
use crate::utils::fill_byte_array;

fn get_track_type_enum(track_type: &TrackType) -> Value {
    match track_type {
        TrackType::Video => Value::Enum(0, "VIDEO".into()),
        TrackType::Meta => Value::Enum(1, "META".into()),
        TrackType::NotImplemented => panic!("Not supported track type")
    }
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct StreamTracksResponse {
    #[pyo3(get, set)]
    pub request_id: i64,
    #[pyo3(get, set)]
    pub stream_name: StreamName,
    #[pyo3(get, set)]
    pub tracks: Vec<TrackInfo>,
}

#[pymethods]
impl StreamTracksResponse {
    #[new]
    pub fn new(request_id: i64,
               stream_name: StreamName,
               tracks: Vec<TrackInfo>) -> Self {
        StreamTracksResponse {
            request_id,
            stream_name,
            tracks,
        }
    }
}

impl ToProtocolMessage for StreamTracksResponse {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(STREAM_TRACKS_RESPONSE_SCHEMA);
        obj.put("request_id", Value::Long(self.request_id));
        obj.put("stream_name", Value::Bytes(self.stream_name.to_vec()));
        let tracks: Vec<Value> = self.tracks
            .iter()
            .map(|track_info| {
                let mut r = mb.get_record(TRACK_INFO_SCHEMA);
                r.put("name", Value::Bytes(track_info.track_name.to_vec()));
                r.put("type", get_track_type_enum(&track_info.track_type));
                r.into()
            })
            .collect();
        obj.put("tracks", Value::Array(tracks));

        Some(ProtocolMessage {
            schema: String::from(STREAM_TRACKS_RESPONSE_SCHEMA),
            object: Value::from(obj),
        })
    }
}

impl FromProtocolMessage for StreamTracksResponse {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        if message.schema != STREAM_TRACKS_RESPONSE_SCHEMA {
            return None;
        }
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::Bytes(stream_name)),
                (_, Value::Array(tracks))
                ] => {
                    let mut sn = StreamName::default();
                    fill_byte_array(&mut sn, stream_name);
                    let track_records: Vec<Option<TrackInfo>> = tracks.iter().map(|t| match t {
                        Value::Record(fields) => {
                            match fields.as_slice() {
                                [
                                (_, Value::Bytes(track_name)),
                                (_, Value::Enum(_, track_type))
                                ] => {
                                    let mut tn = TrackName::default();
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
                        warn!("Not all track info records are parsed well.");
                        None
                    } else {
                        Some(StreamTracksResponse {
                            request_id: request_id.clone(),
                            stream_name: sn,
                            tracks: valid_track_records,
                        })
                    }
                }
                _ => {
                    warn!("Unable to match AVRO Record to to StreamTracksResponse");
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

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct StreamTracksRequest {
    #[pyo3(get, set)]
    pub request_id: i64,
    #[pyo3(get, set)]
    pub topic: String,
    #[pyo3(get, set)]
    pub stream_name: StreamName,
}

#[pymethods]
impl StreamTracksRequest {
    #[new]
    pub fn new(request_id: i64,
               topic: String,
               stream_name: StreamName) -> Self {
        StreamTracksRequest {
            request_id,
            topic,
            stream_name,
        }
    }
}

impl ToProtocolMessage for StreamTracksRequest {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(STREAM_TRACKS_REQUEST_SCHEMA);
        obj.put("request_id", Value::Long(self.request_id));
        obj.put("topic", Value::String(self.topic.clone()));
        obj.put("stream_name", Value::Bytes(self.stream_name.to_vec()));
        Some(ProtocolMessage {
            schema: String::from(STREAM_TRACKS_REQUEST_SCHEMA),
            object: Value::from(obj),
        })
    }
}

impl FromProtocolMessage for StreamTracksRequest {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        if message.schema != STREAM_TRACKS_REQUEST_SCHEMA {
            return None;
        }
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [
                (_, Value::Long(request_id)),
                (_, Value::String(topic)),
                (_, Value::Bytes(stream_name))
                ] => {
                    let mut sn = StreamName::default();
                    fill_byte_array(&mut sn, stream_name);
                    Some(StreamTracksRequest {
                        request_id: request_id.clone(),
                        topic: topic.clone(),
                        stream_name: sn,
                    })
                }
                _ => {
                    warn!("Unable to match AVRO Record to to StreamTracksRequest");
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


#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use crate::protocol::avro::Builder;
    use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::protocol::objects::services::storage::stream_tracks::{StreamTracksRequest, StreamTracksResponse};
    use crate::protocol::primitives::{pack_stream_name, pack_track_name, TrackInfo, TrackType};
    use crate::utils::get_avro_path;

    #[test]
    fn test_load_save_req() {
        let mb = Builder::new(get_avro_path().as_str());

        let stream_uuid = Uuid::parse_str("fa807469-fbb3-4f63-b1a9-f63fbbf90f41").unwrap();
        let stream_name = pack_stream_name(&stream_uuid);

        let req = StreamTracksRequest::new(0, String::from("test"), stream_name);

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = StreamTracksRequest::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }


    #[test]
    fn test_load_save_rep() {
        let mb = Builder::new(get_avro_path().as_str());

        let stream_uuid = Uuid::parse_str("fa807469-fbb3-4f63-b1a9-f63fbbf90f41").unwrap();
        let stream_name = pack_stream_name(&stream_uuid);
        let track_name = pack_track_name(&String::from("test")).unwrap();


        let rep = StreamTracksResponse::new(0, stream_name, vec![
            TrackInfo::new(TrackType::Video, track_name)
        ]);

        let rep_envelope_opt = rep.save(&mb);
        assert!(rep_envelope_opt.is_some());

        let rep_envelope = rep_envelope_opt.unwrap();
        let rep_serialized = mb.save_from_avro(rep_envelope);

        let rep_envelope_opt = mb.load_to_avro(rep_serialized);
        assert!(rep_envelope_opt.is_some());

        let rep_envelope = rep_envelope_opt.unwrap();

        let new_rep_opt = StreamTracksResponse::load(&rep_envelope);

        assert!(new_rep_opt.is_some());

        let new_rep = new_rep_opt.unwrap();

        assert_eq!(rep, new_rep);
    }
}