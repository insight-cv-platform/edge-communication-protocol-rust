use crate::protocol::primitives::{NotifyType, NotifyTypeImpl, Unit};
use pyo3::prelude::*;
use avro_rs::types::Value;
use log::warn;
use crate::protocol::avro::{Builder, NOTIFY_MESSAGE_SCHEMA, ProtocolMessage};
use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct NotifyMessage {
    #[pyo3(get, set)]
    pub stream_unit: Unit,
    #[pyo3(get, set)]
    pub saved_ms: u64,
    #[pyo3(get, set)]
    pub notify_type: NotifyType,
}

#[pymethods]
impl NotifyMessage {
    #[new]
    pub fn new(stream_unit: Unit,
               saved_ms: u64,
               notify_type: NotifyType) -> Self {
        NotifyMessage {
            stream_unit,
            saved_ms,
            notify_type,
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

impl FromProtocolMessage for NotifyMessage {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        if message.schema != NOTIFY_MESSAGE_SCHEMA {
            return None;
        }
        match &message.object {
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
                        Some(NotifyMessage {
                            stream_unit: Unit::new(stream_name.clone(), track_name.clone(), track_type.clone(), *unit),
                            saved_ms: *saved_ms as u64,
                            notify_type: match notify_type.as_str() {
                                "READY" => NotifyType::ready(last_element.clone() as i16),
                                "NEW" => NotifyType::new(),
                                _ => NotifyType { obj: NotifyTypeImpl::NotImplemented }
                            },
                        })
                    }
                    _ => {
                        warn!("Unable to match AVRO Record to Unit");
                        None
                    }
                }
                _ => {
                    warn!("Unable to match AVRO Record to to NotifyMessage");
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

impl ToProtocolMessage for NotifyMessage {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(NOTIFY_MESSAGE_SCHEMA);
        obj.put(
            "stream_unit",
            self.stream_unit.to_avro_record(),
        );
        obj.put("saved_ms", Value::Long(self.saved_ms as i64));
        match &self.notify_type.obj {
            NotifyTypeImpl::Ready(elt) => {
                obj.put("notify_type".into(), Value::Enum(0, "READY".into()));
                obj.put("last_element".into(), Value::Int(*elt as i32));
            }
            NotifyTypeImpl::New => {
                obj.put("notify_type".into(), Value::Enum(1, "NEW".into()));
                obj.put("last_element".into(), Value::Int(-1));
            }
            NotifyTypeImpl::NotImplemented => {
                panic!("Unable to handle unsupported NotifyType");
            }
        }

        Some(ProtocolMessage {
            schema: String::from(NOTIFY_MESSAGE_SCHEMA),
            object: Value::from(obj),
        })
    }
}

#[cfg(test)]
mod tests {
    use uuid::Uuid;
    use crate::protocol::avro::Builder;
    use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::protocol::objects::services::storage::notify_message::NotifyMessage;
    use crate::protocol::primitives::{NotifyType, pack_stream_name, pack_track_name, Unit};
    use crate::utils::get_avro_path;

    fn test_load_save_req_int(notify_type: NotifyType) {
        let mb = Builder::new(get_avro_path().as_str());

        let track_name = pack_track_name(&String::from("test")).unwrap();
        let stream_uuid = Uuid::parse_str("fa807469-fbb3-4f63-b1a9-f63fbbf90f41").unwrap();
        let stream_name = pack_stream_name(&stream_uuid);

        let req = NotifyMessage::new(
            Unit::new(stream_name.to_vec(), track_name.to_vec(), String::from("VIDEO"), 3),
            0, notify_type);

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = NotifyMessage::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }

    #[test]
    fn test_load_save_req() {
        test_load_save_req_int(NotifyType::new());
        test_load_save_req_int(NotifyType::ready(100));
    }
}