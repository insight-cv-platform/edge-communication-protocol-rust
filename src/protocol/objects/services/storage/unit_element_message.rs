use std::collections::HashMap;
use avro_rs::types::Value;
use log::warn;
use pyo3::prelude::*;
use crate::protocol::primitives::{ElementType, Unit};
use crate::protocol::avro::{Builder, ProtocolMessage, UNIT_ELEMENT_MESSAGE_SCHEMA};
use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
use crate::utils::value_to_string;

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct UnitElementMessage {
    #[pyo3(get, set)]
    pub stream_unit: Unit,
    #[pyo3(get, set)]
    pub element: ElementType,
    #[pyo3(get, set)]
    pub value: Vec<u8>,
    #[pyo3(get, set)]
    pub attributes: HashMap<String, String>,
    #[pyo3(get, set)]
    pub last: bool,
}

#[pymethods]
impl UnitElementMessage {
    #[new]
    pub fn new(stream_unit: Unit,
               element: ElementType,
               value: Vec<u8>,
               attributes: HashMap<String, String>,
               last: bool) -> Self {
        UnitElementMessage {
            stream_unit,
            element,
            value,
            attributes,
            last,
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

impl FromProtocolMessage for UnitElementMessage {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized {
        if message.schema != UNIT_ELEMENT_MESSAGE_SCHEMA {
            return None;
        }

        match &message.object {
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
                        Some(UnitElementMessage {
                            stream_unit: Unit::new(stream_name.clone(), track_name.clone(), track_type.clone(), *unit),
                            element: element.clone() as i16,
                            value: value.clone(),
                            attributes: attributes.iter().map(|x| (x.0.clone(), value_to_string(x.1).or(Some(String::from(""))).unwrap())).collect(),
                            last: last.clone(),
                        })
                    }
                    _ => {
                        warn!("Unable to match AVRO Record to Unit");
                        None
                    }
                }
                _ => {
                    warn!("Unable to match AVRO Record to to UnitElementMessage");
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

impl ToProtocolMessage for UnitElementMessage {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut obj = mb.get_record(UNIT_ELEMENT_MESSAGE_SCHEMA);
        obj.put("stream_unit", self.stream_unit.to_avro_record());
        obj.put("element", Value::Long(self.element.into()));
        obj.put("value", Value::Bytes(self.value.clone()));
        obj.put("attributes", Value::Map(self.attributes.iter().map(|x| (x.0.clone(), Value::String(x.1.clone()))).collect()));
        obj.put("last", Value::Boolean(self.last));

        Some(ProtocolMessage {
            schema: String::from(UNIT_ELEMENT_MESSAGE_SCHEMA),
            object: Value::from(obj),
        })
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use uuid::Uuid;
    use crate::protocol::avro::Builder;
    use crate::protocol::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::protocol::objects::services::storage::unit_element_message::UnitElementMessage;
    use crate::protocol::primitives::{pack_stream_name, pack_track_name, Unit};
    use crate::utils::get_avro_path;

    #[test]
    fn test_load_save_req() {
        let mb = Builder::new(get_avro_path().as_str());

        let track_name = pack_track_name(&String::from("test")).unwrap();
        let stream_uuid = Uuid::parse_str("fa807469-fbb3-4f63-b1a9-f63fbbf90f41").unwrap();
        let stream_name = pack_stream_name(&stream_uuid);

        let req = UnitElementMessage::new(
            Unit::new(stream_name.to_vec(), track_name.to_vec(), String::from("VIDEO"), 3),
            2,
            vec![0, 1],
            HashMap::from([("a".into(), "b".into()), ("c".into(), "d".into())]),
            true,
        );

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = UnitElementMessage::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }
}