use crate::avro::{Builder, ProtocolMessage, KEEPALIVE_MESSAGE_SCHEMA};
use crate::objects::{FromProtocolMessage, ToProtocolMessage};
use avro_rs::types::Value;
use log::warn;
use pyo3::prelude::*;

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct KeepAliveMessage {
    #[pyo3(get, set)]
    pub module_id: String,
}

#[pymethods]
impl KeepAliveMessage {
    #[new]
    pub fn new(module_id: String) -> KeepAliveMessage {
        KeepAliveMessage { module_id }
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

impl FromProtocolMessage for KeepAliveMessage {
    fn load(message: &ProtocolMessage) -> Option<Self>
    where
        Self: Sized,
    {
        if message.schema != KEEPALIVE_MESSAGE_SCHEMA {
            return None;
        }
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [(_, Value::String(module_id))] => Some(KeepAliveMessage {
                    module_id: module_id.clone(),
                }),
                _ => {
                    warn!("Unable to match AVRO Record to to KeepAliveMessage");
                    None
                }
            },
            _ => {
                warn!("Unable to match AVRO Record.");
                None
            }
        }
    }
}

impl ToProtocolMessage for KeepAliveMessage {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut object = mb.get_record(KEEPALIVE_MESSAGE_SCHEMA);
        object.put("module_id", Value::String(self.module_id.clone()));

        Some(ProtocolMessage {
            schema: String::from(KEEPALIVE_MESSAGE_SCHEMA),
            object: Value::from(object),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::avro::Builder;
    use crate::objects::services::keep_alive::KeepAliveMessage;
    use crate::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::utils::get_avro_path;

    #[test]
    fn test_load_save() {
        let mb = Builder::new(get_avro_path().as_str());
        let req = KeepAliveMessage::new("module".into());

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = KeepAliveMessage::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }
}
