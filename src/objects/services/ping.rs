use crate::avro::{Builder, ProtocolMessage, PING_REQUEST_RESPONSE_SCHEMA};
use crate::objects::{FromProtocolMessage, ToProtocolMessage};
use avro_rs::types::Value;
use log::warn;
use pyo3::prelude::*;

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub enum PingRequestResponseType {
    Request,
    Response,
}

#[derive(Debug, Clone, PartialEq)]
#[pyclass]
pub struct PingRequestResponse {
    #[pyo3(get, set)]
    pub request_id: i64,
    #[pyo3(get, set)]
    pub topic: String,
    #[pyo3(get, set)]
    pub mtype: PingRequestResponseType,
}

#[pymethods]
impl PingRequestResponse {
    #[new]
    pub fn new(
        request_id: i64,
        topic: String,
        mtype: PingRequestResponseType,
    ) -> PingRequestResponse {
        PingRequestResponse {
            request_id,
            topic,
            mtype,
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

impl FromProtocolMessage for PingRequestResponse {
    fn load(message: &ProtocolMessage) -> Option<Self>
    where
        Self: Sized,
    {
        if message.schema != PING_REQUEST_RESPONSE_SCHEMA {
            return None;
        }
        match &message.object {
            Value::Record(fields) => match fields.as_slice() {
                [(_, Value::Long(request_id)), (_, Value::String(topic)), (_, Value::Enum(_index, ping_m_type))] => {
                    Some(PingRequestResponse {
                        request_id: *request_id,
                        topic: topic.clone(),
                        mtype: if ping_m_type.as_str() == "REQUEST" {
                            PingRequestResponseType::Request
                        } else {
                            PingRequestResponseType::Response
                        },
                    })
                }
                _ => {
                    warn!("Unable to match AVRO Record to to PingRequestResponse");
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

impl ToProtocolMessage for PingRequestResponse {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage> {
        let mut object = mb.get_record(PING_REQUEST_RESPONSE_SCHEMA);
        object.put("request_id", Value::Long(self.request_id));
        object.put("topic", Value::String(self.topic.clone()));
        match self.mtype {
            PingRequestResponseType::Request => {
                object.put("type", Value::Enum(0, "REQUEST".into()));
            }
            PingRequestResponseType::Response => {
                object.put("type", Value::Enum(1, "RESPONSE".into()));
            }
        }

        Some(ProtocolMessage {
            schema: String::from(PING_REQUEST_RESPONSE_SCHEMA),
            object: Value::from(object),
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::avro::Builder;
    use crate::objects::services::ping::{PingRequestResponse, PingRequestResponseType};
    use crate::objects::{FromProtocolMessage, ToProtocolMessage};
    use crate::utils::get_avro_path;

    fn test_load_save_req_rep(mt: PingRequestResponseType) {
        let mb = Builder::new(get_avro_path().as_str());
        let req = PingRequestResponse::new(0, String::from("test"), mt);

        let req_envelope_opt = req.save(&mb);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();
        let req_serialized = mb.save_from_avro(req_envelope);

        let req_envelope_opt = mb.load_to_avro(req_serialized);
        assert!(req_envelope_opt.is_some());

        let req_envelope = req_envelope_opt.unwrap();

        let new_req_opt = PingRequestResponse::load(&req_envelope);

        assert!(new_req_opt.is_some());

        let new_req = new_req_opt.unwrap();

        assert_eq!(req, new_req);
    }

    #[test]
    fn test_load_save_ping() {
        test_load_save_req_rep(PingRequestResponseType::Request);
        test_load_save_req_rep(PingRequestResponseType::Response);
    }
}
