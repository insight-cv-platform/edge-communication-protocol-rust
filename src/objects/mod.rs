pub mod services;

use crate::avro::{Builder, ProtocolMessage};

pub trait FromProtocolMessage {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized;
}

pub trait ToProtocolMessage {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage>;
}

