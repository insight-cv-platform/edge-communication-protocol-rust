mod services;

use crate::protocol2::avro::{Builder, ProtocolMessage};

trait FromProtocolMessage {
    fn load(message: &ProtocolMessage) -> Option<Self> where Self: Sized;
}

trait ToProtocolMessage {
    fn save(&self, mb: &Builder) -> Option<ProtocolMessage>;
}

