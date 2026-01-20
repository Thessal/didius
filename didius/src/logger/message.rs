use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Message {
    pub log_type: String,
    pub log_body: serde_json::Value,
    pub timestamp: f64, // Epoch seconds
}

impl Message {
    pub fn new(log_type: String, log_body: serde_json::Value) -> Self {
        Message {
            log_type,
            log_body,
            timestamp: chrono::Local::now().timestamp_millis() as f64 / 1000.0,
        }
    }
}

pub enum AsyncMessage {
    Computed(Message),
    Lazy {
        log_type: String,
        timestamp: f64,
        generator: Box<dyn FnOnce() -> serde_json::Value + Send>,
    }
}

impl AsyncMessage {
    pub fn new_lazy(log_type: String, f: Box<dyn FnOnce() -> serde_json::Value + Send>) -> Self {
        AsyncMessage::Lazy {
            log_type,
            timestamp: chrono::Local::now().timestamp_millis() as f64 / 1000.0,
            generator: f
        }
    }
    
    pub fn into_message(self) -> Message {
        match self {
            AsyncMessage::Computed(m) => m,
            AsyncMessage::Lazy { log_type, timestamp, generator } => {
                Message {
                    log_type,
                    log_body: generator(),
                    timestamp,
                }
            }
        }
    }
}

impl From<Message> for AsyncMessage {
    fn from(m: Message) -> Self {
        AsyncMessage::Computed(m)
    }
}
