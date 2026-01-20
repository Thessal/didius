pub mod message;
pub mod config;
pub mod aws;

use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;
use crate::logger::message::Message;
use crate::logger::config::{LoggerConfig, LogDestinationInfo};
use anyhow::Result;
use std::fs::OpenOptions;
use std::io::Write;
use aws_sdk_s3::Client as S3Client;
use aws_sdk_s3::primitives::ByteStream;
use tokio::runtime::Runtime;

use std::sync::mpsc;
use crate::logger::message::AsyncMessage;

pub struct Logger {
    config: LoggerConfig,
    sender: Option<mpsc::Sender<AsyncMessage>>,
    is_running: Arc<Mutex<bool>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Logger {
    pub fn new(config: LoggerConfig) -> Self {
        Logger {
            config,
            sender: None,
            is_running: Arc::new(Mutex::new(false)),
            handle: None,
        }
    }

    pub fn start(&mut self) {
        let running = self.is_running.clone();
        {
            let mut r = running.lock().unwrap();
            if *r { return; }
            *r = true;
        }

        let (tx, rx) = mpsc::channel();
        self.sender = Some(tx);
        
        let destination = self.config.destination.clone();
        let interval = self.config.flush_interval_seconds;
        let batch_size = self.config.batch_size;
        let running_clone = self.is_running.clone();

        self.handle = Some(thread::spawn(move || {
            // Create tokio runtime for this thread if needed for S3
            let rt = Runtime::new().ok(); 
            let mut buffer = Vec::new();
            let mut last_flush = std::time::Instant::now();

            loop {
                // Non-blocking try_recv loop with sleep? OR select?
                // Or recv_timeout?
                // Let's use recv_timeout matching flush interval approximately? 
                // Or just loop processing messages and check time.
                
                // If we use simple recv, we might block and miss flush interval if no msgs come?
                // Actually if no msgs come, we don't *need* to flush (buffer empty).
                // So recv_timeout is efficient.
                
                match rx.recv_timeout(Duration::from_millis(100)) {
                    Ok(async_msg) => {
                         // Convert AsyncMessage -> Message (This executes the closure!)
                         let msg = async_msg.into_message();
                         buffer.push(msg);
                         
                         if buffer.len() >= batch_size {
                             Self::flush_buffer(&mut buffer, &destination, rt.as_ref());
                             last_flush = std::time::Instant::now();
                         }
                    },
                    Err(mpsc::RecvTimeoutError::Timeout) => {
                        // Check time
                        if last_flush.elapsed() >= Duration::from_secs(interval) {
                             if !buffer.is_empty() {
                                 Self::flush_buffer(&mut buffer, &destination, rt.as_ref());
                                 last_flush = std::time::Instant::now();
                             }
                        }
                    },
                    Err(mpsc::RecvTimeoutError::Disconnected) => break,
                }
                
                {
                    let r = running_clone.lock().unwrap();
                    if !*r {
                        // Drain remaining
                        while let Ok(amsg) = rx.try_recv() {
                             buffer.push(amsg.into_message());
                        }
                        if !buffer.is_empty() {
                             Self::flush_buffer(&mut buffer, &destination, rt.as_ref());
                        }
                        break;
                    }
                }
            }
        }));
    }

    pub fn stop(&mut self) {
        {
            let mut r = self.is_running.lock().unwrap();
            *r = false;
        }
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }

    pub fn log(&self, msg: Message) {
        if let Some(tx) = &self.sender {
            let _ = tx.send(AsyncMessage::from(msg));
        }
    }
    
    pub fn log_lazy(&self, log_type: String, f: Box<dyn FnOnce() -> serde_json::Value + Send>) {
        if let Some(tx) = &self.sender {
             let _ = tx.send(AsyncMessage::new_lazy(log_type, f));
        }
    }

    fn flush_buffer(messages: &mut Vec<Message>, destination: &LogDestinationInfo, rt: Option<&Runtime>) {
        if messages.is_empty() { return; }
        // Take messages out of buffer to flush, clearing buffer
        let batch = std::mem::take(messages);
        
        match destination {
            LogDestinationInfo::LocalFile { path } => {
                if let Err(e) = Self::write_to_file(path, &batch) {
                    eprintln!("Failed to write logs to file: {}", e);
                }
            },
            LogDestinationInfo::AmazonS3 { bucket, key_prefix, region: _ } => {
                if let Some(runtime) = rt {
                     runtime.block_on(async {
                        // Re-use logic or refactor. Copying logic for now.
                        let config = match crate::logger::aws::load_aws_config("auth/aws.yaml").await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("Failed to load AWS config: {}", e);
                                return;
                            }
                        };
                         let client = S3Client::new(&config);
                         let mut content = String::new();
                         for msg in &batch {
                             if let Ok(json) = serde_json::to_string(msg) {
                                 content.push_str(&json);
                                 content.push('\n');
                             }
                         }
                         let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
                         let key = format!("{}/{}_{}.jsonl", key_prefix, timestamp, uuid::Uuid::new_v4());
                         
                         let result = client.put_object()
                                .bucket(bucket)
                                .key(&key)
                                .body(ByteStream::from(content.into_bytes()))
                                .send()
                                .await;
                         if let Err(e) = result { eprintln!("S3 Upload Failed: {}", e); }
                     });
                }
            }
        }
    }



    fn write_to_file(path: &str, messages: &[Message]) -> Result<()> {
        let mut file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;
            
        for msg in messages {
            let json = serde_json::to_string(msg)?;
            writeln!(file, "{}", json)?;
        }
        Ok(())
    }
}

impl Drop for Logger {
    fn drop(&mut self) {
        self.stop();
    }
}
