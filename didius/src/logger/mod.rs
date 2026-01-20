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

pub struct Logger {
    config: LoggerConfig,
    buffer: Arc<Mutex<Vec<Message>>>,
    is_running: Arc<Mutex<bool>>,
    handle: Option<thread::JoinHandle<()>>,
}

impl Logger {
    pub fn new(config: LoggerConfig) -> Self {
        Logger {
            config,
            buffer: Arc::new(Mutex::new(Vec::new())),
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

        let buffer = self.buffer.clone();
        let destination = self.config.destination.clone();
        let interval = self.config.flush_interval_seconds;
        let running_clone = self.is_running.clone();

        self.handle = Some(thread::spawn(move || {
            // Create tokio runtime for this thread if needed for S3
            let rt = Runtime::new().ok(); 

            loop {
                // Sleep
                thread::sleep(Duration::from_secs(interval));
                
                {
                    let r = running_clone.lock().unwrap();
                    if !*r {
                        // Flush one last time
                        Self::flush(&buffer, &destination, rt.as_ref());
                        break;
                    }
                }
                
                Self::flush(&buffer, &destination, rt.as_ref());
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
        let mut buf = self.buffer.lock().unwrap();
        buf.push(msg);
        // Explicit batch size flush check could act here too
        if buf.len() >= self.config.batch_size {
             // To avoid blocking the log call with IO, we might signal the flusher?
             // Or just let the timer handle it for now to stay non-blocking.
             // Or spawn a quick flush?
             // For simplicity, we rely on the timer in MVP.
        }
    }

    fn flush(buffer: &Arc<Mutex<Vec<Message>>>, destination: &LogDestinationInfo, rt: Option<&Runtime>) {
        let mut messages = {
            let mut b = buffer.lock().unwrap();
            if b.is_empty() { return; }
            std::mem::take(&mut *b)
        };
        
        match destination {
            LogDestinationInfo::LocalFile { path } => {
                if let Err(e) = Self::write_to_file(path, &messages) {
                    eprintln!("Failed to write logs to file: {}", e);
                }
            },
            LogDestinationInfo::AmazonS3 { bucket, key_prefix, region: _region_u } => {
                if let Some(runtime) = rt {
                    // Upload to S3
                    // We need to load config? For efficiency, we should probably load config ONCE.
                    // But for now, let's load it here or assume environment.
                    // The user said auth/aws.yaml has credentials.
                    
                    // Note: In a real high-perf logger, we'd initialize the client once at start() and pass it.
                    // For this implementation refactor, I'll load it inside flush (inefficient but safe) or better, 
                    // init it in spawn() and pass to flush.
                    // Since I can't easily change flush signature without affecting generic structure too much,
                    // I'll do it inside runtime block for now, but optimize later if needed.
                    
                    runtime.block_on(async {
                        let config = match crate::logger::aws::load_aws_config("auth/aws.yaml").await {
                            Ok(c) => c,
                            Err(e) => {
                                eprintln!("Failed to load AWS config: {}", e);
                                return;
                            }
                        };
                        
                        let client = S3Client::new(&config);
                        
                        // Create object content (JSONL)
                        let mut content = String::new();
                        for msg in &messages {
                            if let Ok(json) = serde_json::to_string(msg) {
                                content.push_str(&json);
                                content.push('\n');
                            }
                        }
                        
                        // Key: prefix/timestamp.jsonl
                        let timestamp = chrono::Local::now().format("%Y%m%d_%H%M%S").to_string();
                        let key = format!("{}/{}_{}.jsonl", key_prefix, timestamp, uuid::Uuid::new_v4());
                        
                        let result = client.put_object()
                            .bucket(bucket)
                            .key(&key)
                            .body(ByteStream::from(content.into_bytes()))
                            .send()
                            .await;
                            
                        if let Err(e) = result {
                            eprintln!("S3 Upload Failed: {}", e);
                        } else {
                            println!("S3 Upload Success: s3://{}/{}", bucket, key);
                        }
                    });
                } else {
                    eprintln!("Tokio Runtime not available for S3 upload");
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
