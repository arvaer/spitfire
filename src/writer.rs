use std::{collections::HashMap, path::PathBuf};
use tokio::{
    fs::File,
    io::BufWriter,
    sync::{
        mpsc::{self, channel},
        oneshot, Mutex,
    },
    task::spawn_blocking,
};

struct Writer {
    message_id: Mutex<usize>,
    receiver: mpsc::Receiver<WriterMessage>,
    bufwriter: HashMap<PathBuf, Mutex<BufWriter<File>>>,
}
impl Writer {
    fn new(receiver: mpsc::Receiver<WriterMessage>) -> Self {
        Self {
            message_id: Mutex::new(0),
            receiver,
            bufwriter: HashMap::new(),
        }
    }
    async fn handle_message(&mut self, message: WriterMessage) {
        match message {
            WriterMessage::BeginWriting {
                filename,
                respond_to,
            } => {
                let mut guard = self.message_id.lock().await;
                *guard += 1;
                drop(guard);

                self.bufwriter.entry(filename.clone()).or_insert({
                    let file = File::options()
                        .append(true)
                        .create(true)
                        .open(&filename)
                        .await
                        .unwrap();
                    Mutex::new(BufWriter::new(file))
                });

                let response = Packet {
                    header: String::from("Begin Writing"),
                    body: String::from("BeginWriting Finished"),
                    status: 200,
                };

                let _ = respond_to.send(response);
            }

            WriterMessage::Write {
                message,
                respond_to,
            } => {
                todo!();
            }
            _ => {}
        }
    }
}

struct Packet {
    header: String,
    body: String,
    status: usize,
}

enum WriterMessage {
    BeginWriting {
        filename: PathBuf,
        respond_to: oneshot::Sender<Packet>,
    },
    Write {
        message: Packet,
        respond_to: mpsc::Sender<Packet>,
    },
    EndWriting {
        respond_to: oneshot::Sender<String>,
    },
}

struct WriterHandle {
    sender: mpsc::Sender<WriterMessage>,
}
async fn run_writer(mut writer: Writer) {
    while let Some(message) = writer.receiver.recv().await {
        writer.handle_message(message).await;
    }
}

impl WriterHandle {
    pub async fn new(&mut self) -> Self {
        let (sender, receiver) = channel(100);
        let writer = Writer::new(receiver);
        tokio::spawn(run_writer(writer));

        return Self { sender };
    }

    pub async fn begin_writing(&mut self, filename: PathBuf) -> Packet {
        let (sender, receiver) = oneshot::channel();
        let message = WriterMessage::BeginWriting {
            respond_to: sender,
            filename,
        };
        let _ = self.sender.send(message).await.unwrap();
        receiver.await.unwrap()
    }
}



#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_be
}

