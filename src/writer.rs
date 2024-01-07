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
                    header: PacketHeader::Ready,
                    body: Some(String::from("BeginWriting Finished")),
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
    header: PacketHeader,
    body: Option<String>,
    status: usize,
}

enum PacketHeader {
    Ready,
    Newline,
    Done,
    Error
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
        respond_to: oneshot::Sender<Packet>,
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
    pub async fn new() -> Self {
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
    async fn test_begin_writing() {
        let mut writer = WriterHandle::new().await;
        let packet = writer.begin_writing(PathBuf::from("test.txt")).await;
        assert_eq!(packet.status, 200);
    }
}

