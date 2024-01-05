use tokio::{sync::{ mpsc::{self, channel}, oneshot, Mutex}, task::spawn_blocking, io::BufWriter, fs::File};
use std::{collections::HashMap, path::PathBuf};

struct Writer {
    message_id: Mutex<usize>,
    receiver: mpsc::Receiver<WriterMessage>,
    bufwriter: HashMap<PathBuf,Mutex<BufWriter<File>>>
}
impl Writer {
    fn new(receiver:mpsc::Receiver<WriterMessage>) -> Self {
        Self {
            message_id: Mutex::new(0),
            receiver,
            bufwriter: HashMap::new()
        }
    }
    async fn handle_message(&mut self, message: WriterMessage){
        match message {
            WriterMessage::BeginWriting { respond_to, filename: PathBuf } => {
                let mut guard = self.message_id.lock().await;
                *guard += 1;
                drop(guard);

                if let Some(writer) = self.bufwriter.remove(&filename) {
                     let _ = respond_to.send(String::from("found writer. Ready to begin"));
                }
                else {
                    self.bufwriter.insert(filename, Mutex::new(BufWriter::new(&filename)));
                    let _ = respond_to.send(String::from("created new writer. Ready to begin"));
                }




            }
            _ => {}

        }
    }
}

enum WriterMessage {
    BeginWriting {
        respond_to: oneshot::Sender<String>,
        filename: PathBuf
    },
    Write {
        message: String,
        respond_to: mpsc::Sender<String>
    },
    EndWriting {
        respond_to: oneshot::Sender<String>
    }
}

struct WriterHandle {
    sender: mpsc::Sender<WriterMessage>
}
async fn run_writer(mut writer:  Writer) {
    while let Some(message) = writer.receiver.recv().await {
        writer.handle_message(message).await;
    }
}

impl WriterHandle {
    pub async fn new(&mut self) -> Self {
        let (sender, receiver) = channel(100);
        let writer = Writer::new(receiver);
        tokio::spawn(run_writer(writer));

        return Self{
            sender
        };
    }

    pub async fn begin_writing(&mut self) -> String {
        let (sender, receiver) = oneshot::channel();
        let message = WriterMessage::BeginWriting {
            respond_to: sender
        };
        let _ = self.sender.send(message).await.unwrap();
        receiver.await.unwrap()
    }

}

