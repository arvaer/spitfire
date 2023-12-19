use std::fs::File;
use std::io::prelude::*;
use std::path::PathBuf;
use tokio::sync::{mpsc, oneshot};

pub struct Mapper {
    receiver: mpsc::Receiver<MapperMessage>,
    message_id: usize,
}

pub enum MapperMessage {
    GetId {
        respond_to: oneshot::Sender<usize>,
    },
    ProcessFile {
        filename: PathBuf,
        respond_to: oneshot::Sender<String>,
    },
}

impl Mapper {
    fn new(receiver: mpsc::Receiver<MapperMessage>) -> Self {
        Mapper {
            receiver,
            message_id: 0,
        }
    }

    fn handle_message(&mut self, msg: MapperMessage) {
        match msg {
            MapperMessage::GetId { respond_to } => {
                self.message_id += 1;
                let _ = respond_to.send(self.message_id);
            }
            MapperMessage::ProcessFile {
                filename,
                respond_to,
            } => {
                let mut file = File::open(&filename).unwrap();
                let mut contents = String::new();
                file.read_to_string(&mut contents).unwrap();
                let _ = respond_to.send(contents);
            }
        }
    }
}

pub async fn run_mapper(mut mapper: Mapper) {
    while let Some(msg) = mapper.receiver.recv().await {
        mapper.handle_message(msg);
    }
}

#[derive(Clone)]
pub struct HandleMapper {
    sender: mpsc::Sender<MapperMessage>,
}

impl HandleMapper {
    pub fn new() -> Self {
        let (sender, receiver) = mpsc::channel(8);
        let mapper = Mapper::new(receiver);
        tokio::spawn(run_mapper(mapper));

        Self { sender }
    }
    pub async fn get_unique_id(&self) -> usize {
        let (send, recv) = oneshot::channel();
        let message = MapperMessage::GetId { respond_to: send };
        let _ = self.sender.send(message).await;
        recv.await.expect("Actor died")
    }
    pub async fn load_file(&self, filename: PathBuf) -> String {
        let (send, recv) = oneshot::channel();
        let message = MapperMessage::ProcessFile {
            filename,
            respond_to: send,
        };
        let _ = self.sender.send(message).await;
        recv.await.expect("Actor ded")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_mapper() {
        let mapper = HandleMapper::new();
        let id1 = mapper.get_unique_id().await;
        let id2 = mapper.get_unique_id().await;
        println!("id1: {}, id2: {}", id1, id2);
        assert_eq!(id1 + 1, id2);
    }

    #[tokio::test]
    async fn test_mapper_load_file() {
        let mapper = HandleMapper::new();
        let res = mapper.load_file(PathBuf::from("./test.txt")).await;
        assert_eq!("Hello World!\n", &res);
    }
}
