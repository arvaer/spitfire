use std::{collections::HashMap, path::PathBuf};
use tokio::{
    fs::File,
    io::{AsyncWriteExt, BufWriter},
    sync::{
        mpsc::{self, channel},
        oneshot, Mutex,
    },
};

#[derive(Debug)]
pub struct Request {
    header: RequestHeader,
    body: String,
}

#[derive(Debug)]
pub struct Response {
    header: ResponseHeader,
    body: Option<String>,
    status: usize,
}

#[derive(Debug)]
enum RequestHeader {
    Prepare,
    Payload { key: PathBuf },
    Cleanup,
    Error,
}

#[derive(Debug)]
enum ResponseHeader {
    Ready,
    Finished,
    Error,
}

#[derive(Debug)]
enum WriterMessage {
    BeginWriting {
        filename: PathBuf,
        respond_to: oneshot::Sender<Response>,
    },
    Write {
        message: Request,
        respond_to: oneshot::Sender<Response>,
    },
    EndWriting {
        respond_to: oneshot::Sender<Response>,
    },
}

struct Writer {
    message_id: Mutex<usize>,
    bufwriter: HashMap<PathBuf, Mutex<BufWriter<File>>>,
    receiver: mpsc::Receiver<WriterMessage>,
}
impl Writer {
    fn new(receiver: mpsc::Receiver<WriterMessage>) -> Self {
        Self {
            message_id: Mutex::new(0),
            bufwriter: HashMap::new(),
            receiver,
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

                let response = Response {
                    header: ResponseHeader::Ready,
                    body: Some(String::from("BeginWriting Finished")),
                    status: 200,
                };

                let _ = respond_to.send(response);
            }

            WriterMessage::Write {
                message,
                respond_to,
            } => {
                let mut guard = self.message_id.lock().await;
                *guard += 1;
                drop(guard);

                match message.header {
                    RequestHeader::Prepare => {
                        let _ = respond_to.send(Response {
                            header: ResponseHeader::Ready,
                            body: None,
                            status: 200,
                        });
                    }
                    RequestHeader::Payload { key } => {
                        if let Some(writer) = self.bufwriter.get(&key) {
                            let mut guard = writer.lock().await;
                            guard
                                .write(message.body.as_bytes())
                                .await
                                .expect("issue writing to file");
                            //print info about guard
                            guard.flush().await.expect("issue flushing file");
                            drop(guard);
                            let _ = respond_to.send(Response {
                                header: ResponseHeader::Finished,
                                body: None,
                                status: 200,
                            });
                        } else {
                            let _ = respond_to.send(Response {
                                header: ResponseHeader::Error,
                                body: None,
                                status: 400,
                            });
                        }
                    }
                    RequestHeader::Cleanup => {
                        let _ = respond_to.send(Response {
                            header: ResponseHeader::Finished,
                            body: None,
                            status: 200,
                        });
                    }
                    RequestHeader::Error => {
                        let _ = respond_to.send(Response {
                            header: ResponseHeader::Error,
                            body: None,
                            status: 400,
                        });
                    }
                }
            }
            _ => {}
        }
    }
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

    pub async fn begin_writing(&mut self, filename: PathBuf) -> Response {
        let (send, recv) = oneshot::channel();
        let message = WriterMessage::BeginWriting {
            respond_to: send,
            filename,
        };
        let _ = self.sender.send(message).await;
        recv.await.expect("Actor ded")
    }

    pub async fn write_message(&mut self, message: Request) -> Response {
        let (send, recv) = oneshot::channel();
        let message = WriterMessage::Write {
            respond_to: send,
            message,
        };
        println!("Sending message: {:?}", message);

        let _ = self.sender.send(message).await;

        return recv.await.expect("Actor ded");
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

    #[tokio::test]
    async fn test_write_message() {
        let mut writer = WriterHandle::new().await;
        let zero = writer.begin_writing(PathBuf::from("test2.txt")).await;
        println!("{:?}", zero);
        assert_eq!(zero.status, 200);

        let first_message = Request {
            header: RequestHeader::Prepare,
            body: String::from("We're starting now"),
        };
        let first = writer.write_message(first_message).await;
        println!("{:?}", first);
        assert_eq!(first.status, 200);

        let message_second = Request {
            header: RequestHeader::Payload {
                key: PathBuf::from("test2.txt"),
            },
            body: String::from("A new dog is here!"),
        };
        let second = writer.write_message(message_second).await;
        println!("{:?}", second);
        assert_eq!(second.status, 200);

        let message_third = Request {
            header: RequestHeader::Cleanup,
            body: String::from("No more data!"),
        };

        let third = writer.write_message(message_third).await;
        println!("{:?}", third);
        assert_eq!(third.status, 200);

        let file_contents = std::fs::read_to_string("test2.txt").unwrap();
        assert_eq!(file_contents, "A new dog is here!");

        //clean up file
        std::fs::remove_file("test2.txt").unwrap();
    }
}
