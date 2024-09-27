use crate::{error::Result, proto::GenMessage, Error};
use std::{collections::HashMap, io::ErrorKind, ops::DerefMut, sync::Arc};
use tokio::{
    fs::File,
    io::{AsyncSeekExt, AsyncWriteExt},
    sync::Mutex,
};

const SOCK_NAME_LEN: usize = 36;

#[derive(Clone)]
pub struct MessageStore {
    file: Arc<Mutex<File>>,
    cache: Arc<Mutex<HashMap<String, Vec<SockMessage>>>>,
}

#[derive(Default, Debug, Clone, PartialEq, Eq)]
pub struct SockMessage {
    pub(crate) sock_name: String,
    pub(crate) id: u64,
    pub(crate) message: GenMessage,
}

impl SockMessage {
    pub async fn write_to(&self, mut writer: impl tokio::io::AsyncWriteExt + Unpin) -> Result<()> {
        writer
            .write_all(self.sock_name.as_bytes())
            .await
            .map_err(|e| Error::Others(e.to_string()))?;
        writer
            .write_u64(self.id)
            .await
            .map_err(|e| Error::Others(e.to_string()))?;
        self.message.write_to(writer).await?;
        Ok(())
    }

    pub async fn read_from(mut reader: impl tokio::io::AsyncReadExt + Unpin) -> Result<Self> {
        let mut sock_name_buf = vec![0u8; SOCK_NAME_LEN];
        let len = reader.read_exact(&mut sock_name_buf).await.map_err(|e| {
            if e.kind() == ErrorKind::UnexpectedEof {
                Error::Eof
            } else {
                Error::Others(format!("failed to read messages from memfd {}", e))
            }
        })?;
        if len < SOCK_NAME_LEN {
            return Err(Error::Others(format!("read {} bytes for socket name", len)));
        }
        let sock_name =
            String::from_utf8(sock_name_buf).map_err(|e| Error::Others(e.to_string()))?;
        let id = reader
            .read_u64()
            .await
            .map_err(|e| Error::Others(e.to_string()))?;
        let message = GenMessage::read_from(reader).await?;
        Ok(Self {
            sock_name,
            id,
            message,
        })
    }
}

impl MessageStore {
    pub async fn load(mut f: File) -> Result<Self> {
        f.seek(std::io::SeekFrom::Start(0))
            .await
            .map_err(|e| Error::Others(e.to_string()))?;
        let s = Self {
            file: Arc::new(Mutex::new(f)),
            cache: Arc::new(Mutex::new(HashMap::new())),
        };
        let file = s.file.clone();
        let mut f = file.lock().await;
        loop {
            let a = SockMessage::read_from(f.deref_mut()).await;
            match a {
                Ok(m) => {
                    trace!("load a message from {}, with id {}", m.sock_name, m.id);
                    s.insert_sock_message(m).await;
                }
                Err(e) => match e {
                    Error::Eof => {
                        break;
                    }
                    _ => {
                        return Err(Error::Others(format!(
                            "failed to read message from memfd in fdstore: {}",
                            e
                        )));
                    }
                },
            }
        }
        return Ok(s);
    }

    pub async fn insert(&self, sock_name: String, id: u64, m: GenMessage) {
        assert_eq!(SOCK_NAME_LEN, sock_name.len());
        self.insert_sock_message(SockMessage {
            sock_name,
            id,
            message: m,
        })
        .await;
        self.dump().await;
    }

    pub async fn dump(&self) {
        let mut file = self.file.lock().await;
        file.set_len(0).await.unwrap_or_default();
        file.rewind().await.unwrap_or_default();
        let cache = self.cache.lock().await;
        for v in cache.values() {
            for m in v {
                m.write_to(file.deref_mut()).await.unwrap_or_default();
            }
        }
        file.flush().await.unwrap_or_default();
    }

    pub async fn get_messages(&self, key: &str) -> Vec<SockMessage> {
        let mut res = vec![];
        let cache = self.cache.lock().await;
        if let Some(l) = cache.get(key) {
            for m in l {
                res.push(m.clone());
            }
        }
        res
    }

    pub async fn remove(&self, sock_name: String, id: u64) {
        self.remove_sock_message(sock_name, id).await;
        self.dump().await;
    }

    async fn remove_sock_message(&self, sock_name: String, id: u64) {
        let mut cache = self.cache.lock().await;
        if let Some(v) = cache.get_mut(&sock_name) {
            v.retain(|x| x.id != id);
        }
    }

    async fn insert_sock_message(&self, m: SockMessage) {
        let mut cache = self.cache.lock().await;
        let sock_name = m.sock_name.clone();
        if let Some(v) = cache.get_mut(&sock_name) {
            v.push(m);
        } else {
            let mut l = Vec::new();
            l.push(m);
            cache.insert(sock_name, l);
        }
    }
}
