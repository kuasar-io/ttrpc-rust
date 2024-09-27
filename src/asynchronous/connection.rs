// Copyright 2022 Alibaba Cloud. All rights reserved.
// Copyright (c) 2020 Ant Financial
//
// SPDX-License-Identifier: Apache-2.0
//

use std::os::unix::io::AsRawFd;

use async_trait::async_trait;
use log::{error, trace};
use tokio::{
    io::{split, AsyncRead, AsyncWrite, ReadHalf},
    select, task,
};

use crate::error::Error;
use crate::proto::GenMessage;

use super::stream::SendingMessage;
#[cfg(feature = "fdstore")]
use crate::r#async::fdstore::MessageStore;

pub trait Builder {
    type Reader;
    type Writer;

    fn build(&mut self) -> (Self::Reader, Self::Writer);
}

#[async_trait]
pub trait WriterDelegate {
    async fn recv(&mut self) -> Option<SendingMessage>;
    async fn disconnect(&self, msg: &GenMessage, e: Error);
    async fn exit(&self);
}

#[async_trait]
pub trait ReaderDelegate {
    async fn wait_shutdown(&self);
    async fn disconnect(&self, e: Error, task: &mut task::JoinHandle<()>);
    async fn exit(&self);
    // handle message with id, the id is only for message store.
    async fn handle_msg(&self, id: u64, msg: GenMessage);
}

pub struct Connection<S, B: Builder> {
    #[cfg(feature = "fdstore")]
    name: String,
    reader: ReadHalf<S>,
    writer_task: task::JoinHandle<()>,
    reader_delegate: B::Reader,
}

impl<S, B> Connection<S, B>
where
    S: AsyncRead + AsyncWrite + AsRawFd + Send + 'static,
    B: Builder,
    B::Reader: ReaderDelegate + Send + Sync + 'static,
    B::Writer: WriterDelegate + Send + Sync + 'static,
{
    pub fn new(conn: S, mut builder: B, #[cfg(feature = "fdstore")] name: String) -> Self {
        let (reader, mut writer) = split(conn);

        let (reader_delegate, mut writer_delegate) = builder.build();

        let writer_task = tokio::spawn(async move {
            while let Some(mut sending_msg) = writer_delegate.recv().await {
                trace!("write message: {:?}", sending_msg.msg);
                if let Err(e) = sending_msg.msg.write_to(&mut writer).await {
                    error!("write_message got error: {:?}", e);
                    sending_msg.send_result(Err(e.clone()));
                    writer_delegate.disconnect(&sending_msg.msg, e).await;
                }
                sending_msg.send_result(Ok(()));
            }
            writer_delegate.exit().await;
            trace!("Writer task exit.");
        });

        Self {
            #[cfg(feature = "fdstore")]
            name,
            reader,
            writer_task,
            reader_delegate,
        }
    }

    pub async fn run(self) -> std::io::Result<()> {
        let Connection {
            #[cfg(feature = "fdstore")]
                name: _,
            mut reader,
            mut writer_task,
            reader_delegate,
        } = self;
        loop {
            select! {
                res = GenMessage::read_from(&mut reader) => {
                    match res {
                        Ok(msg) => {
                            trace!("Got Message {:?}", msg);
                            reader_delegate.handle_msg(0, msg).await;
                        }
                        Err(e) => {
                            trace!("Read msg err: {:?}", e);
                            reader_delegate.disconnect(e, &mut writer_task).await;
                            break;
                        }
                    }
                }
                _v = reader_delegate.wait_shutdown() => {
                    trace!("Receive shutdown.");
                    break;
                }
            }
        }
        reader_delegate.exit().await;
        trace!("Reader task exit.");

        Ok(())
    }

    #[cfg(feature = "fdstore")]
    pub async fn run_with_message_store(self, message_store: MessageStore) -> std::io::Result<()> {
        let Connection {
            name,
            mut reader,
            mut writer_task,
            reader_delegate,
        } = self;

        let messages = message_store.get_messages(&name).await;
        // the next message id should be larger than the message id before restart.
        let mut id = 0u64;
        for m in messages {
            if m.id >= id {
                id = m.id + 1;
            }
            // handle the stored request
            reader_delegate.handle_msg(m.id, m.message).await;
        }
        loop {
            select! {
                res = GenMessage::read_from(&mut reader) => {
                    match res {
                        Ok(msg) => {
                            trace!("Got Message {:?}", msg);
                            message_store.insert(name.clone(), id, msg.clone()).await;
                            reader_delegate.handle_msg(id, msg).await;
                            id += 1;
                        }
                        Err(e) => {
                            trace!("Read msg err: {:?}", e);
                            reader_delegate.disconnect(e, &mut writer_task).await;
                            break;
                        }
                    }
                }
                _v = reader_delegate.wait_shutdown() => {
                    trace!("Receive shutdown.");
                    break;
                }
            }
        }
        reader_delegate.exit().await;
        #[cfg(feature = "fdstore")]
        if let Err(e) = libsystemd::daemon::notify_with_fds(
            false,
            &[
                libsystemd::daemon::NotifyState::Fdname(name.to_string()),
                libsystemd::daemon::NotifyState::FdstoreRemove,
            ],
            &[],
        ) {
            warn!("failed to notify systemd to remove the fd {}: {}", name, e);
        }
        trace!("Reader task exit.");

        Ok(())
    }
}
