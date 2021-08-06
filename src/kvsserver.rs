use crate::errors::*;
use crate::kvmessage::KvMessage;
use crate::kvsengine::kvstore::KvStore;
use crate::kvsengine::kvstore::*;
use crate::kvsengine::KvsEngine;
use message_io::events::{EventReceiver, EventSender};
use message_io::network::{Endpoint, NetEvent, Transport};
use message_io::node::{self, NodeEvent, NodeHandler, NodeListener};
use serde::{Deserialize, Serialize};
use std::net::Ipv4Addr;
use std::net::{SocketAddr, SocketAddrV4};
use tracing::{debug, info};

pub struct Kvserver {
    local_socketadr: SocketAddr,
    storage_engine: KvsStorageEngine,
}

impl Kvserver {
    /// Initializer of the server struc
    pub fn new(local_addr: &str, local_port: u16, storage_engine: KvsStorageEngine) -> Kvserver {
        let ipvadr = local_addr.parse::<Ipv4Addr>();
        match ipvadr {
            Ok(addr) => {
                return Kvserver {
                    local_socketadr: SocketAddr::V4(SocketAddrV4::new(addr, local_port)),
                    storage_engine,
                };
            }
            Err(x) => {
                debug!("Following error occurred : {:?}", x);
                panic!("Connexion could not initiate on the specified error");
            }
        }
    }

    pub fn run_server(&mut self) -> Result<()> {
        //First, intiate the store - This can take some time if indexes need to be rebuilt
        let mut my_store: KvStore = KvStore::open(std::env::current_dir()?);

        //Finaly, we can connect and start to wait for events
        let (handler, listener) = node::split::<()>();
        handler
            .network()
            .listen(Transport::Tcp, self.local_socketadr)
            .unwrap();
        println!("Listening to connexions...");
        listener.for_each(move |event| match event.network() {
            NetEvent::Connected(_, _) => (),
            NetEvent::Disconnected(endpoint) => {
                info!("{} just disconnected", endpoint.addr());
            }
            NetEvent::Accepted(_endpoint, _listener) => {
                info!("New connexion from {}", _endpoint.addr())
            }
            NetEvent::Message(endpoint, input_data) => {
                info!("New command from {}", endpoint.addr());

                // This safety may not be necessary but we have exeperienced 0 bytes long frames...
                // in FramedTcp mode.
                // Cost of this should not weight in for now regarding other optimisation than can
                // be done
                if input_data.len() > 0 {
                    let message: KvMessage = bincode::deserialize(&input_data).unwrap();
                    let mut _reponse = vec![];
                    match message {
                        KvMessage::Response(_) => {
                            println!("Response received");
                        }
                        KvMessage::Get(key) => {
                            match my_store.get(key) {
                                Ok(Some(value)) => {
                                    _reponse =
                                        bincode::serialize(&KvMessage::Response(value)).unwrap();
                                }
                                Ok(None) => {
                                    _reponse = bincode::serialize(&KvMessage::Response(
                                        "Key not found".to_string(),
                                    ))
                                    .unwrap()
                                }
                                Err(z) => {
                                    _reponse =
                                        bincode::serialize(&KvMessage::Response(format!("{:?}", z)))
                                            .unwrap()
                                }
                            }
                            handler.network().send(endpoint, &_reponse);
                        }
                        KvMessage::Set(key, value) => {
                            debug!("Set command was issued - Trying to process");
                            match my_store.set(key, value) {
                                Ok(_) => {
                                    debug!("Set command done successfuly");
                                    _reponse =
                                        bincode::serialize(&KvMessage::Response("ok".to_string()))
                                            .unwrap()
                                }
                                Err(err) => {
                                    debug!("Error occured during the set command");
                                    _reponse = bincode::serialize(&KvMessage::Response(format!(
                                        "{:?}",
                                        err
                                    )))
                                    .unwrap()
                                }
                            }
                            handler.network().send(endpoint, &_reponse);
                        }
                        KvMessage::Remove(key) => {
                            match my_store.remove(key) {
                                Ok(_) => {
                                    _reponse =
                                        bincode::serialize(&KvMessage::Response("ok".to_string()))
                                            .unwrap();
                                }
                                Err(err) => {
                                    _reponse = bincode::serialize(&KvMessage::Response(format!(
                                        "{:?}",
                                        err
                                    )))
                                    .unwrap()
                                }
                            }
                            handler.network().send(endpoint, &_reponse);
                        }
                        _ => println!("No know pattern was found"),
                    }
                }
            }
        });

        Ok(())
    }
}
