extern crate clap;
use clap::{App, Arg, SubCommand};
use kvs::kvmessage::KvMessage;
use std::io::prelude::*;
use std::net::TcpListener;
use std::net::TcpStream;
use std::process;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

fn main() -> kvs::Result<()> {
    setup()?;

    let m = App::new(env!("CARGO_PKG_NAME"))
        .subcommand(
            SubCommand::with_name("set")
                .about("set value")
                .arg(Arg::with_name("key").takes_value(true).index(1))
                .arg(Arg::with_name("value").takes_value(true).index(2)),
        )
        .subcommand(
            SubCommand::with_name("get")
                .about("Get value")
                .help("kvs get <key> -- Get the value of the key in parameter")
                .arg(Arg::with_name("key").takes_value(true).index(1)),
        )
        .subcommand(
            SubCommand::with_name("rm")
                .about("Remove value")
                .help("kvs rm <key> -- Delete the key/value ")
                .arg(Arg::with_name("key").takes_value(true).index(1)),
        )
        .arg(Arg::with_name("version").short("V").long("V"))
        .arg(Arg::with_name("open").short("o").long("o"))
        .arg(Arg::with_name("compaction").short("c").long("c"))
        .get_matches();

    if m.is_present("version") {
        println!("KvsNetwork Client v0.1");
        process::exit(4);
    }

    if m.is_present("set") {
        debug!("Set command has beed issued");
        if let Some(subcommand) = m.subcommand_matches("set") {
            if subcommand.is_present("key") && subcommand.is_present("value") {
                let message = KvMessage::Set(
                    subcommand.value_of("key").unwrap().to_string(),
                    subcommand.value_of("value").unwrap().to_string(),
                );

                // Now is the time to initiate the connexion to the server
                // The adress of the server and the port should be in config file
                // We can use .XXXX_connecion.config to create different endpoints
                // Just like an ODBC entry on Windows
                if let Ok(mut stream) = TcpStream::connect("127.0.0.1:48567") {
                    debug!("Connected to the server");
                    let bincode = bincode::serialize(&message);
                    if let Ok(vect) = bincode {
                        match stream.write_all(vect.as_slice()) {
                            Ok(_) => debug!("The slice has been written to the socket"),
                            Err(z) => info!("Could not write to the stocket {:?}", z),
                        }
                        //We wait for the response
                        let mut buff_reponse = [0u8; 128];
                        match stream.read(&mut buff_reponse) {
                            Ok(x) => {
                                if x > 0 {
                                    let reponse: KvMessage =
                                        bincode::deserialize(&buff_reponse).unwrap();
                                    if let KvMessage::Response(z) = reponse {
                                        println!("{}", z);
                                        process::exit(-1);
                                    }
                                }
                            }
                            Err(err) => {
                                debug!("No response was received after the command : {:?}", err);
                                panic!("Error occurred when retrieving the response.");
                            }
                        }
                    } else {
                        println!("Serialisation did not went wel...");
                        process::exit(1);
                    }
                //                    stream.flush().unwrap();
                } else {
                    info!("Could not connect to server...");
                    process::exit(1);
                }
            }
        }
    }

    if m.is_present("get") {
        if let Some(subcommand) = m.subcommand_matches("get") {}
    }

    Ok(())
}

fn setup() -> kvs::Result<()> {
    if std::env::var("RUST_LIB_BACKTRACE").is_err() {
        std::env::set_var("RUST_LIB_BACKTRACE", "1")
    }
    color_eyre::install()?;

    if std::env::var("RUST_LOG").is_err() {
        std::env::set_var("RUST_LOG", "debug")
    }
    tracing_subscriber::fmt::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .init();

    Ok(())
}
