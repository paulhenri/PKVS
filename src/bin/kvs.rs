extern crate clap;
use clap::{App, Arg, SubCommand};
use kvs::kvsengine::kvstore::KvStore;
use kvs::kvsengine::*;
use std::env;
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
        // println!("kvs - KeyValue Store manager vi.1");
        println!("0.1.0");
        process::exit(4);
    }

    if m.is_present("set") {
        debug!("Set command has beed issued");
        if let Some(subcommand) = m.subcommand_matches("set") {
            if subcommand.is_present("key") && subcommand.is_present("value") {
                debug!("Value and Key have been provided.");
                let mut my_store: KvStore = KvStore::open(std::env::current_dir()?);
                my_store.set(
                    subcommand.value_of("key").unwrap().to_string(),
                    subcommand.value_of("value").unwrap().to_string(),
                );
                match my_store.sync_index() {
                    Ok(_) => {
                        process::exit(3);
                    }
                    Err(x) => {
                        debug!("Error when syncing indexes : {:?}", x);
                        process::exit(-1)
                    }
                }
            }
        }
    }

    if m.is_present("get") {
        if let Some(subcommand) = m.subcommand_matches("get") {
            let mut my_store: KvStore = KvStore::open(std::env::current_dir()?);
            match my_store.get(subcommand.value_of("key").unwrap().to_string()) {
                Ok(x) => {
                    if let Some(z) = x {
                        println!("{}", z);
                    }
                }
                Err(x) => {
                    debug!("Error when getting the value: {:?}", x);

                    println!("Key not found")
                }
            }
        }
        process::exit(3);
    }

    panic!();
    process::exit(3)
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
