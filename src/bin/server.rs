use kvs::errors::{KvsError, KvsStorageEngine};
use kvs::kvsserver::Kvserver;
use kvs::kvsserver::*;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;
fn main() -> kvs::Result<()> {
    setup()?;
    let mut my_server = Kvserver::new("0.0.0.0", 48567, KvsStorageEngine::KvsEngine);
    my_server.run_server();
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
