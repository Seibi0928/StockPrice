mod entities;
mod readers;
mod repositories;
mod usecases;
use anyhow::{bail, Context, Result};
use clap::{App, Arg};
use readers::SFTPCSVReader;
use repositories::PostgresRepository;
use ssh2::Session;
use std::{
    env,
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
};
use tracing::{error, info, warn, Level};
use usecases::import_stock_prices;

#[tokio::main]
async fn main() {
    init_logger();

    let arg_matches = set_console();
    let filename = if let Some(filename) = arg_matches.value_of("csv_file") {
        filename.to_owned()
    } else {
        warn!(message = "No file specified.");
        return;
    };

    info!("import task started.");
    match execute_import(filename).await {
        Ok(()) => info!("import task was succeeded."),
        Err(e) => {
            let traces = &*e
                .chain()
                .map(|e| e.to_string())
                .collect::<Vec<String>>()
                .join(",\n");
            error!(message = &*e, trace = traces);
            warn!("import task was failed.");
        }
    };
}

async fn execute_import(filename: String) -> Result<()> {
    let envs = get_env_settings();
    if envs.iter().any(|x| x.is_err()) {
        bail!(create_error_messages(envs));
    }
    let [sftp_host, sftp_username, sftp_password, base_dir, db_server, db_userid, db_name, db_port, db_password] =
        envs.map(|x| x.unwrap());
    let addr = get_addr(sftp_host)?;
    let sftp = create_sftp_session(addr, &sftp_username, &sftp_password)?;
    let mut file_reader = get_file_reader(sftp, &base_dir, &filename)?;
    let mut reader = SFTPCSVReader::new(&mut file_reader);
    let mut repository =
        PostgresRepository::new(db_server, db_port, db_name, db_userid, db_password).await?;
    import_stock_prices(&mut reader, &mut repository).await
}

fn init_logger() {
    tracing_subscriber::fmt()
        // filter spans/events with level TRACE or higher.
        .with_max_level(Level::INFO)
        .json()
        .flatten_event(true)
        // build but do not install the subscriber.
        .init();
}

fn set_console() -> clap::ArgMatches {
    App::new("Import stock price batch")
        .version("1.0.0")
        .author("Ryoya Osaki")
        .about("Import stock price csv file")
        .arg(
            Arg::new("csv_file")
                .long("csvfile")
                .short('f')
                .value_name("FILE")
                .required(false),
        )
        .get_matches()
}

fn create_error_messages(envs: [Result<String, String>; 9]) -> String {
    envs.iter()
        .filter_map(|x| x.as_ref().err().map(|x| x.to_owned()))
        .collect::<Vec<String>>()
        .join(",\n")
}

fn get_env_settings() -> [Result<String, String>; 9] {
    [
        "FILESTORAGE_HOST",
        "FILESTORAGE_USERID",
        "FILESTORAGE_PASSWORD",
        "FILESTORAGE_BASEDIR",
        "DB_SERVERNAME",
        "DB_USERID",
        "DB_NAME",
        "DB_PORT",
        "DB_PASSWORD",
    ]
    .map(|key| env::var(key).map_err(|err| format!("{err}({key})")))
}

fn get_addr(host: String) -> Result<SocketAddr> {
    format!(r#"{host}:22"#)
        .to_socket_addrs()
        .context("converting to scokert address is failed.")?
        .next()
        .context("socket address is not found.")
}

fn create_sftp_session(
    addr: std::net::SocketAddr,
    username: &str,
    password: &str,
) -> Result<ssh2::Sftp> {
    let mut session = Session::new().context("initializing session is failed.")?;
    _ = TcpStream::connect(addr)
        .map(|tcp| session.set_tcp_stream(tcp))
        .map(|_| session.handshake())
        .map(|_| session.userauth_password(username, password))
        .context("creating tcp session is failed.")?;
    session.sftp().context("initializing sftp is failed.")
}

fn get_file_reader(
    sftp: ssh2::Sftp,
    base_dir: &str,
    filename: &str,
) -> Result<csv::Reader<ssh2::File>> {
    sftp.open(Path::new(&format!("{base_dir}/{filename}")))
        .map(csv::Reader::from_reader)
        .context("creating csv reader is failed.")
}
