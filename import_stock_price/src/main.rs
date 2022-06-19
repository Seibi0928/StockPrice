mod entities;
mod readers;
mod repositories;
mod usecases;
use anyhow::{bail, Result};
use clap::{App, Arg};
use readers::SFTPCSVReader;
use repositories::PostgresRepository;
use std::env;
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

    info!("import task was started.");
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

async fn execute_import(filename: String) -> Result<(), anyhow::Error> {
    let envs = get_env_settings();
    if envs.iter().any(|x| x.is_err()) {
        bail!(create_error_messages(envs));
    }
    let [sftp_host, sftp_username, sftp_password, base_dir, db_server, db_userid, db_name, db_port, db_password] =
        envs.map(|x| x.unwrap());
    let reader = SFTPCSVReader::new(sftp_host, sftp_username, sftp_password, base_dir, filename)?;
    let mut repository =
        PostgresRepository::new(db_server, db_port, db_name, db_userid, db_password).await?;
    import_stock_prices(&reader, &mut repository).await
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
