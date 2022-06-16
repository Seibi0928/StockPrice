mod entities;
mod readers;
mod repositories;
mod usecases;
use anyhow::{bail, Result};
use clap::{App, Arg};
use readers::SFTPCSVReader;
use repositories::PostgresRepository;
use std::env;
use usecases::import_stock_prices;

#[tokio::main]
async fn main() -> Result<()> {
    let arg_matches = set_console();
    let filename = if let Some(filename) = arg_matches.value_of("csv_file") {
        filename.to_owned()
    } else {
        println!("No file specified.");
        return Ok(());
    };
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
