mod entity;
mod reader;
mod repository;
use chrono::NaiveDate;
use entity::StockPrice;
use repository::{PostgresRepository, Repository};
use rust_decimal::Decimal;
use ssh2::Session;
use std::{
    env::{self, VarError},
    net::{TcpStream, ToSocketAddrs},
    path::Path,
    str::FromStr,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let envs = get_env_settings();

    if envs.iter().any(|x| x.is_err()) {
        return Err(create_error_messages(&envs));
    }

    let [host, username, password, base_dir, db_server, db_userid, db_name, db_port, db_password] =
        envs.map(|x| x.unwrap());

    let maybe_addr = match &format!(r#"{host}:22"#).to_socket_addrs() {
        Ok(res) => res.to_owned().next(),
        Err(err) => return Err(err.to_string()),
    };
    let addr = match maybe_addr {
        Some(addr) => addr,
        None => return Err("socket address is not found.".to_string()),
    };

    let sftp = match create_sftp_session(addr, &username, &password) {
        Ok(res) => res,
        Err(err) => return Err(err),
    };
    let maybe_reader = sftp
        .open(Path::new(&format!("{base_dir}/PriceExp_2000_2020.csv")))
        .map(csv::Reader::from_reader);
    let mut reader = match maybe_reader {
        Ok(res) => res,
        Err(err) => return Err(err.to_string()),
    };
    let mut vec = Vec::new();
    for result in reader.records().take(10) {
        match result {
            Ok(record) => vec.push(StockPrice {
                securities_code: record.get(2).unwrap().parse::<i32>().unwrap(),
                recorded_date: record
                    .get(3)
                    .map(|x| NaiveDate::from_str(x).unwrap())
                    .unwrap(),
                close_price: record.get(4).map(|x| x.parse::<Decimal>().unwrap()),
                adjusted_close_price: record.get(5).map(|x| x.parse::<Decimal>().unwrap()),
                adjusted_close_price_including_ex_divided: record
                    .get(6)
                    .map(|x| x.parse::<Decimal>().unwrap()),
            }),
            Err(err) => return Err(err.to_string()),
        }
    }

    let mut repository =
        PostgresRepository::new(db_server, db_port, db_name, db_userid, db_password).await;
    if let Err(err) = repository.bulk_insert(vec).await {
        return Err(err.to_string());
    }
    Ok(())
}

fn create_error_messages(envs: &[Result<String, (VarError, String)>; 9]) -> String {
    envs.iter()
        .filter(|x| x.is_err())
        .map(|x| {
            let (err, key) = x.as_ref().unwrap_err();
            err.to_string() + "(" + key + ")"
        })
        .collect::<Vec<String>>()
        .join(",\n")
}

fn get_env_settings() -> [Result<String, (VarError, String)>; 9] {
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
    .map(|key| env::var(key).map_err(|err| (err, key.to_owned())))
}
