use chrono::Utc;
use futures::pin_mut;
use rust_decimal::Decimal;
use ssh2::Session;
use std::{
    env::{self, VarError},
    net::{TcpStream, ToSocketAddrs},
    path::Path,
};
use tokio_postgres::binary_copy::BinaryCopyInWriter;
use tokio_postgres::{
    types::{Date, ToSql, Type},
    Client, Error as PostgreError, NoTls,
};

#[tokio::main]
async fn main() -> Result<(), String> {
    let envs = get_env_settings();

    if envs.iter().any(|x| x.is_err()) {
        return Err(create_error_messages(&envs));
    }

    let [host, username, password, base_dir, db_server, db_userid, db_name, db_port, db_password] =
        envs.map(|x| x.unwrap());

    let client =
        match connect_database(&db_server, &db_port, &db_name, &db_userid, &db_password).await {
            Ok(client) => client,
            Err(err) => return Err(err.to_string()),
        };

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
    for result in reader.records() {
        match result {
            Ok(record) => println!("{:?}", record),
            Err(err) => return Err(err.to_string()),
        }
    }

    Ok(())
}

fn create_sftp_session(
    addr: std::net::SocketAddr,
    username: &str,
    password: &str,
) -> Result<ssh2::Sftp, String> {
    let mut session = match Session::new() {
        Ok(res) => res,
        Err(err) => return Err(err.to_string()),
    };
    match TcpStream::connect(addr)
        .map(|tcp| session.set_tcp_stream(tcp))
        .map(|_| session.handshake())
        .map(|_| session.userauth_password(username, password))
    {
        Ok(_) => {}
        Err(err) => return Err(err.to_string()),
    }
    let sftp = session.sftp().unwrap();
    Ok(sftp)
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

pub struct StockPrice {
    pub securities_code: u32,
    pub recorded_date: Date<Utc>,
    pub close_price: Option<Decimal>,
    pub adjusted_close_price: Option<Decimal>,
    pub adjusted_close_price_including_ex_divided: Option<Decimal>,
}

async fn connect_database(
    server: &String,
    port: &String,
    database: &String,
    user_id: &String,
    password: &String,
) -> Result<Client, PostgreError> {
    let connection_str = &format!("Server={server};Port={port};Database={database};User Id={user_id};Password={password};Pooling=true;Minimum Pool Size=0;Maximum Pool Size=100");
    let (client, connection) = tokio_postgres::connect(connection_str, NoTls).await?;

    tokio::spawn(async move {
        if let Err(e) = connection.await {
            eprintln!("connection error: {}", e);
        }
    });
    Ok(client)
}

async fn bulk_insert(
    client: &mut Client,
    data: &Vec<StockPrice>,
) -> Result<(), tokio_postgres::error::Error> {
    let tx = client.transaction().await?;
    let sink = tx
        .copy_in(
            "
COPY stock_prices
(
    securities_code,
    recorded_date,
    close_price,
    adjusted_close_price,
    adjusted_close_price_including_ex_divided
) FROM STDIN BINARY
 ",
        )
        .await?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &vec![
            Type::INT4,
            Type::DATE,
            Type::NUMERIC,
            Type::NUMERIC,
            Type::NUMERIC,
        ],
    );
    write(writer, &data).await?;
    tx.commit().await?;
    Ok(())
}

async fn write(
    writer: BinaryCopyInWriter,
    data: &Vec<StockPrice>,
) -> Result<(), tokio_postgres::error::Error> {
    pin_mut!(writer);

    let mut row: Vec<&'_ (dyn ToSql + Sync)> = Vec::new();
    for d in data {
        row.clear();
        row.push(&d.securities_code);
        row.push(&d.recorded_date);
        row.push(&d.close_price);
        row.push(&d.adjusted_close_price);
        row.push(&d.adjusted_close_price_including_ex_divided);
        writer.as_mut().write(&row).await?;
    }

    writer.finish().await?;

    Ok(())
}
