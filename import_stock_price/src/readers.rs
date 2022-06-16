use crate::entities::StockPrice;
use anyhow::{Context, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use ssh2::{Session, Sftp};
use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    str::FromStr,
};

pub trait DataReader {
    fn read(&self) -> Result<Vec<StockPrice>>;
}

pub struct SFTPCSVReader {
    base_dir: String,
    filename: String,
    sftp: Sftp,
}

impl SFTPCSVReader {
    pub fn new(
        host: String,
        username: String,
        password: String,
        base_dir: String,
        filename: String,
    ) -> Result<Self> {
        let addr = SFTPCSVReader::get_addr(host)?;
        let sftp = SFTPCSVReader::create_sftp_session(addr, &username, &password)?;
        Ok(Self {
            base_dir,
            filename,
            sftp,
        })
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

    fn get_csv_reader(&self, base_dir: &str, filename: &str) -> Result<csv::Reader<ssh2::File>> {
        self.sftp
            .open(Path::new(&format!("{base_dir}/{filename}")))
            .map(csv::Reader::from_reader)
            .context("creating csv reader is failed.")
    }

    fn read_csv(mut reader: csv::Reader<ssh2::File>) -> Result<Vec<StockPrice>> {
        let mut stock_prices = Vec::new();
        for result in reader.records() {
            let record = result
                .map(SFTPCSVReader::read_stock_price)
                .context("reading record is failed.")??;
            stock_prices.push(record);
        }
        Ok(stock_prices)
    }

    fn read_stock_price(record: csv::StringRecord) -> Result<StockPrice> {
        let securities_code = record
            .get(2)
            .context("A securities_code connot be retrieved.")?
            .parse::<i32>()
            .context("securities_code cannot be parsed")?;

        let recorded_date = NaiveDate::from_str(
            record
                .get(3)
                .context("A recorded_date connot be retrieved.")?,
        )
        .context("recorded_date cannot be parsed.")?;

        let close_price = record.get(4).and_then(|x| x.parse::<Decimal>().ok());

        let adjusted_close_price = record.get(5).and_then(|x| x.parse::<Decimal>().ok());

        let adjusted_close_price_including_ex_divided =
            record.get(6).and_then(|x| x.parse::<Decimal>().ok());

        Ok(StockPrice {
            securities_code,
            recorded_date,
            close_price,
            adjusted_close_price,
            adjusted_close_price_including_ex_divided,
        })
    }
}

impl DataReader for SFTPCSVReader {
    fn read(&self) -> Result<Vec<StockPrice>> {
        let reader = self.get_csv_reader(&self.base_dir, &self.filename)?;
        SFTPCSVReader::read_csv(reader)
    }
}
