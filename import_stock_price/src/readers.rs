use crate::entities::StockPrice;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use ssh2::{Session, Sftp};
use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    str::FromStr,
};

pub trait Reader {
    fn read(&self) -> Result<Vec<StockPrice>, String>;
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
    ) -> Result<Self, String> {
        let sftp = SFTPCSVReader::get_addr(host)
            .and_then(|addr| SFTPCSVReader::create_sftp_session(addr, &username, &password))?;
        Ok(Self {
            base_dir,
            filename,
            sftp,
        })
    }

    fn get_addr(host: String) -> Result<SocketAddr, String> {
        match format!(r#"{host}:22"#)
            .to_socket_addrs()
            .map_err(|e| e.to_string())?
            .next()
        {
            Some(addr) => Ok(addr),
            None => Err("socket address is not found.".to_owned()),
        }
    }

    fn create_sftp_session(
        addr: std::net::SocketAddr,
        username: &str,
        password: &str,
    ) -> Result<ssh2::Sftp, String> {
        let mut session = Session::new().map_err(|e| e.to_string())?;
        _ = TcpStream::connect(addr)
            .map(|tcp| session.set_tcp_stream(tcp))
            .map(|_| session.handshake())
            .map(|_| session.userauth_password(username, password))
            .map_err(|e| e.to_string())?;
        session.sftp().map_err(|e| e.to_string())
    }

    fn get_csv_reader(
        &self,
        base_dir: &str,
        filename: &str,
    ) -> Result<csv::Reader<ssh2::File>, String> {
        self.sftp
            .open(Path::new(&format!("{base_dir}/{filename}")))
            .map(csv::Reader::from_reader)
            .map_err(|e| e.to_string())
    }

    fn read_csv(mut reader: csv::Reader<ssh2::File>) -> Result<Vec<StockPrice>, String> {
        let mut stock_prices = Vec::new();
        for result in reader.records().take(10) {
            match result {
                Ok(record) => stock_prices.push(StockPrice {
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
        Ok(stock_prices)
    }
}

impl Reader for SFTPCSVReader {
    fn read(&self) -> Result<Vec<StockPrice>, String> {
        let reader = self.get_csv_reader(&self.base_dir, &self.filename)?;
        SFTPCSVReader::read_csv(reader)
    }
}
