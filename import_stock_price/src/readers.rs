use crate::entities::StockPrice;
use chrono::NaiveDate;
use rust_decimal::Decimal;
use ssh2::{Session, Sftp};
use std::{
    net::{SocketAddr, TcpStream, ToSocketAddrs},
    path::Path,
    str::FromStr,
};

pub trait DataReader {
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
            let record = result
                .map(SFTPCSVReader::read_stock_price)
                .map_err(|x| x.to_string())??;
            stock_prices.push(record);
        }
        Ok(stock_prices)
    }

    fn read_stock_price(record: csv::StringRecord) -> Result<StockPrice, String> {
        let securities_code = match record.get(2) {
            Some(x) => x.parse::<i32>(),
            None => return Err("A securities_code connot be retrieved.".to_owned()),
        }
        .map_err(|e| format!("securities_code cannot be parsed:{e}"))?;

        let recorded_date = match record.get(3) {
            Some(x) => NaiveDate::from_str(x),
            None => return Err("A recorded_date connot be retrieved.".to_owned()),
        }
        .map_err(|e| format!("recorded_date cannot be parsed.:{e}"))?;

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
    fn read(&self) -> Result<Vec<StockPrice>, String> {
        let reader = self.get_csv_reader(&self.base_dir, &self.filename)?;
        SFTPCSVReader::read_csv(reader)
    }
}
