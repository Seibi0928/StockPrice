use crate::entities::StockPrice;
use anyhow::{Context, Result};
use chrono::NaiveDate;
use rust_decimal::Decimal;
use std::str::FromStr;

pub trait DataReader {
    fn read<'a>(&'a mut self) -> Box<dyn Iterator<Item = Result<StockPrice>> + 'a>;
}

pub struct SFTPCSVReader<'a> {
    reader: &'a mut csv::Reader<ssh2::File>,
}

impl<'a> SFTPCSVReader<'a> {
    pub fn new(reader: &'a mut csv::Reader<ssh2::File>) -> Self {
        Self { reader }
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

impl DataReader for SFTPCSVReader<'_> {
    fn read<'a>(&'a mut self) -> Box<dyn Iterator<Item = Result<StockPrice>> + 'a> {
        Box::new(
            self.reader
                .records()
                .map(|result| SFTPCSVReader::read_stock_price(result?)),
        )
    }
}
