use chrono::NaiveDate;
use rust_decimal::Decimal;

pub struct StockPrice {
    pub securities_code: i32,
    pub recorded_date: NaiveDate,
    pub close_price: Option<Decimal>,
    pub adjusted_close_price: Option<Decimal>,
    pub adjusted_close_price_including_ex_divided: Option<Decimal>,
}
