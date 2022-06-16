use anyhow::Context;

use crate::{readers::DataReader, repositories::Repository};
use anyhow::Result;

pub async fn import_stock_prices(
    reader: &impl DataReader,
    repository: &mut impl Repository,
) -> Result<()> {
    let stock_prices = reader.read().context("reading stock prices is failed.")?;
    repository.insert(stock_prices).await
}
