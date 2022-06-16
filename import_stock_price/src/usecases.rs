use crate::{readers::Reader, repositories::Repository};

pub async fn import_stock_prices(
    reader: &impl Reader,
    repository: &mut impl Repository,
) -> Result<(), String> {
    let stock_prices = reader.read()?;
    repository.insert(stock_prices).await
}
