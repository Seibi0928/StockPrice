use itertools::Itertools;

use crate::{readers::DataReader, repositories::Repository};
use anyhow::Result;

pub async fn import_stock_prices(
    reader: &mut impl DataReader,
    repository: &mut impl Repository,
) -> Result<()> {
    for chunked in reader.read().chunks(10000).into_iter() {
        repository.insert(chunked.collect_vec()).await?;
    }
    Ok(())
}
