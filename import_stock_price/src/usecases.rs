use crate::{readers::DataReader, repositories::Repository};
use anyhow::Result;
use itertools::Itertools;
use tracing::warn;

pub async fn import_stock_prices(
    reader: &mut impl DataReader,
    repository: &mut impl Repository,
) -> Result<()> {
    for chunked in reader
        .read()
        .filter_map(|result| match result {
            Ok(r) => Some(r),
            Err(e) => {
                warn!(message = "invalid record.", trace = &*e);
                None
            }
        })
        .chunks(5000)
        .into_iter()
    {
        repository.insert(chunked.collect_vec()).await?;
    }
    Ok(())
}
