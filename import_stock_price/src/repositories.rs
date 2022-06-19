use crate::entities::StockPrice;
use anyhow::{Context, Result};
use async_trait::async_trait;
use futures::pin_mut;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{ToSql, Type},
    Client, NoTls,
};
use uuid::Uuid;

#[async_trait]
pub trait Repository {
    async fn insert(&mut self, data: Vec<StockPrice>) -> Result<()>;
}

pub struct PostgresRepository {
    client: Client,
}

impl PostgresRepository {
    pub async fn new(
        db_server: String,
        db_port: String,
        db_name: String,
        db_userid: String,
        db_password: String,
    ) -> Result<Self> {
        let client = PostgresRepository::connect_database(
            &db_server,
            &db_port,
            &db_name,
            &db_userid,
            &db_password,
        )
        .await
        .context("creating sql client is failed.")?;

        Ok(Self { client })
    }

    async fn connect_database(
        server: &str,
        port: &str,
        database: &str,
        user_id: &str,
        password: &str,
    ) -> Result<Client> {
        let connection_str =
            &format!("postgresql://{user_id}:{password}@{server}:{port}/{database}");
        let (client, connection) = tokio_postgres::connect(connection_str, NoTls).await?;

        tokio::spawn(async move {
            if let Err(e) = connection.await {
                eprintln!("connection error: {}", e);
            }
        });
        Ok(client)
    }

    async fn write(writer: BinaryCopyInWriter, data: &[StockPrice]) -> Result<()> {
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

    async fn bulk_insert(&mut self, data: Vec<StockPrice>) -> Result<()> {
        let tx = self.client.transaction().await?;
        // 重複したデータの挿入でエラーにならないように
        // あらかじめ全データを一時テーブルへ格納し、
        // 新規データのみを実テーブルに移動させる
        let temp_table = create_temp_table(&tx).await?;
        bulk_copy_to_temp_table(&temp_table, &tx, data).await?;
        transfer_data_to_actual_table(&temp_table, &tx).await?;

        tx.commit()
            .await
            .context("committing transaction is failed.")
    }
}

async fn bulk_copy_to_temp_table(
    temp_table: &str,
    tx: &tokio_postgres::Transaction<'_>,
    data: Vec<StockPrice>,
) -> Result<(), anyhow::Error> {
    let sink = tx
        .copy_in(&*format!(
            "
        COPY {temp_table}
        (
            securities_code,
            recorded_date,
            close_price,
            adjusted_close_price,
            adjusted_close_price_including_ex_divided
        ) FROM STDIN BINARY;
        "
        ))
        .await
        .context("bulk copy is failed.")?;
    let writer = BinaryCopyInWriter::new(
        sink,
        &[
            Type::INT4,
            Type::DATE,
            Type::NUMERIC,
            Type::NUMERIC,
            Type::NUMERIC,
        ],
    );
    PostgresRepository::write(writer, &data).await?;
    Ok(())
}

async fn transfer_data_to_actual_table<'a>(
    temp_table: &str,
    tx: &'a tokio_postgres::Transaction<'_>,
) -> Result<()> {
    tx.query(
        &*format!(
            "
            INSERT INTO
                stock_prices
            SELECT
                TMP.securities_code,
                TMP.recorded_date,
                TMP.close_price,
                TMP.adjusted_close_price,
                TMP.adjusted_close_price_including_ex_divided
            FROM
                {temp_table} TMP
            LEFT OUTER JOIN
                stock_prices SP
            ON
                TMP.securities_code = SP.securities_code
            AND
                TMP.recorded_date = SP.recorded_date
            WHERE
                SP.securities_code IS NULL
            AND 
                SP.recorded_date IS NULL;"
        ),
        &[],
    )
    .await
    .context("executing insert into query is failed.")?;
    Ok(())
}

async fn create_temp_table(tx: &tokio_postgres::Transaction<'_>) -> Result<String> {
    let guid = Uuid::new_v4().simple().to_string();
    let temp_table = format!("temp_stock_prices_{guid}");
    tx.query(
        &*format!(
            "
        CREATE TEMP TABLE {temp_table}
        (
            securities_code int not null,
            recorded_date date not null,
            close_price decimal null,
            adjusted_close_price decimal null,
            adjusted_close_price_including_ex_divided decimal null,
            PRIMARY KEY (securities_code, recorded_date)
        );"
        ),
        &[],
    )
    .await
    .context("creating temp table is failed.")?;
    Ok(temp_table)
}

#[async_trait]
impl Repository for PostgresRepository {
    async fn insert(&mut self, data: Vec<StockPrice>) -> Result<()> {
        self.bulk_insert(data)
            .await
            .context("bulk insert is failed.")
    }
}
