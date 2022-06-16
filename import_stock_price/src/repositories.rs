use async_trait::async_trait;
use futures::pin_mut;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{ToSql, Type},
    Client, Error as PostgreError, NoTls,
};

use crate::entities::StockPrice;

#[async_trait]
pub trait Repository {
    async fn insert(&mut self, data: Vec<StockPrice>) -> Result<(), String>;
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
    ) -> Result<Self, String> {
        let client = PostgresRepository::connect_database(
            &db_server,
            &db_port,
            &db_name,
            &db_userid,
            &db_password,
        )
        .await
        .map_err(|e| e.to_string())?;

        Ok(Self { client })
    }

    async fn connect_database(
        server: &str,
        port: &str,
        database: &str,
        user_id: &str,
        password: &str,
    ) -> Result<Client, PostgreError> {
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

    async fn write(
        writer: BinaryCopyInWriter,
        data: &[StockPrice],
    ) -> Result<(), tokio_postgres::error::Error> {
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

    async fn bulk_insert(
        &mut self,
        data: Vec<StockPrice>,
    ) -> Result<(), tokio_postgres::error::Error> {
        let tx = self.client.transaction().await?;
        let sink = tx
            .copy_in(
                "
        COPY stock_prices
        (
            securities_code,
            recorded_date,
            close_price,
            adjusted_close_price,
            adjusted_close_price_including_ex_divided
        ) FROM STDIN BINARY
        ",
            )
            .await?;
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
        tx.commit().await
    }
}

#[async_trait]
impl Repository for PostgresRepository {
    async fn insert(&mut self, data: Vec<StockPrice>) -> Result<(), String> {
        self.bulk_insert(data).await.map_err(|e| e.to_string())
    }
}
