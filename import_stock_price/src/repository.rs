use async_trait::async_trait;
use futures::pin_mut;
use tokio_postgres::{
    binary_copy::BinaryCopyInWriter,
    types::{ToSql, Type},
    Client, Error as PostgreError, NoTls,
};

use crate::entity::StockPrice;

#[async_trait]
pub trait Repository {
    async fn bulk_insert(
        &mut self,
        data: Vec<StockPrice>,
    ) -> Result<(), tokio_postgres::error::Error>;
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
    ) -> Self {
        let client = match PostgresRepository::connect_database(
            &db_server,
            &db_port,
            &db_name,
            &db_userid,
            &db_password,
        )
        .await
        {
            Ok(client) => client,
            Err(err) => panic!("{}", err.to_string()),
        };
        Self { client }
    }

    async fn connect_database(
        server: &String,
        port: &String,
        database: &String,
        user_id: &String,
        password: &String,
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
        data: &Vec<StockPrice>,
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
}

#[async_trait]
impl Repository for PostgresRepository {
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
        tx.commit().await?;
        Ok(())
    }
}
