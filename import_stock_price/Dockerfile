FROM rust:1.60 AS builder

WORKDIR /import_stock_price
COPY ./import_stock_price/Cargo.toml Cargo.toml
RUN mkdir src
RUN echo "fn main(){}" > src/main.rs
RUN cargo build --release
COPY ./import_stock_price/src ./src
RUN rm -f target/release/deps/import_stock_price*
RUN cargo build --release

FROM debian:10.4

COPY --from=builder /import_stock_price/target/release/import_stock_price /usr/local/bin/import_stock_price
CMD ["import_stock_price"]