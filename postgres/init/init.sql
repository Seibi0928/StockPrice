set client_encoding = 'UTF8';

create table stock_prices (
  securities_code int not null,
  recorded_date date not null,
  close_price decimal null, -- 終値
  adjusted_close_price decimal null, -- 調整済み終値
  adjusted_close_price_including_ex_divided decimal null, -- 配当落ちが考慮された調整済み終値
  PRIMARY KEY (securities_code, recorded_date)
);