use std::{
    env,
    net::{TcpStream, ToSocketAddrs},
    path::Path,
};

use ssh2::Session;

// extern crate ssh2;

fn main() {
    let host = env::var("FILESTORAGE_HOST").unwrap();
    let username = env::var("FILESTORAGE_USERID").unwrap();
    let password = env::var("FILESTORAGE_PASSWORD").unwrap();
    let base_dir = env::var("FILESTORAGE_BASEDIR").unwrap();

    let address = &format!(r#"{host}:22"#)
        .to_socket_addrs()
        .unwrap()
        .next()
        .unwrap();
    let tcp = TcpStream::connect(address).unwrap();
    let mut session = Session::new().unwrap();
    session.set_tcp_stream(tcp);
    session.handshake().unwrap();
    session.userauth_password(&username, &password).unwrap();

    let sftp = session.sftp().unwrap();
    println!("Hello, {base_dir}!");
    let file_path = &format!("{base_dir}/PriceExp_2000_2020.csv");
    let file = sftp.open(Path::new(file_path)).unwrap();

    let mut rdr = csv::Reader::from_reader(file);
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result.unwrap();
        println!("{:?}", record);
    }
    println!("Hello, world!");
}
