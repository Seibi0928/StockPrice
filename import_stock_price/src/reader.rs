use std::net::{SocketAddr, TcpStream, ToSocketAddrs};

use ssh2::{Session, Sftp};

pub trait Reader {}

pub struct SFTPReader {
    sftp: Sftp,
}

impl SFTPReader {
    pub fn new(host: String, username: String, password: String) -> Result<Self, String> {
        let sftp = SFTPReader::get_addr(host)
            .and_then(|addr| SFTPReader::create_sftp_session(addr, &username, &password))?;
        Ok(Self { sftp })
    }

    fn get_addr(host: String) -> Result<SocketAddr, String> {
        let maybe_addr = match &format!(r#"{host}:22"#).to_socket_addrs() {
            Ok(res) => res.to_owned().next(),
            Err(err) => return Err(err.to_string()),
        };
        match maybe_addr {
            Some(addr) => Ok(addr),
            None => return Err("socket address is not found.".to_string()),
        }
    }

    fn create_sftp_session(
        addr: std::net::SocketAddr,
        username: &str,
        password: &str,
    ) -> Result<ssh2::Sftp, String> {
        let mut session = match Session::new() {
            Ok(res) => res,
            Err(err) => return Err(err.to_string()),
        };
        match TcpStream::connect(addr)
            .map(|tcp| session.set_tcp_stream(tcp))
            .map(|_| session.handshake())
            .map(|_| session.userauth_password(username, password))
        {
            Ok(_) => {}
            Err(err) => return Err(err.to_string()),
        }
        let sftp = session.sftp().unwrap();
        Ok(sftp)
    }
}

impl Reader for SFTPReader {}
