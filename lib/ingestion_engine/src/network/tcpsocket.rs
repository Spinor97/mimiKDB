use anyhow::Result;
use tokio::net::TcpStream;
use tokio_native_tls::native_tls::TlsConnector as NativeTlsConnector;
use tokio_native_tls::TlsConnector;

use crate::config::configs::LogOnConfig;

/// Opens a TLS-wrapped TCP connection to the exchange described by `logon_config`.
pub async fn connect(logon_config: &LogOnConfig) -> Result<tokio_native_tls::TlsStream<TcpStream>> {
    let addr = format!("{}:{}", logon_config.host, logon_config.port);

    let stream = TcpStream::connect(&addr).await?;
    let cx = NativeTlsConnector::new()?;
    let connector = TlsConnector::from(cx);
    Ok(connector.connect(&logon_config.host, stream).await?)
}
