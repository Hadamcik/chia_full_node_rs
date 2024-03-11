use std::{fs, sync::Arc};

use chia_client::{Peer};
use chia_protocol::NodeType;
use chia_ssl::ChiaCertificate;
use native_tls::{Identity, TlsConnector};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Error, Connector};

#[tokio::main]
async fn main() {
    let cert = load_ssl_cert("full_node.crt", "full_node.key");
    let tls = create_tls_connector(&cert);
    let my_peer = connect_peer("localhost:8444", tls).await.unwrap();
    my_peer.send_handshake("mainnet".to_string(), NodeType::FullNode).await.unwrap();
}

fn load_ssl_cert(cert_path: &str, key_path: &str) -> ChiaCertificate {
    fs::read_to_string(cert_path)
        .and_then(|cert| {
            fs::read_to_string(key_path).map(|key| ChiaCertificate {
                cert_pem: cert,
                key_pem: key,
            })
        })
        .unwrap_or_else(|_| {
            let cert = ChiaCertificate::generate().expect("could not generate certificate");
            fs::write(cert_path, &cert.cert_pem).expect("could not write certificate");
            fs::write(key_path, &cert.key_pem).expect("could not write private key");
            cert
        })
}

fn create_tls_connector(cert: &ChiaCertificate) -> TlsConnector {
    let identity = Identity::from_pkcs8(cert.cert_pem.as_bytes(), cert.key_pem.as_bytes())
        .expect("could not create identity");

    TlsConnector::builder()
        .identity(identity)
        .danger_accept_invalid_certs(true)
        .build()
        .expect("could not create connector")
}
async fn connect_peer(
    full_node_uri: &str,
    tls_connector: TlsConnector,
) -> Result<Arc<Peer>, Error> {
    let ws = connect_async_tls_with_config(
        format!("wss://{}/ws", full_node_uri),
        None,
        false,
        Some(Connector::NativeTls(tls_connector)),
    )
        .await?
        .0;
    Ok(Arc::new(Peer::new(ws)))
}
