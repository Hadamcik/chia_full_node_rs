use std::{fs, sync::Arc};
use std::collections::HashMap;
use std::thread::sleep;
use std::time::Duration;

use chia_client::{Peer};
use chia_protocol::{HeaderBlock, NodeType, RespondAdditions, RespondRemovals};
use chia_ssl::ChiaCertificate;
use native_tls::{Identity, TlsConnector};
use neo4rs::{Graph, Node, query};
use tokio_tungstenite::{connect_async_tls_with_config, tungstenite::Error, Connector};

#[tokio::main]
async fn main() {
    let uri = String::from("localhost:7687");
    let user = "neo4j";
    let pass = "localhost";
    let graph = Graph::new(&uri, user, pass).await.unwrap();
    let cert = load_ssl_cert("full_node.crt", "full_node.key");
    let tls = create_tls_connector(&cert);
    let my_peer = connect_peer("localhost:8444", tls).await.unwrap();
    my_peer.send_handshake("mainnet".to_string(), NodeType::FullNode).await.unwrap();

    loop {
        let last_local_height = get_last_synced_block(&graph).await.unwrap_or(-1);
        println!("Last local height: {}", last_local_height);
        let height = last_local_height + 1;
        let block_header_result = my_peer.request_block_header(height as u32).await;
        if block_header_result.is_err() {
            println!("Block not found.");
            break;
        }
        let block_header = block_header_result.unwrap();
        add_block(&graph, &height, &block_header).await;
        let additions = my_peer.request_additions(height as u32, Some(block_header.header_hash()), None).await.unwrap();
        add_coins(&graph, &height, additions).await;
        let removals_result = my_peer.request_removals(height as u32, block_header.header_hash(), None).await;
        if removals_result.is_ok() {
            remove_coins(&graph, &height, removals_result.unwrap()).await;
        }
    }

    sleep(Duration::from_secs(1));
}

async fn get_last_synced_block(graph: &Graph) -> Option<i64> {
    let graph = graph.clone();
    let mut result = graph.execute(
        query("MATCH (b:Block) RETURN b ORDER BY b.height DESC LIMIT 1")
    ).await.unwrap();

    let mut height: Option<i64> = None;
    while let Ok(Some(row)) = result.next().await {
        let node: Node = row.get("b").unwrap();
        height = node.get("height").unwrap();
    }

    height
}

async fn add_block(graph: &Graph, height: &i64, header_block: &HeaderBlock) {
    let graph = graph.clone();
    let _ = graph.execute(
        query("\
            MERGE (block:Block {height: $height}) \
            ON CREATE SET block.header_hash = $headerHash
        ")
            .param("height", height.clone())
            .param("headerHash", header_block.header_hash().to_string())
    ).await.expect("TODO: panic message").next().await;
}

async fn add_coins(graph: &Graph, block_height: &i64, additions: RespondAdditions) {
    let graph = graph.clone();
    let coins_data: Vec<_> = additions.coins
        .iter()
        .map(|coin| {
            let coin = coin.1.first().unwrap();
            let mut data = HashMap::new();
            data.insert("coin_id".to_string(), coin.coin_id().iter().map(|byte| format!("{:02x}", byte)).collect::<String>());
            data.insert("parent_coin_info".to_string(), hex::encode(&coin.parent_coin_info));
            data.insert("puzzle_hash".to_string(), hex::encode(&coin.puzzle_hash));
            data.insert("amount".to_string(), coin.amount.to_string());

            data
        })
        .collect();

    let _ = graph.execute(
        query("\
            WITH $blockHeight AS blockHeight, $coins as coins \
            UNWIND coins AS coin \
            MATCH (block:Block {height: blockHeight}) \
            MERGE (c:Coin {id: coin.coin_id}) \
            MERGE (block)-[:ADDITION]->(c) \
            ON CREATE SET \
                c.parent_coin_info = coin.parent_coin_info, \
                c.puzzle_hash = coin.puzzle_hash, \
                c.amount = coin.amount \
            "
        )
            .param("blockHeight", block_height.clone())
            .param("coins", coins_data)
    ).await.expect("TODO: panic message").next().await;
}

async fn remove_coins(graph: &Graph, block_height: &i64, removals: RespondRemovals) {
    let graph = graph.clone();
    let coin_ids: Vec<String> = removals.coins
        .iter()
        .map(|coin| {
            coin.clone().1.unwrap().coin_id().iter().map(|byte| format!("{:02x}", byte)).collect::<String>()
        })
        .collect();

    let _ = graph.execute(
        query("\
            WITH $blockHeight AS blockHeight, $coinIds AS coinIds \
            MATCH (block:Block {height: blockHeight}), (coin:Coin) \
            WHERE coin.id IN coinIds \
            WITH block, coin \
            MERGE (block)-[:REMOVAL]->(coin)
            "
        )
            .param("blockHeight", block_height.clone())
            .param("coinIds", coin_ids)
    ).await.expect("TODO: panic message").next().await;
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
