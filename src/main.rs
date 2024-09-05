mod error;
mod leader_election;

use clap::Parser;
use leader_election::*;
use miette::{miette, IntoDiagnostic, Result};
use std::{env, io, time::Duration};
use tokio::{
    select,
    sync::{mpsc, oneshot},
};
use tokio_graceful_shutdown::{SubsystemBuilder, SubsystemHandle, Toplevel};
use tracing_subscriber::filter::EnvFilter;
use worterbuch_client::{config::Config, AuthToken};

#[derive(Parser)]
#[command(author, version, about = "Perform a leader election on worterbuch.", long_about = None)]
struct Args {
    /// Connect to the Wörterbuch server using SSL encryption.
    #[arg(short, long)]
    ssl: bool,
    /// The address of the Wörterbuch server. When omitted, the value of the env var WORTERBUCH_HOST_ADDRESS will be used. If that is not set, 127.0.0.1 will be used.
    #[arg(short, long)]
    addr: Option<String>,
    /// The port of the Wörterbuch server. When omitted, the value of the env var WORTERBUCH_PORT will be used. If that is not set, 4242 will be used.
    #[arg(short, long)]
    port: Option<u16>,
    /// Auth token to be used for acquiring authorization from the server. When omitted, the value of the env var WORTERBUCH_AUTH_TOKEN will be used. If that is not set, 4242 will be used.
    #[arg(long)]
    auth: Option<AuthToken>,
    /// Namespace on worterbuch to ensure the election does not collide with that of other applications. When omitted, the value of the env var ELECTION_NAMESPACE will be used [default: "leader/election"]
    #[arg(short, long)]
    namespace: Option<String>,
    /// Group name for which to perform the leader election. All instances in the group for which a leader is to be elected must use the same group name. When omitted, the value of the env var ELECTION_GROUP_NAME will be used.
    #[arg(short, long)]
    group_name: Option<String>,
    /// Election quorum (i.e. minimum number of votes an instance needs to become leader). When omitted, the value of the env var ELECTION_QUORUM will be used [default: 2]
    #[arg(short, long)]
    quorum: Option<usize>,
    /// Minimum delay in ms to be used in election algorithm. A random delay is used to reduce the chance of tied votes. When omitted, the value of the env var ELECTION_MIN_DELAY will be used [default: 50]
    #[arg(long)]
    min_delay: Option<u64>,
    /// Maximum delay in ms to be used in election algorithm. A random delay is used to reduce the chance of tied votes. When omitted, the value of the env var ELECTION_MAX_DELAY will be used [default: 200]
    #[arg(long)]
    max_delay: Option<u64>,
    /// Heartbeat interval in ms at which the leader pings its followers to let them know it still exists. Followers will wait a maximum of 2x the heartbeat interval before initiating a new election. When omitted, the value of the env var ELECTION_HEARTBEAT will be used [default: 500]
    #[arg(long)]
    heartbeat: Option<u64>,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    dotenv::dotenv().ok();
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_writer(io::stderr)
        .init();
    Toplevel::new(|s| async move {
        s.start(SubsystemBuilder::new("leader-election", leader_election));
    })
    .catch_signals()
    .handle_shutdown_requests(Duration::from_millis(1000))
    .await?;

    Ok(())
}

async fn leader_election(subsys: SubsystemHandle) -> Result<()> {
    let mut config = Config::new();
    let args: Args = Args::parse();

    config.auth_token = args.auth.or(config.auth_token);

    config.proto = if args.ssl {
        "wss".to_owned()
    } else {
        config.proto
    };
    config.host_addr = args.addr.unwrap_or(config.host_addr);
    config.port = args.port.or(config.port);

    let (wb_disconnected, on_wb_disconnected) = oneshot::channel();
    let wb = worterbuch_client::connect(config, async move {
        wb_disconnected.send(()).ok();
    })
    .await
    .into_diagnostic()?;

    let group_name = if let Some(it) = env::var("ELECTION_GROUP_NAME").ok().or(args.group_name) {
        it.into()
    } else {
        return Err(miette!("No group name specified"));
    };

    let namespace = env::var("ELECTION_NAMESPACE")
        .ok()
        .or(args.namespace)
        .map(|it| it.into());

    let quorum = env::var("ELECTION_QUORUM")
        .ok()
        .and_then(|v| v.parse().ok())
        .or(args.quorum)
        .unwrap_or(2);

    let min_delay = env::var("ELECTION_MIN_DELAY")
        .ok()
        .and_then(|v| v.parse().ok())
        .or(args.min_delay)
        .unwrap_or(50);

    let max_delay = env::var("ELECTION_MAX_DELAY")
        .ok()
        .and_then(|v| v.parse().ok())
        .or(args.max_delay)
        .unwrap_or(200);

    let heartbeat = env::var("ELECTION_HEARTBEAT")
        .ok()
        .and_then(|v| v.parse().ok())
        .or(args.heartbeat)
        .unwrap_or(500);

    let (osdr_tx, on_shutdown_requested) = oneshot::channel::<()>();
    subsys.start(SubsystemBuilder::new(
        "on-shutdown-requested",
        |s| async move {
            select! {
                _ = s.on_shutdown_requested() => (),
                _ = on_wb_disconnected => (),
            }
            osdr_tx.send(()).ok();
            Ok(()) as Result<()>
        },
    ));

    let instance_id = env::var("ELECTION_INSTANCE_ID")
        .unwrap_or(wb.client_id().to_owned())
        .into();

    let (rqstshtdn_tx, mut rqstshtdn_rx) = mpsc::unbounded_channel();

    subsys.start(SubsystemBuilder::new("request-shutdown", |s| async move {
        rqstshtdn_rx.recv().await;
        s.request_shutdown();
        Ok(()) as Result<()>
    }));

    let request_shutdown = move || {
        rqstshtdn_tx.send(()).ok();
    };

    let mut leader_changes = elect_leader(
        namespace,
        wb,
        instance_id,
        group_name,
        quorum,
        min_delay,
        max_delay,
        heartbeat,
        request_shutdown,
        on_shutdown_requested,
    )
    .await
    .into_diagnostic()?;

    while let Some(e) = leader_changes.recv().await {
        log::info!("{e:?}");
    }

    Ok(())
}
