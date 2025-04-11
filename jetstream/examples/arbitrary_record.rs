//! An example of how to listen for create/delete events on a specific DID and potentialy unknown
//! NSID

use atrium_api::types::string;
use clap::Parser;
use jetstream::{
    events::{
        CommitOp,
        EventKind,
        JetstreamEvent,
    },
    DefaultJetstreamEndpoints,
    JetstreamCompression,
    JetstreamConfig,
    JetstreamConnector,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// The DIDs to listen for events on, if not provided we will listen for all DIDs.
    #[arg(short, long)]
    did: Option<Vec<string::Did>>,
    /// The NSID for the collection to listen for (e.g. `blue.flashes.feed.post`).
    #[arg(short, long)]
    nsid: string::Nsid,
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let dids = args.did.unwrap_or_default();
    let config: JetstreamConfig = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![args.nsid.clone()],
        wanted_dids: dids.clone(),
        compression: JetstreamCompression::Zstd,
        ..Default::default()
    };

    let jetstream = JetstreamConnector::new(config)?;
    let mut receiver = jetstream.connect().await?;

    println!(
        "Listening for new and updated '{}' events on DIDs: {:?}",
        args.nsid.as_str(),
        dids
    );

    while let Some(event) = receiver.recv().await {
        if let JetstreamEvent {
            kind: EventKind::Commit,
            commit: Some(commit),
            ..
        } = event
        {
            if commit.collection != args.nsid {
                continue;
            }
            if !(commit.operation == CommitOp::Create || commit.operation == CommitOp::Update) {
                continue;
            }
            let Some(rec) = commit.record else { continue };
            println!(
                "New or updated record! ({})\n{:?}\n",
                commit.rkey.as_str(),
                rec.get()
            );
        }
    }

    Ok(())
}
