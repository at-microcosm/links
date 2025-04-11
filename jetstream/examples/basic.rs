//! A very basic example of how to listen for create/delete events on a specific DID and NSID.

use atrium_api::{
    record::KnownRecord::AppBskyFeedPost,
    types::string,
};
use clap::Parser;
use jetstream::{
    events::{
        CommitEvent,
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
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let args = Args::parse();

    let dids = args.did.unwrap_or_default();
    let config = JetstreamConfig {
        endpoint: DefaultJetstreamEndpoints::USEastOne.into(),
        wanted_collections: vec![string::Nsid::new("app.bsky.feed.post".to_string()).unwrap()],
        wanted_dids: dids.clone(),
        compression: JetstreamCompression::Zstd,
        ..Default::default()
    };

    let jetstream = JetstreamConnector::new(config)?;
    let mut receiver = jetstream.connect().await?;

    println!("Listening for 'app.bsky.feed.post' events on DIDs: {dids:?}");

    while let Some(event) = receiver.recv().await {
        if let JetstreamEvent {
            kind: EventKind::Commit,
            commit:
                Some(CommitEvent {
                    operation: CommitOp::Create,
                    rkey,
                    record: Some(record),
                    ..
                }),
            ..
        } = event
        {
            if let Ok(AppBskyFeedPost(rec)) = serde_json::from_str(record.get()) {
                println!("New post created! ({})\n{:?}\n", rkey.as_str(), rec.text);
            }
        }
    }

    Ok(())
}
