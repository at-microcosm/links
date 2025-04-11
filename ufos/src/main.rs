use clap::Parser;
use std::path::PathBuf;
use ufos::consumer;
use ufos::error::StorageError;
use ufos::server;
use jetstream::events::Cursor;
use ufos::storage::{StorageWhatever, StoreReader, StoreWriter};
use ufos::storage_fjall::FjallStorage;
use ufos::storage_mem::MemStorage;

#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Jetstream server to connect to (exclusive with --fixture). Provide either a wss:// URL, or a shorhand value:
    /// 'us-east-1', 'us-east-2', 'us-west-1', or 'us-west-2'
    #[arg(long)]
    jetstream: String,
    /// allow changing jetstream endpoints
    #[arg(long, action)]
    jetstream_force: bool,
    /// don't request zstd-compressed jetstream events
    ///
    /// reduces CPU at the expense of more ingress bandwidth
    #[arg(long, action)]
    jetstream_no_zstd: bool,
    /// Location to store persist data to disk
    #[arg(long)]
    data: PathBuf,
    /// DEBUG: don't start the jetstream consumer or its write loop
    /// todo: restore this
    #[arg(long, action)]
    pause_writer: bool,
    /// DEBUG: force the rw loop to fall behind  by pausing it
    /// todo: restore this
    #[arg(long, action)]
    pause_rw: bool,
    /// DEBUG: use an in-memory store instead of fjall
    #[arg(long, action)]
    in_mem: bool,
}

// #[tokio::main(flavor = "current_thread")] // TODO: move this to config via args
#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    let args = Args::parse();
    let jetstream = args.jetstream.clone();
    if args.in_mem {
        let (read_store, write_store, cursor) = MemStorage::init(
            args.data,
            jetstream,
            args.jetstream_force,
            Default::default(),
        )?;
        go(args.jetstream, args.pause_writer, read_store, write_store, cursor).await?;
    } else {
        let (read_store, write_store, cursor) = FjallStorage::init(
            args.data,
            jetstream,
            args.jetstream_force,
            Default::default(),
        )?;
        go(args.jetstream, args.pause_writer, read_store, write_store, cursor).await?;
    }

    Ok(())
}

async fn go(
    jetstream: String,
    pause_writer: bool,
    read_store: impl StoreReader + 'static,
    mut write_store: impl StoreWriter + 'static,
    cursor: Option<Cursor>,
) -> anyhow::Result<()> {
    println!("starting server with storage...");
    let serving = server::serve(read_store);

    let t1 = tokio::task::spawn(async {
        let r = serving.await;
        log::warn!("serving ended with: {r:?}");
    });

    let t2: tokio::task::JoinHandle<anyhow::Result<()>> = tokio::task::spawn({
        async move {
            if !pause_writer {
                println!(
                    "starting consumer with cursor: {cursor:?} from {:?} ago",
                    cursor.map(|c| c.elapsed())
                );
                let mut batches =
                    consumer::consume(&jetstream, cursor, false).await?;

                log::info!("started consumer, got chan etc...");

                tokio::task::spawn_blocking(move || {
                    while let Some(event_batch) = batches.blocking_recv() {
                        log::info!("got batch, putting to storage...");
                        write_store.insert_batch(event_batch)?;
                        log::info!("inserted batch...");
                        write_store.step_rollup()
                            .inspect_err(|e| log::error!("laksjdfl: {e:?}"))?;
                        log::info!("inserted and stepped rollup. ready for next...");
                    }
                    log::warn!("??????????????????????");
                    Ok::<(), StorageError>(())
                })
                .await??;

                // let r = storage.receive(batches).await;
                log::warn!("storage.receive ended with");
            } else {
                log::info!("not starting jetstream or the write loop.");
            }
            Ok(())
        }
    });

    // let t3 = tokio::task::spawn(async move {
    //     if !args.pause_rw {
    //         let r = storage.rw_loop().await;
    //         log::warn!("storage.rw_loop ended with: {r:?}");
    //     } else {
    //         log::info!("not starting rw loop.");
    //     }
    // });

    // tokio::select! {
    //     // v = serving => eprintln!("serving ended: {v:?}"),
    //     v = storage.receive(batches) => eprintln!("storage consumer ended: {v:?}"),
    //     v = storage.rw_loop() => eprintln!("storage rw-loop ended: {v:?}"),
    // };

    log::trace!("tasks running. waiting.");
    tokio::select! {
        z = t1 => log::warn!("serve task ended: {z:?}"),
        z = t2 => log::warn!("storage task ended: {z:?}"),
    };

    // t1.await?;
    // log::trace!("serve task ended.");
    // t2.await??;
    // log::trace!("storage receive task ended.");
    // // t3.await?;
    // // log::trace!("storage rw task ended.");

    println!("bye!");

    Ok(())
}
