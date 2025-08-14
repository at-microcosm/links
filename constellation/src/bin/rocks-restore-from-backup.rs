use crate::time::Instant;
use anyhow::Result;
use clap::Parser;
use std::path::PathBuf;

#[cfg(feature = "rocks")]
use rocksdb::backup::{BackupEngine, BackupEngineOptions, RestoreOptions};

use std::time;

/// Aggregate links in the at-mosphere
#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// the backup directory to restore *from*
    #[arg(long)]
    from_backup_dir: PathBuf,
    /// the db dir to restore *to*
    #[arg(long)]
    to_data_dir: PathBuf,
}

#[cfg(feature = "rocks")]
fn main() -> Result<()> {
    let args = Args::parse();

    eprintln!(
        "restoring latest rocksdb backup from {:?} to {:?}...",
        args.from_backup_dir, args.to_data_dir
    );

    let mut engine = BackupEngine::open(
        &BackupEngineOptions::new(args.from_backup_dir)?,
        &rocksdb::Env::new()?,
    )?;

    let t0 = Instant::now();
    if let Err(e) = engine.restore_from_latest_backup(
        &args.to_data_dir,
        &args.to_data_dir,
        &RestoreOptions::default(),
    ) {
        eprintln!(
            "restoring from backup failed after {:?}: {e:?}",
            t0.elapsed()
        );
    } else {
        eprintln!(
            "success, restored latest from backup after {:?}",
            t0.elapsed()
        );
    }

    eprintln!("bye.");
    Ok(())
}
