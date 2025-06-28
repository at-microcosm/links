// use bincode::config::Options;
// use clap::Parser;
// use serde::Serialize;
// use std::collections::HashMap;
// use std::path::PathBuf;

// use tokio_util::sync::CancellationToken;

// use constellation::storage::rocks_store::{
//     Collection, DidId, RKey, RPath, Target, TargetKey, TargetLinkers, _bincode_opts,
// };
// use constellation::storage::RocksStorage;
// use constellation::Did;

// use links::parse_any_link;
// use rocksdb::IteratorMode;
// use std::time;

// xxxx/// Aggregate links in the at-mosphere
// #[derive(Parser, Debug)]
// #[command(version, about, long_about = None)]
// struct Args {
//     /// where is rocksdb's data
//     #[arg(short, long)]
//     data: PathBuf,
//     /// slow down so we don't kill the firehose consumer, if running concurrently
//     #[arg(short, long)]
//     limit: Option<u64>,
// }

// type LinkType = String;

// #[derive(Debug, Eq, Hash, PartialEq, Serialize)]
// struct SourceLink(Collection, RPath, LinkType, Option<Collection>); // last is target collection, if it's an at-uri link with a collection

// #[derive(Debug, Serialize)]
// struct SourceSample {
//     did: String,
//     rkey: String,
// }

// #[derive(Debug, Default, Serialize)]
// struct Bucket {
//     count: u64,
//     sum: u64,
//     sample: Option<SourceSample>,
// }

// #[derive(Debug, Default, Serialize)]
// struct Buckets([Bucket; 23]);

// const BUCKETS: [u64; 23] = [
//     1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 32, 64, 128, 256, 512, 1024, 4096, 16_384, 65_535,
//     262_144, 1_048_576,
// ];

// xxx// b1, b2, b3, b4, b5, b6, b7, b8, b9, b10, b12, b16, b32, b64, b128, b256, b512, b1024, b4096, b16384, b65535, b262144, bmax

// static DID_IDS_CF: &str = "did_ids";
// static TARGET_IDS_CF: &str = "target_ids";
// static TARGET_LINKERS_CF: &str = "target_links";

// const REPORT_INTERVAL: usize = 50_000;

// type Stats = HashMap<SourceLink, Buckets>;

// #[derive(Debug, Serialize)]
// struct Printable {
//     collection: String,
//     path: String,
//     link_type: String,
//     target_collection: Option<String>,
//     buckets: Buckets,
// }

// #[derive(Debug, Default)]
// struct ErrStats {
//     failed_to_get_sample: usize,
//     failed_to_read_target_id: usize,
//     failed_to_deserialize_target_key: usize,
//     failed_to_parse_target_as_link: usize,
//     failed_to_get_links: usize,
//     failed_to_deserialize_linkers: usize,
// }

// fn thousands(n: usize) -> String {
//     n.to_string()
//         .as_bytes()
//         .rchunks(3)
//         .rev()
//         .map(std::str::from_utf8)
//         .collect::<Result<Vec<&str>, _>>()
//         .unwrap()
//         .join(",")
// }

// fn main() {
//     let args = Args::parse();

//     let limit = args.limit.map(|amount| {
//         ratelimit::Ratelimiter::builder(amount, time::Duration::from_secs(1))
//             .max_tokens(amount)
//             .initial_available(amount)
//             .build()
//             .unwrap()
//     });

//     eprintln!("starting rocksdb...");
//     let rocks = RocksStorage::open_readonly(args.data).unwrap();
//     eprintln!("rocks ready.");

//     let RocksStorage { ref db, .. } = rocks;

//     let stay_alive = CancellationToken::new();
//     ctrlc::set_handler({
//         let mut desperation: u8 = 0;
//         let stay_alive = stay_alive.clone();
//         move || match desperation {
//             0 => {
//                 eprintln!("ok, shutting down...");
//                 stay_alive.cancel();
//                 desperation += 1;
//             }
//             1.. => panic!("fine, panicking!"),
//         }
//     })
//     .unwrap();

//     let mut stats = Stats::new();
//     let mut err_stats: ErrStats = Default::default();

//     let did_ids_cf = db.cf_handle(DID_IDS_CF).unwrap();
//     let target_id_cf = db.cf_handle(TARGET_IDS_CF).unwrap();
//     let target_links_cf = db.cf_handle(TARGET_LINKERS_CF).unwrap();

//     let t0 = time::Instant::now();
//     let mut t_prev = t0;

//     let mut i = 0;
//     for item in db.iterator_cf(&target_id_cf, IteratorMode::Start) {
//         if stay_alive.is_cancelled() {
//             break;
//         }

//         if let Some(ref limiter) = limit {
//             if let Err(dur) = limiter.try_wait() {
//                 std::thread::sleep(dur)
//             }
//         }

//         if i > 0 && i % REPORT_INTERVAL == 0 {
//             let now = time::Instant::now();
//             let rate = (REPORT_INTERVAL as f32) / (now.duration_since(t_prev).as_secs_f32());
//             eprintln!(
//                 "{i}\t({}k)\t{:.2}\t{rate:.1}/s",
//                 thousands(i / 1000),
//                 t0.elapsed().as_secs_f32()
//             );
//             t_prev = now;
//         }
//         i += 1;

//         let Ok((target_key, target_id)) = item else {
//             err_stats.failed_to_read_target_id += 1;
//             continue;
//         };

//         let Ok(TargetKey(Target(target), collection, rpath)) =
//             _bincode_opts().deserialize(&target_key)
//         else {
//             err_stats.failed_to_deserialize_target_key += 1;
//             continue;
//         };

//         let source = {
//             let Some(parsed) = parse_any_link(&target) else {
//                 err_stats.failed_to_parse_target_as_link += 1;
//                 continue;
//             };
//             SourceLink(
//                 collection,
//                 rpath,
//                 parsed.name().into(),
//                 parsed.at_uri_collection().map(Collection),
//             )
//         };

//         let Ok(Some(links_raw)) = db.get_cf(&target_links_cf, &target_id) else {
//             err_stats.failed_to_get_links += 1;
//             continue;
//         };
//         let Ok(linkers) = _bincode_opts().deserialize::<TargetLinkers>(&links_raw) else {
//             err_stats.failed_to_deserialize_linkers += 1;
//             continue;
//         };
//         let (n, _) = linkers.count();

//         if n == 0 {
//             continue;
//         }

//         let mut bucket = 0;
//         for edge in BUCKETS {
//             if n <= edge || bucket == 22 {
//                 break;
//             }
//             bucket += 1;
//         }

//         let b = &mut stats.entry(source).or_default().0[bucket];
//         b.count += 1;
//         b.sum += n;
//         if b.sample.is_none() {
//             let (DidId(did_id), RKey(k)) = &linkers.0[(n - 1) as usize];
//             if let Ok(Some(did_bytes)) = db.get_cf(&did_ids_cf, did_id.to_be_bytes()) {
//                 if let Ok(Did(did)) = _bincode_opts().deserialize(&did_bytes) {
//                     b.sample = Some(SourceSample {
//                         did,
//                         rkey: k.clone(),
//                     });
//                 } else {
//                     err_stats.failed_to_get_sample += 1;
//                 }
//             } else {
//                 err_stats.failed_to_get_sample += 1;
//             }
//         }

//         // if i >= 40_000 {
//         //     break;
//         // }
//     }

//     let dt = t0.elapsed();

//     eprintln!("gathering stats for output...");

//     let itemified = stats
//         .into_iter()
//         .map(
//             |(
//                 SourceLink(Collection(collection), RPath(path), link_type, target_collection),
//                 buckets,
//             )| Printable {
//                 collection,
//                 path,
//                 link_type,
//                 target_collection: target_collection.map(|Collection(c)| c),
//                 buckets,
//             },
//         )
//         .collect::<Vec<_>>();

//     match serde_json::to_string(&itemified) {
//         Ok(s) => println!("{s}"),
//         Err(e) => eprintln!("failed to serialize results: {e:?}"),
//     }

//     eprintln!(
//         "{} summarizing {} link targets in {:.1}s",
//         if stay_alive.is_cancelled() {
//             "STOPPED"
//         } else {
//             "FINISHED"
//         },
//         thousands(i),
//         dt.as_secs_f32()
//     );
//     eprintln!("{err_stats:?}");
//     eprintln!("bye.");
// }

// xxx// scan plan

// xxx// buckets (backlink count)
// xxx// 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 12, 16, 32, 64, 128, 256, 512, 1024, 4096, 16384, 65535, 262144, 1048576+
// xxx// by
// xxx// - collection
// xxx// - json path
// xxx// - link type
// xxx// samples for each bucket for each variation
fn main() {}
