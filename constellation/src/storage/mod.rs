use crate::{ActionableEvent, CountsByCount, Did, RecordId};
use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

pub mod mem_store;
pub use mem_store::MemStorage;

#[cfg(feature = "rocks")]
pub mod rocks_store;
#[cfg(feature = "rocks")]
pub use rocks_store::RocksStorage;

#[derive(Debug, PartialEq)]
pub struct PagedAppendingCollection<T> {
    pub version: (u64, u64), // (collection length, deleted item count) // TODO: change to (total, active)? since dedups isn't "deleted"
    pub items: Vec<T>,
    pub next: Option<u64>,
    pub total: u64,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct StorageStats {
    /// estimate of how many accounts we've seen create links. the _subjects_ of any links are not represented here.
    /// for example: new user A follows users B and C. this count will only increment by one, for A.
    pub dids: u64,

    /// estimate targets * distinct (collection, path)s to reference them.
    /// distinct targets alone are currently challenging to estimate.
    pub targetables: u64,

    /// estimate of the count of atproto records seen that contain links.
    /// records with multiple links are single-counted.
    /// for LSM stores, deleted links don't decrement this, and updated records with any links will likely increment it.
    pub linking_records: u64,
}

pub trait LinkStorage: Send + Sync {
    /// jetstream cursor from last saved actions, if available
    fn get_cursor(&mut self) -> Result<Option<u64>> {
        Ok(None)
    }

    fn push(&mut self, event: &ActionableEvent, cursor: u64) -> Result<()>;

    // readers are  off from the writer instance
    fn to_readable(&mut self) -> impl LinkReader;
}

pub trait LinkReader: Clone + Send + Sync + 'static {
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64>;

    fn get_distinct_did_count(&self, target: &str, collection: &str, path: &str) -> Result<u64>;

    fn get_links(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
        filter_dids: &HashSet<Did>,
    ) -> Result<PagedAppendingCollection<RecordId>>;

    fn get_distinct_dids(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
    ) -> Result<PagedAppendingCollection<Did>>; // TODO: reflect dedups in cursor

    fn get_all_record_counts(&self, _target: &str)
        -> Result<HashMap<String, HashMap<String, u64>>>;

    fn get_all_counts(
        &self,
        _target: &str,
    ) -> Result<HashMap<String, HashMap<String, CountsByCount>>>;

    /// assume all stats are estimates, since exact counts are very challenging for LSMs
    fn get_stats(&self) -> Result<StorageStats>;
}

#[cfg(test)]
mod tests {
    use super::*;
    use links::{CollectedLink, Link};
    use std::ops::RangeBounds;

    macro_rules! test_each_storage {
        ($test_name:ident, |$storage_label:ident| $test_code:block) => {
            #[test]
            fn $test_name() -> Result<()> {
                {
                    println!("=> testing with memstorage backend");
                    #[allow(unused_mut)]
                    let mut $storage_label = MemStorage::new();
                    $test_code
                }

                #[cfg(feature = "rocks")]
                {
                    println!("=> testing with rocksdb backend");
                    let rocks_db_path = tempfile::tempdir()?;
                    #[allow(unused_mut)]
                    let mut $storage_label = RocksStorage::new(rocks_db_path.path())?;
                    $test_code
                }

                Ok(())
            }
        };
    }

    fn assert_stats(
        stats: StorageStats,
        dids: impl RangeBounds<u64>,
        targetables: impl RangeBounds<u64>,
        linking_records: impl RangeBounds<u64>,
    ) {
        fn check(name: &str, stat: u64, rb: impl RangeBounds<u64>) {
            assert!(
                rb.contains(&stat),
                "{name:?}: {stat:?} not in range {:?}–{:?}",
                rb.start_bound(),
                rb.end_bound()
            );
        }
        check("dids", stats.dids, dids);
        check("targetables", stats.targetables, targetables);
        check("linking_records", stats.linking_records, linking_records);
    }

    test_each_storage!(test_empty, |storage| {
        assert_eq!(storage.get_count("", "", "")?, 0);
        assert_eq!(storage.get_count("a", "b", "c")?, 0);
        assert_eq!(
            storage.get_count(
                "at://did:plc:b3rzzkblqsxhr3dgcueymkqe/app.bsky.feed.post/3lf6yc4drhk2f",
                "app.t.c",
                ".reply.parent.uri"
            )?,
            0
        );
        assert_eq!(storage.get_distinct_did_count("", "", "")?, 0);
        assert_eq!(
            storage.get_links(
                "a.com",
                "app.t.c",
                ".abc.uri",
                100,
                None,
                &HashSet::default()
            )?,
            PagedAppendingCollection {
                version: (0, 0),
                items: vec![],
                next: None,
                total: 0,
            }
        );
        assert_eq!(
            storage.get_distinct_dids("a.com", "app.t.c", ".abc.uri", 100, None)?,
            PagedAppendingCollection {
                version: (0, 0),
                items: vec![],
                next: None,
                total: 0,
            }
        );
        assert_eq!(storage.get_all_counts("bad-example.com")?, HashMap::new());
        assert_eq!(
            storage.get_all_record_counts("bad-example.com")?,
            HashMap::new()
        );

        assert_stats(storage.get_stats()?, 0..=0, 0..=0, 0..=0);
    });

    test_each_storage!(test_add_link, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            1
        );
        assert_eq!(storage.get_count("bad.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("e.com", "app.t.c", ".bad.uri")?, 0);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".bad.uri")?,
            0
        );
        assert_stats(storage.get_stats()?, 1..=1, 1..=1, 1..=1);
    });

    test_each_storage!(test_links, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;

        // delete under the wrong collection
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.test.wrongcollection".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // delete under the wrong rkey
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "wrongkey".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);

        // finally actually delete it
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);

        // put it back
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            1
        );

        // add another link from this user
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa2".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            1
        );

        // add a link from someone else
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdfasdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 3);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            2
        );

        // aaaand delete the first one again
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            2
        );
        assert_stats(storage.get_stats()?, 2..=2, 1..=1, 2..=2);
    });

    test_each_storage!(test_two_user_links_delete_one, |storage| {
        // create the first link
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "A".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            1
        );

        // create the second link (same user, different rkey)
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "B".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("e.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 2);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            1
        );

        // aaaand delete the first link
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "A".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            1
        );
        assert_stats(storage.get_stats()?, 1..=1, 1..=1, 1..=1);
    });

    test_each_storage!(test_accounts, |storage| {
        // create two links
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "A".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "B".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("b.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("b.com", "app.t.c", ".abc.uri")?, 1);

        // and a third from a different account
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:fdsa".into(),
                    collection: "app.t.c".into(),
                    rkey: "A".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 2);

        // delete the first account
        storage.push(&ActionableEvent::DeleteAccount("did:plc:asdf".into()), 0)?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("b.com", "app.t.c", ".abc.uri")?, 0);
        assert_stats(storage.get_stats()?, 1..=2, 2..=2, 1..=1);
    });

    test_each_storage!(multi_link, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("e.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("f.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("g.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            1
        );
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(
            storage.get_distinct_did_count("f.com", "app.t.c", ".xyz[].uri")?,
            1
        );
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(
            storage.get_distinct_did_count("g.com", "app.t.c", ".xyz[].uri")?,
            1
        );

        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "fdsa".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(
            storage.get_distinct_did_count("e.com", "app.t.c", ".abc.uri")?,
            0
        );
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 0);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 0);
        assert_stats(storage.get_stats()?, 1..=1, 3..=3, 0..=0);
    });

    test_each_storage!(update_link, |storage| {
        // create the links
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("e.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("f.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("g.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 1);

        // update them
        storage.push(
            &ActionableEvent::UpdateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "fdsa".into(),
                },
                new_links: vec![
                    CollectedLink {
                        target: Link::Uri("h.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("f.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("i.com".into()),
                        path: ".xyz[].uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("e.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("h.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("f.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_eq!(storage.get_count("g.com", "app.t.c", ".xyz[].uri")?, 0);
        assert_eq!(storage.get_count("i.com", "app.t.c", ".xyz[].uri")?, 1);
        assert_stats(storage.get_stats()?, 1..=1, 5..=5, 1..=1);
    });

    test_each_storage!(update_no_links_to_links, |storage| {
        // update without prior create (consumer would have filtered out the original)
        storage.push(
            &ActionableEvent::UpdateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                new_links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_stats(storage.get_stats()?, 1..=1, 1..=1, 1..=1);
    });

    test_each_storage!(delete_multi_link_same_target, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".def.uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 1);
        assert_eq!(storage.get_count("a.com", "app.t.c", ".def.uri")?, 1);

        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf".into(),
                collection: "app.t.c".into(),
                rkey: "asdf".into(),
            }),
            0,
        )?;
        assert_eq!(storage.get_count("a.com", "app.t.c", ".abc.uri")?, 0);
        assert_eq!(storage.get_count("a.com", "app.t.c", ".def.uri")?, 0);
        assert_stats(storage.get_stats()?, 1..=1, 2..=2, 0..=0);
    });

    test_each_storage!(get_links_basic, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        assert_eq!(
            storage.get_links(
                "a.com",
                "app.t.c",
                ".abc.uri",
                100,
                None,
                &HashSet::default()
            )?,
            PagedAppendingCollection {
                version: (1, 0),
                items: vec![RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                }],
                next: None,
                total: 1,
            }
        );
        assert_eq!(
            storage.get_distinct_dids("a.com", "app.t.c", ".abc.uri", 100, None)?,
            PagedAppendingCollection {
                version: (1, 0),
                items: vec!["did:plc:asdf".into()],
                next: None,
                total: 1,
            }
        );
        assert_stats(storage.get_stats()?, 1..=1, 1..=1, 1..=1);
    });

    test_each_storage!(get_links_paged, |storage| {
        for i in 1..=5 {
            storage.push(
                &ActionableEvent::CreateLinks {
                    record_id: RecordId {
                        did: format!("did:plc:asdf-{i}").into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    links: vec![CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    }],
                },
                0,
            )?;
        }
        let links =
            storage.get_links("a.com", "app.t.c", ".abc.uri", 2, None, &HashSet::default())?;
        let dids = storage.get_distinct_dids("a.com", "app.t.c", ".abc.uri", 2, None)?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (5, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-5".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-4".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: Some(3),
                total: 5,
            }
        );
        assert_eq!(
            dids,
            PagedAppendingCollection {
                version: (5, 0),
                items: vec!["did:plc:asdf-5".into(), "did:plc:asdf-4".into()],
                next: Some(3),
                total: 5,
            }
        );
        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            links.next,
            &HashSet::default(),
        )?;
        let dids = storage.get_distinct_dids("a.com", "app.t.c", ".abc.uri", 2, dids.next)?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (5, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-3".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-2".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: Some(1),
                total: 5,
            }
        );
        assert_eq!(
            dids,
            PagedAppendingCollection {
                version: (5, 0),
                items: vec!["did:plc:asdf-3".into(), "did:plc:asdf-2".into()],
                next: Some(1),
                total: 5,
            }
        );
        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            links.next,
            &HashSet::default(),
        )?;
        let dids = storage.get_distinct_dids("a.com", "app.t.c", ".abc.uri", 2, dids.next)?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (5, 0),
                items: vec![RecordId {
                    did: "did:plc:asdf-1".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },],
                next: None,
                total: 5,
            }
        );
        assert_eq!(
            dids,
            PagedAppendingCollection {
                version: (5, 0),
                items: vec!["did:plc:asdf-1".into()],
                next: None,
                total: 5,
            }
        );
        assert_stats(storage.get_stats()?, 5..=5, 1..=1, 5..=5);
    });

    test_each_storage!(get_filtered_links, |storage| {
        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            None,
            &HashSet::from([Did("did:plc:linker".to_string())]),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (0, 0),
                items: vec![],
                next: None,
            }
        );

        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:linker".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;

        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            None,
            &HashSet::from([Did("did:plc:linker".to_string())]),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (1, 0),
                items: vec![RecordId {
                    did: "did:plc:linker".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },],
                next: None,
            }
        );

        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            None,
            &HashSet::from([Did("did:plc:someone-else".to_string())]),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (0, 0),
                items: vec![],
                next: None,
            }
        );

        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:linker".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf-2".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:someone-else".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;

        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            None,
            &HashSet::from([Did("did:plc:linker".to_string())]),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (2, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:linker".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf-2".into(),
                    },
                    RecordId {
                        did: "did:plc:linker".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: None,
            }
        );

        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            None,
            &HashSet::from([
                Did("did:plc:linker".to_string()),
                Did("did:plc:someone-else".to_string()),
            ]),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (3, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:someone-else".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:linker".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf-2".into(),
                    },
                ],
                next: Some(1),
            }
        );

        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            None,
            &HashSet::from([Did("did:plc:someone-unknown".to_string())]),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (0, 0),
                items: vec![],
                next: None,
            }
        );
    });

    test_each_storage!(get_links_exact_multiple, |storage| {
        for i in 1..=4 {
            storage.push(
                &ActionableEvent::CreateLinks {
                    record_id: RecordId {
                        did: format!("did:plc:asdf-{i}").into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    links: vec![CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    }],
                },
                0,
            )?;
        }
        let links =
            storage.get_links("a.com", "app.t.c", ".abc.uri", 2, None, &HashSet::default())?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (4, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-4".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-3".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: Some(2),
                total: 4,
            }
        );
        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            links.next,
            &HashSet::default(),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (4, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-2".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-1".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: None,
                total: 4,
            }
        );
        assert_stats(storage.get_stats()?, 4..=4, 1..=1, 4..=4);
    });

    test_each_storage!(page_links_while_new_links_arrive, |storage| {
        for i in 1..=4 {
            storage.push(
                &ActionableEvent::CreateLinks {
                    record_id: RecordId {
                        did: format!("did:plc:asdf-{i}").into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    links: vec![CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    }],
                },
                0,
            )?;
        }
        let links =
            storage.get_links("a.com", "app.t.c", ".abc.uri", 2, None, &HashSet::default())?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (4, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-4".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-3".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: Some(2),
                total: 4,
            }
        );
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf-5".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![CollectedLink {
                    target: Link::Uri("a.com".into()),
                    path: ".abc.uri".into(),
                }],
            },
            0,
        )?;
        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            links.next,
            &HashSet::default(),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (5, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-2".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-1".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: None,
                total: 5,
            }
        );
        assert_stats(storage.get_stats()?, 5..=5, 1..=1, 5..=5);
    });

    test_each_storage!(page_links_while_some_are_deleted, |storage| {
        for i in 1..=4 {
            storage.push(
                &ActionableEvent::CreateLinks {
                    record_id: RecordId {
                        did: format!("did:plc:asdf-{i}").into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    links: vec![CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    }],
                },
                0,
            )?;
        }
        let links =
            storage.get_links("a.com", "app.t.c", ".abc.uri", 2, None, &HashSet::default())?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (4, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-4".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-3".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: Some(2),
                total: 4,
            }
        );
        storage.push(
            &ActionableEvent::DeleteRecord(RecordId {
                did: "did:plc:asdf-2".into(),
                collection: "app.t.c".into(),
                rkey: "asdf".into(),
            }),
            0,
        )?;
        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            links.next,
            &HashSet::default(),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (4, 1),
                items: vec![RecordId {
                    did: "did:plc:asdf-1".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },],
                next: None,
                total: 3,
            }
        );
        assert_stats(storage.get_stats()?, 4..=4, 1..=1, 3..=3);
    });

    test_each_storage!(page_links_accounts_inactive, |storage| {
        for i in 1..=4 {
            storage.push(
                &ActionableEvent::CreateLinks {
                    record_id: RecordId {
                        did: format!("did:plc:asdf-{i}").into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    links: vec![CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    }],
                },
                0,
            )?;
        }
        let links =
            storage.get_links("a.com", "app.t.c", ".abc.uri", 2, None, &HashSet::default())?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (4, 0),
                items: vec![
                    RecordId {
                        did: "did:plc:asdf-4".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                    RecordId {
                        did: "did:plc:asdf-3".into(),
                        collection: "app.t.c".into(),
                        rkey: "asdf".into(),
                    },
                ],
                next: Some(2),
                total: 4,
            }
        );
        storage.push(
            &ActionableEvent::DeactivateAccount("did:plc:asdf-1".into()),
            0,
        )?;
        let links = storage.get_links(
            "a.com",
            "app.t.c",
            ".abc.uri",
            2,
            links.next,
            &HashSet::default(),
        )?;
        assert_eq!(
            links,
            PagedAppendingCollection {
                version: (4, 0),
                items: vec![RecordId {
                    did: "did:plc:asdf-2".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },],
                next: None,
                total: 4,
            }
        );
        assert_stats(storage.get_stats()?, 4..=4, 1..=1, 4..=4);
    });

    test_each_storage!(get_all_counts, |storage| {
        storage.push(
            &ActionableEvent::CreateLinks {
                record_id: RecordId {
                    did: "did:plc:asdf".into(),
                    collection: "app.t.c".into(),
                    rkey: "asdf".into(),
                },
                links: vec![
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".abc.uri".into(),
                    },
                    CollectedLink {
                        target: Link::Uri("a.com".into()),
                        path: ".def.uri".into(),
                    },
                ],
            },
            0,
        )?;
        assert_eq!(storage.get_all_record_counts("a.com")?, {
            let mut counts = HashMap::new();
            let mut t_c_counts = HashMap::new();
            t_c_counts.insert(".abc.uri".into(), 1);
            t_c_counts.insert(".def.uri".into(), 1);
            counts.insert("app.t.c".into(), t_c_counts);
            counts
        });
        assert_eq!(storage.get_all_counts("a.com")?, {
            let mut counts = HashMap::new();
            let mut t_c_counts = HashMap::new();
            t_c_counts.insert(
                ".abc.uri".into(),
                CountsByCount {
                    records: 1,
                    distinct_dids: 1,
                },
            );
            t_c_counts.insert(
                ".def.uri".into(),
                CountsByCount {
                    records: 1,
                    distinct_dids: 1,
                },
            );
            counts.insert("app.t.c".into(), t_c_counts);
            counts
        });
        assert_stats(storage.get_stats()?, 1..=1, 2..=2, 1..=1);
    });
}
