use super::{LinkReader, LinkStorage, PagedAppendingCollection, StorageStats};
use crate::{ActionableEvent, CountsByCount, Did, RecordId};
use anyhow::Result;
use links::CollectedLink;
use std::collections::{HashMap, HashSet};
use std::sync::{Arc, Mutex};

// hopefully-correct simple hashmap version, intended only for tests to verify disk impl
#[derive(Debug, Clone)]
pub struct MemStorage(Arc<Mutex<MemStorageData>>);

type Linkers = Vec<Option<(Did, RKey)>>; // optional because we replace with None for deleted links to keep cursors stable

#[derive(Debug, Default)]
struct MemStorageData {
    dids: HashMap<Did, bool>,                           // bool: active or nah
    targets: HashMap<Target, HashMap<Source, Linkers>>, // target -> (collection, path) -> (did, rkey)?[]
    links: HashMap<Did, HashMap<RepoId, Vec<(RecordPath, Target)>>>, // did -> collection:rkey -> (path, target)[]
}

impl MemStorage {
    pub fn new() -> Self {
        Self(Arc::new(Mutex::new(MemStorageData::default())))
    }

    fn add_links(&mut self, record_id: &RecordId, links: &[CollectedLink]) {
        let mut data = self.0.lock().unwrap();
        for link in links {
            data.dids.entry(record_id.did()).or_insert(true); // if they are inserting a link, presumably they are active
            data.targets
                .entry(Target::new(link.target.as_str()))
                .or_default()
                .entry(Source::new(&record_id.collection, &link.path))
                .or_default()
                .push(Some((record_id.did(), RKey(record_id.rkey()))));
            data.links
                .entry(record_id.did())
                .or_default()
                .entry(RepoId::from_record_id(record_id))
                .or_insert(Vec::with_capacity(1))
                .push((
                    RecordPath::new(&link.path),
                    Target::new(link.target.as_str()),
                ))
        }
    }

    fn remove_links(&mut self, record_id: &RecordId) {
        let mut data = self.0.lock().unwrap();
        let repo_id = RepoId::from_record_id(record_id);
        if let Some(Some(link_targets)) = data.links.get(&record_id.did).map(|cr| cr.get(&repo_id))
        {
            let link_targets = link_targets.clone(); // satisfy borrowck
            for (record_path, target) in link_targets {
                data.targets
                    .get_mut(&target)
                    .expect("must have the target if we have a link saved")
                    .get_mut(&Source::new(&record_id.collection, &record_path.0))
                    .expect("must have the target at this path if we have a link to it saved")
                    .iter_mut()
                    .rfind(|d| **d == Some((record_id.did(), RKey(record_id.rkey()))))
                    .expect("must be in dids list if we have a link to it")
                    .take();
            }
        }
        data.links
            .get_mut(&record_id.did)
            .map(|cr| cr.remove(&repo_id));
    }

    fn update_links(&mut self, record_id: &RecordId, new_links: &[CollectedLink]) {
        self.remove_links(record_id);
        self.add_links(record_id, new_links);
    }

    fn set_account(&mut self, did: &Did, active: bool) {
        let mut data = self.0.lock().unwrap();
        if let Some(account) = data.dids.get_mut(did) {
            *account = active;
        }
    }

    fn delete_account(&mut self, did: &Did) {
        let mut data = self.0.lock().unwrap();
        if let Some(links) = data.links.get(did) {
            let links = links.clone();
            for (repo_id, targets) in links {
                let targets = targets.clone();
                for (record_path, target) in targets {
                    data.targets
                        .get_mut(&target)
                        .expect("must have the target if we have a link saved")
                        .get_mut(&Source::new(&repo_id.collection, &record_path.0))
                        .expect("must have the target at this path if we have a link to it saved")
                        .iter_mut()
                        .find(|d| **d == Some((did.clone(), repo_id.rkey.clone())))
                        .expect("lkasjdlfkj")
                        .take();
                }
            }
        }
        data.links.remove(did); // nb: this is removing by a whole prefix in kv context
        data.dids.remove(did);
    }
}

impl Default for MemStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl LinkStorage for MemStorage {
    fn push(&mut self, event: &ActionableEvent, _cursor: u64) -> Result<()> {
        match event {
            ActionableEvent::CreateLinks { record_id, links } => self.add_links(record_id, links),
            ActionableEvent::UpdateLinks {
                record_id,
                new_links,
            } => self.update_links(record_id, new_links),
            ActionableEvent::DeleteRecord(record_id) => self.remove_links(record_id),
            ActionableEvent::ActivateAccount(did) => self.set_account(did, true),
            ActionableEvent::DeactivateAccount(did) => self.set_account(did, false),
            ActionableEvent::DeleteAccount(did) => self.delete_account(did),
        }
        Ok(())
    }

    fn to_readable(&mut self) -> impl LinkReader {
        self.clone()
    }
}

impl LinkReader for MemStorage {
    fn get_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(0);
        };
        let Some(linkers) = paths.get(&Source::new(collection, path)) else {
            return Ok(0);
        };
        Ok(linkers.iter().flatten().count() as u64)
    }

    fn get_distinct_did_count(&self, target: &str, collection: &str, path: &str) -> Result<u64> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(0);
        };
        let Some(linkers) = paths.get(&Source::new(collection, path)) else {
            return Ok(0);
        };
        Ok(linkers
            .iter()
            .flatten()
            .map(|(did, _)| did)
            .collect::<HashSet<_>>()
            .len() as u64)
    }

    fn get_links(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
        filter_dids: &HashSet<Did>,
    ) -> Result<PagedAppendingCollection<RecordId>> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(PagedAppendingCollection {
                version: (0, 0),
                items: Vec::new(),
                next: None,
                total: 0,
            });
        };
        let Some(did_rkeys) = paths.get(&Source::new(collection, path)) else {
            return Ok(PagedAppendingCollection {
                version: (0, 0),
                items: Vec::new(),
                next: None,
                total: 0,
            });
        };

        let did_rkeys: Vec<_> = if !filter_dids.is_empty() {
            did_rkeys
                .iter()
                .filter(|m| {
                    Option::<(Did, RKey)>::clone(m)
                        .map(|(did, _)| filter_dids.contains(&did))
                        .unwrap_or(false)
                })
                .cloned()
                .collect()
        } else {
            did_rkeys.to_vec()
        };

        let total = did_rkeys.len();
        let end = until
            .map(|u| std::cmp::min(u as usize, total))
            .unwrap_or(total);
        let begin = end.saturating_sub(limit as usize);
        let next = if begin == 0 { None } else { Some(begin as u64) };

        let alive = did_rkeys.iter().flatten().count();
        let gone = total - alive;

        let items: Vec<_> = did_rkeys[begin..end]
            .iter()
            .rev()
            .flatten()
            .filter(|(did, _)| *data.dids.get(did).expect("did must be in dids"))
            .map(|(did, rkey)| RecordId {
                did: did.clone(),
                rkey: rkey.0.clone(),
                collection: collection.to_string(),
            })
            .collect();

        Ok(PagedAppendingCollection {
            version: (total as u64, gone as u64),
            items,
            next,
            total: alive as u64,
        })
    }

    fn get_distinct_dids(
        &self,
        target: &str,
        collection: &str,
        path: &str,
        limit: u64,
        until: Option<u64>,
    ) -> Result<PagedAppendingCollection<Did>> {
        let data = self.0.lock().unwrap();
        let Some(paths) = data.targets.get(&Target::new(target)) else {
            return Ok(PagedAppendingCollection {
                version: (0, 0),
                items: Vec::new(),
                next: None,
                total: 0,
            });
        };
        let Some(did_rkeys) = paths.get(&Source::new(collection, path)) else {
            return Ok(PagedAppendingCollection {
                version: (0, 0),
                items: Vec::new(),
                next: None,
                total: 0,
            });
        };

        let dids: Vec<Option<Did>> = {
            let mut seen = HashSet::new();
            did_rkeys
                .iter()
                .map(|o| {
                    o.clone().and_then(|(did, _)| {
                        if seen.contains(&did) {
                            None
                        } else {
                            seen.insert(did.clone());
                            Some(did)
                        }
                    })
                })
                .collect()
        };

        let total = dids.len();
        let end = until
            .map(|u| std::cmp::min(u as usize, total))
            .unwrap_or(total);
        let begin = end.saturating_sub(limit as usize);
        let next = if begin == 0 { None } else { Some(begin as u64) };

        let alive = dids.iter().flatten().count();
        let gone = total - alive;

        let items: Vec<Did> = dids[begin..end]
            .iter()
            .rev()
            .flatten()
            .filter(|did| *data.dids.get(did).expect("did must be in dids"))
            .cloned()
            .collect();

        Ok(PagedAppendingCollection {
            version: (total as u64, gone as u64),
            items,
            next,
            total: alive as u64,
        })
    }

    fn get_all_record_counts(&self, target: &str) -> Result<HashMap<String, HashMap<String, u64>>> {
        let data = self.0.lock().unwrap();
        let mut out: HashMap<String, HashMap<String, u64>> = HashMap::new();
        if let Some(asdf) = data.targets.get(&Target::new(target)) {
            for (Source { collection, path }, linkers) in asdf {
                let count = linkers.iter().flatten().count() as u64;
                out.entry(collection.to_string())
                    .or_default()
                    .insert(path.to_string(), count);
            }
        }
        Ok(out)
    }

    fn get_all_counts(
        &self,
        target: &str,
    ) -> Result<HashMap<String, HashMap<String, CountsByCount>>> {
        let data = self.0.lock().unwrap();
        let mut out: HashMap<String, HashMap<String, CountsByCount>> = HashMap::new();
        if let Some(asdf) = data.targets.get(&Target::new(target)) {
            for (Source { collection, path }, linkers) in asdf {
                let records = linkers.iter().flatten().count() as u64;
                let distinct_dids = linkers
                    .iter()
                    .flatten()
                    .map(|(did, _)| did)
                    .collect::<HashSet<_>>()
                    .len() as u64;
                out.entry(collection.to_string()).or_default().insert(
                    path.to_string(),
                    CountsByCount {
                        records,
                        distinct_dids,
                    },
                );
            }
        }
        Ok(out)
    }

    fn get_stats(&self) -> Result<StorageStats> {
        let data = self.0.lock().unwrap();
        let dids = data.dids.len() as u64;
        let targetables = data
            .targets
            .values()
            .map(|sources| sources.len())
            .sum::<usize>() as u64;
        let linking_records = data.links.values().map(|recs| recs.len()).sum::<usize>() as u64;
        Ok(StorageStats {
            dids,
            targetables,
            linking_records,
        })
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Target(String);

impl Target {
    fn new(t: &str) -> Self {
        Self(t.into())
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct Source {
    collection: String,
    path: String,
}

impl Source {
    fn new(collection: &str, path: &str) -> Self {
        Self {
            collection: collection.into(),
            path: path.into(),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RKey(String);

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RepoId {
    collection: String,
    rkey: RKey,
}

impl RepoId {
    fn from_record_id(record_id: &RecordId) -> Self {
        Self {
            collection: record_id.collection.clone(),
            rkey: RKey(record_id.rkey.clone()),
        }
    }
}

#[derive(Debug, PartialEq, Hash, Eq, Clone)]
struct RecordPath(String);

impl RecordPath {
    fn new(rp: &str) -> Self {
        Self(rp.into())
    }
}
