use crate::storage::StoreReader;
use crate::{ConsumerInfo, Nsid, TopCollections, UFOsRecord};
use dropshot::endpoint;
use dropshot::ApiDescription;
use dropshot::ConfigDropshot;
use dropshot::ConfigLogging;
use dropshot::ConfigLoggingLevel;
use dropshot::HttpError;
use dropshot::HttpResponseHeaders;
use dropshot::HttpResponseOk;
use dropshot::Query;
use dropshot::RequestContext;
use dropshot::ServerBuilder;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

struct Context {
    pub spec: Arc<serde_json::Value>,
    storage: Box<dyn StoreReader>,
}

/// Meta: get the openapi spec for this api
#[endpoint {
    method = GET,
    path = "/openapi",
}]
async fn get_openapi(ctx: RequestContext<Context>) -> OkCorsResponse<serde_json::Value> {
    let spec = (*ctx.context().spec).clone();
    ok_cors(spec)
}

#[derive(Debug, Serialize, JsonSchema)]
struct MetaInfo {
    storage: serde_json::Value,
    consumer: ConsumerInfo,
}
/// Get meta information about UFOs itself
#[endpoint {
    method = GET,
    path = "/meta"
}]
async fn get_meta_info(ctx: RequestContext<Context>) -> OkCorsResponse<MetaInfo> {
    let Context { storage, .. } = ctx.context();
    let failed_to_get =
        |what| move |e| HttpError::for_internal_error(format!("failed to get {what}: {e:?}"));

    let storage_info = storage
        .get_storage_stats()
        .await
        .map_err(failed_to_get("storage info"))?;

    let consumer = storage
        .get_consumer_info()
        .await
        .map_err(failed_to_get("consumer info"))?;

    ok_cors(MetaInfo {
        storage: storage_info,
        consumer,
    })
}

#[derive(Debug, Deserialize, JsonSchema)]
struct CollectionsQuery {
    collection: String, // JsonSchema not implemented for Nsid :(
}
impl CollectionsQuery {
    fn to_multiple_nsids(&self) -> Result<Vec<Nsid>, String> {
        let mut out = Vec::with_capacity(self.collection.len());
        for collection in self.collection.split(',') {
            let Ok(nsid) = Nsid::new(collection.to_string()) else {
                return Err(format!("collection {collection:?} was not a valid NSID"));
            };
            out.push(nsid);
        }
        Ok(out)
    }
}
#[derive(Debug, Serialize, JsonSchema)]
struct ApiRecord {
    did: String,
    collection: String,
    rkey: String,
    record: Box<serde_json::value::RawValue>,
    time_us: u64,
}
impl From<UFOsRecord> for ApiRecord {
    fn from(ufo: UFOsRecord) -> Self {
        Self {
            did: ufo.did.to_string(),
            collection: ufo.collection.to_string(),
            rkey: ufo.rkey.to_string(),
            record: ufo.record,
            time_us: ufo.cursor.to_raw_u64(),
        }
    }
}
/// Get recent records by collection
///
/// Multiple collections are supported. they will be delivered in one big array with no
/// specified order.
#[endpoint {
    method = GET,
    path = "/records",
}]
async fn get_records_by_collection(
    ctx: RequestContext<Context>,
    collection_query: Query<CollectionsQuery>,
) -> OkCorsResponse<Vec<ApiRecord>> {
    let Context { storage, .. } = ctx.context();

    let collections = collection_query
        .into_inner()
        .to_multiple_nsids()
        .map_err(|reason| HttpError::for_bad_request(None, reason))?;

    let records = storage
        .get_records_by_collections(&collections, 100)
        .await
        .map_err(|e| HttpError::for_internal_error(e.to_string()))?
        .into_iter()
        .map(|r| r.into())
        .collect();

    ok_cors(records)
}

#[derive(Debug, Serialize, JsonSchema)]
struct TotalCounts {
    total_records: u64,
    dids_estimate: u64,
}
/// Get total records seen by collection
#[endpoint {
    method = GET,
    path = "/records/total-seen"
}]
async fn get_records_total_seen(
    ctx: RequestContext<Context>,
    collection_query: Query<CollectionsQuery>,
) -> OkCorsResponse<HashMap<String, TotalCounts>> {
    let Context { storage, .. } = ctx.context();

    let collections = collection_query
        .into_inner()
        .to_multiple_nsids()
        .map_err(|reason| HttpError::for_bad_request(None, reason))?;

    let mut seen_by_collection = HashMap::with_capacity(collections.len());

    for collection in &collections {
        let (total_records, dids_estimate) = storage
            .get_counts_by_collection(collection)
            .await
            .map_err(|e| HttpError::for_internal_error(format!("boooo: {e:?}")))?;

        seen_by_collection.insert(
            collection.to_string(),
            TotalCounts {
                total_records,
                dids_estimate,
            },
        );
    }

    ok_cors(seen_by_collection)
}

/// Get top collections
#[endpoint {
    method = GET,
    path = "/collections"
}]
async fn get_top_collections(ctx: RequestContext<Context>) -> OkCorsResponse<TopCollections> {
    let Context { storage, .. } = ctx.context();
    let collections = storage
        .get_top_collections()
        .await
        .map_err(|e| HttpError::for_internal_error(format!("boooo: {e:?}")))?;

    ok_cors(collections)
}

pub async fn serve(storage: impl StoreReader + 'static) -> Result<(), String> {
    let log = ConfigLogging::StderrTerminal {
        level: ConfigLoggingLevel::Info,
    }
    .to_logger("hello-ufos")
    .map_err(|e| e.to_string())?;

    let mut api = ApiDescription::new();

    api.register(get_openapi).unwrap();
    api.register(get_meta_info).unwrap();
    api.register(get_records_by_collection).unwrap();
    api.register(get_records_total_seen).unwrap();
    api.register(get_top_collections).unwrap();

    let context = Context {
        spec: Arc::new(
            api.openapi("UFOs", semver::Version::new(0, 0, 0))
                .json()
                .map_err(|e| e.to_string())?,
        ),
        storage: Box::new(storage),
    };

    ServerBuilder::new(api, context, log)
        .config(ConfigDropshot {
            bind_address: "0.0.0.0:9999".parse().unwrap(),
            ..Default::default()
        })
        .start()
        .map_err(|error| format!("failed to start server: {}", error))?
        .await
}

/// awkward helpers
type OkCorsResponse<T> = Result<HttpResponseHeaders<HttpResponseOk<T>>, HttpError>;
fn ok_cors<T: Send + Sync + Serialize + JsonSchema>(t: T) -> OkCorsResponse<T> {
    let mut res = HttpResponseHeaders::new_unnamed(HttpResponseOk(t));
    res.headers_mut()
        .insert("access-control-allow-origin", "*".parse().unwrap());
    Ok(res)
}
