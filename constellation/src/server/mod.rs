use askama::Template;
use axum::{
    extract::{Query, Request},
    http::{self, header},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::get,
    Router,
};
use axum_metrics::{ExtraMetricLabels, MetricLayer};
use bincode::Options;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, UNIX_EPOCH};
use tokio::net::{TcpListener, ToSocketAddrs};
use tokio::task::block_in_place;
use tokio_util::sync::CancellationToken;

use crate::storage::{LinkReader, StorageStats};
use crate::{CountsByCount, Did, RecordId};

mod acceptable;
mod filters;

use acceptable::{acceptable, ExtractAccept};

const DEFAULT_CURSOR_LIMIT: u64 = 16;
const DEFAULT_CURSOR_LIMIT_MAX: u64 = 100;

const INDEX_BEGAN_AT_TS: u64 = 1738083600; // TODO: not this

pub async fn serve<S, A>(store: S, addr: A, stay_alive: CancellationToken) -> anyhow::Result<()>
where
    S: LinkReader,
    A: ToSocketAddrs,
{
    let app = Router::new()
        .route("/robots.txt", get(robots))
        .route(
            "/",
            get({
                let store = store.clone();
                move |accept| async { block_in_place(|| hello(accept, store)) }
            }),
        )
        .route(
            "/links/count",
            get({
                let store = store.clone();
                move |accept, query| async { block_in_place(|| count_links(accept, query, store)) }
            }),
        )
        .route(
            "/links/count/distinct-dids",
            get({
                let store = store.clone();
                move |accept, query| async {
                    block_in_place(|| count_distinct_dids(accept, query, store))
                }
            }),
        )
        .route(
            "/links",
            get({
                let store = store.clone();
                move |accept, query| async { block_in_place(|| get_links(accept, query, store)) }
            }),
        )
        .route(
            "/links/distinct-dids",
            get({
                let store = store.clone();
                move |accept, query| async {
                    block_in_place(|| get_distinct_dids(accept, query, store))
                }
            }),
        )
        .route(
            // deprecated
            "/links/all/count",
            get({
                let store = store.clone();
                move |accept, query| async {
                    block_in_place(|| count_all_links(accept, query, store))
                }
            }),
        )
        .route(
            "/links/all",
            get({
                let store = store.clone();
                move |accept, query| async {
                    block_in_place(|| explore_links(accept, query, store))
                }
            }),
        )
        .layer(tower_http::cors::CorsLayer::permissive())
        .layer(middleware::from_fn(add_lables))
        .layer(MetricLayer::default());

    let listener = TcpListener::bind(addr).await?;
    println!("api: listening at http://{:?}", listener.local_addr()?);
    axum::serve(listener, app)
        .with_graceful_shutdown(async move { stay_alive.cancelled().await })
        .await?;

    Ok(())
}

async fn add_lables(request: Request, next: Next) -> Response {
    let origin = request
        .headers()
        .get(header::ORIGIN)
        .and_then(|o| o.to_str().map(|v| v.to_owned()).ok());
    let user_agent = request.headers().get(header::USER_AGENT).and_then(|ua| {
        ua.to_str()
            .map(|v| {
                if v.starts_with("Mozilla/") {
                    "Mozilla/...".into()
                } else {
                    v.to_owned()
                }
            })
            .ok()
    });

    let mut res = next.run(request).await;

    let mut labels = Vec::new();
    if let Some(o) = origin {
        labels.push(metrics::Label::new("origin", o));
    }
    if let Some(ua) = user_agent {
        labels.push(metrics::Label::new("user_agent", ua));
    }
    res.extensions_mut().insert(ExtraMetricLabels(labels));
    res
}

async fn robots() -> &'static str {
    "\
User-agent: *
Disallow: /links
Disallow: /links/
    "
}

#[derive(Template, Serialize, Deserialize)]
#[template(path = "hello.html.j2")]
struct HelloReponse {
    help: &'static str,
    days_indexed: u64,
    stats: StorageStats,
}
fn hello(
    accept: ExtractAccept,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let stats = store
        .get_stats()
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    let days_indexed = (UNIX_EPOCH + Duration::from_secs(INDEX_BEGAN_AT_TS))
        .elapsed()
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?
        .as_secs()
        / 86400;
    Ok(acceptable(accept, HelloReponse {
        help: "open this URL in a web browser (or request with Accept: text/html) for information about this API.",
        days_indexed,
        stats,
    }))
}

#[derive(Clone, Deserialize)]
struct GetLinksCountQuery {
    target: String,
    collection: String,
    path: String,
}
#[derive(Template, Serialize)]
#[template(path = "links-count.html.j2")]
struct GetLinksCountResponse {
    total: u64,
    #[serde(skip_serializing)]
    query: GetLinksCountQuery,
}
fn count_links(
    accept: ExtractAccept,
    query: Query<GetLinksCountQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let total = store
        .get_count(&query.target, &query.collection, &query.path)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        GetLinksCountResponse {
            total,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetDidsCountQuery {
    target: String,
    collection: String,
    path: String,
}
#[derive(Template, Serialize)]
#[template(path = "dids-count.html.j2")]
struct GetDidsCountResponse {
    total: u64,
    #[serde(skip_serializing)]
    query: GetDidsCountQuery,
}
fn count_distinct_dids(
    accept: ExtractAccept,
    query: Query<GetDidsCountQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let total = store
        .get_distinct_did_count(&query.target, &query.collection, &query.path)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        GetDidsCountResponse {
            total,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetLinkItemsQuery {
    target: String,
    collection: String,
    path: String,
    cursor: Option<OpaqueApiCursor>,
    from_dids: Option<String>, // comma separated: gross
    limit: Option<u64>,
    // TODO: allow reverse (er, forward) order as well
}
#[derive(Template, Serialize)]
#[template(path = "links.html.j2")]
struct GetLinkItemsResponse {
    // what does staleness mean?
    // - new links have appeared. would be nice to offer a `since` cursor to fetch these. and/or,
    // - links have been deleted. hmm.
    total: u64,
    linking_records: Vec<RecordId>,
    cursor: Option<OpaqueApiCursor>,
    #[serde(skip_serializing)]
    query: GetLinkItemsQuery,
}
fn get_links(
    accept: ExtractAccept,
    query: Query<GetLinkItemsQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let until = query
        .cursor
        .clone()
        .map(|oc| ApiCursor::try_from(oc).map_err(|_| http::StatusCode::BAD_REQUEST))
        .transpose()?
        .map(|c| c.next);

    let limit = query.limit.unwrap_or(DEFAULT_CURSOR_LIMIT);
    if limit > DEFAULT_CURSOR_LIMIT_MAX {
        return Err(http::StatusCode::BAD_REQUEST);
    }

    let filter_dids = &query
        .from_dids
        .clone()
        .map(|comma_joined| HashSet::from_iter(comma_joined.split(',').map(|d| Did(d.to_string()))))
        .unwrap_or_default();

    let paged = store
        .get_links(
            &query.target,
            &query.collection,
            &query.path,
            limit,
            until,
            filter_dids,
        )
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let cursor = paged.next.map(|next| {
        ApiCursor {
            version: paged.version,
            next,
        }
        .into()
    });

    Ok(acceptable(
        accept,
        GetLinkItemsResponse {
            total: paged.total,
            linking_records: paged.items,
            cursor,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetDidItemsQuery {
    target: String,
    collection: String,
    path: String,
    cursor: Option<OpaqueApiCursor>,
    limit: Option<u64>,
    // TODO: allow reverse (er, forward) order as well
}
#[derive(Template, Serialize)]
#[template(path = "dids.html.j2")]
struct GetDidItemsResponse {
    // what does staleness mean?
    // - new links have appeared. would be nice to offer a `since` cursor to fetch these. and/or,
    // - links have been deleted. hmm.
    total: u64,
    linking_dids: Vec<Did>,
    cursor: Option<OpaqueApiCursor>,
    #[serde(skip_serializing)]
    query: GetDidItemsQuery,
}
fn get_distinct_dids(
    accept: ExtractAccept,
    query: Query<GetDidItemsQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let until = query
        .cursor
        .clone()
        .map(|oc| ApiCursor::try_from(oc).map_err(|_| http::StatusCode::BAD_REQUEST))
        .transpose()?
        .map(|c| c.next);

    let limit = query.limit.unwrap_or(DEFAULT_CURSOR_LIMIT);
    if limit > DEFAULT_CURSOR_LIMIT_MAX {
        return Err(http::StatusCode::BAD_REQUEST);
    }

    let paged = store
        .get_distinct_dids(&query.target, &query.collection, &query.path, limit, until)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;

    let cursor = paged.next.map(|next| {
        ApiCursor {
            version: paged.version,
            next,
        }
        .into()
    });

    Ok(acceptable(
        accept,
        GetDidItemsResponse {
            total: paged.total,
            linking_dids: paged.items,
            cursor,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct GetAllLinksQuery {
    target: String,
}
#[derive(Template, Serialize)]
#[template(path = "links-all-count.html.j2")]
struct GetAllLinksResponse {
    links: HashMap<String, HashMap<String, u64>>,
    #[serde(skip_serializing)]
    query: GetAllLinksQuery,
}
fn count_all_links(
    accept: ExtractAccept,
    query: Query<GetAllLinksQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let links = store
        .get_all_record_counts(&query.target)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        GetAllLinksResponse {
            links,
            query: (*query).clone(),
        },
    ))
}

#[derive(Clone, Deserialize)]
struct ExploreLinksQuery {
    target: String,
}
#[derive(Template, Serialize)]
#[template(path = "explore-links.html.j2")]
struct ExploreLinksResponse {
    links: HashMap<String, HashMap<String, CountsByCount>>,
    #[serde(skip_serializing)]
    query: ExploreLinksQuery,
}
fn explore_links(
    accept: ExtractAccept,
    query: Query<ExploreLinksQuery>,
    store: impl LinkReader,
) -> Result<impl IntoResponse, http::StatusCode> {
    let links = store
        .get_all_counts(&query.target)
        .map_err(|_| http::StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(acceptable(
        accept,
        ExploreLinksResponse {
            links,
            query: (*query).clone(),
        },
    ))
}

#[serde_as]
#[derive(Clone, Serialize, Deserialize)] // for json
struct OpaqueApiCursor(#[serde_as(as = "serde_with::hex::Hex")] Vec<u8>);

#[derive(Serialize, Deserialize)] // for bincode
struct ApiCursor {
    version: (u64, u64), // (collection length, deleted item count)
    next: u64,
}

impl TryFrom<OpaqueApiCursor> for ApiCursor {
    type Error = bincode::Error;

    fn try_from(item: OpaqueApiCursor) -> Result<Self, Self::Error> {
        bincode::DefaultOptions::new().deserialize(&item.0)
    }
}

impl From<ApiCursor> for OpaqueApiCursor {
    fn from(item: ApiCursor) -> Self {
        OpaqueApiCursor(bincode::DefaultOptions::new().serialize(&item).unwrap())
    }
}
