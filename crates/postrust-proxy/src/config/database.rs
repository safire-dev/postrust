//! Database-backed configuration loading.

use crate::config::{Backend, HealthCheckConfig, LoadBalanceStrategy, Route, Upstream};
use crate::error::{ProxyError, ProxyResult};
use sqlx::PgPool;

/// Load routes from the database.
pub async fn load_from_database(pool: &PgPool) -> ProxyResult<(Vec<Route>, Vec<Upstream>)> {
    let routes = load_routes(pool).await?;
    let upstreams = load_upstreams(pool).await?;
    Ok((routes, upstreams))
}

async fn load_routes(pool: &PgPool) -> ProxyResult<Vec<Route>> {
    // TODO: Implement database query
    // For now, return empty
    Ok(Vec::new())
}

async fn load_upstreams(pool: &PgPool) -> ProxyResult<Vec<Upstream>> {
    // TODO: Implement database query
    // For now, return empty
    Ok(Vec::new())
}
