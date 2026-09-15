//! Regression tests for GraphQL JSON numbers and SQLx statement caching.
//!
//! These tests require a running PostgreSQL database.
//! Run with:
//! `cargo test --package postrust-graphql --test numeric_binding_integration -- --ignored`

use async_graphql::{Request, Variables};
use postrust_auth::AuthResult;
use postrust_core::schema_cache::{SchemaCache, SchemaCacheRef};
use postrust_graphql::context::GraphQLContext;
use postrust_graphql::handler::GraphQLState;
use postrust_graphql::schema::{MutationType, SchemaConfig};
use serde_json::json;
use sqlx::postgres::PgPoolOptions;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

static TABLE_COUNTER: AtomicU32 = AtomicU32::new(0);

fn database_url() -> String {
    std::env::var("DATABASE_URL")
        .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/postrust_test".to_string())
}

fn unique_table_name() -> String {
    let id = TABLE_COUNTER.fetch_add(1, Ordering::SeqCst);
    let timestamp = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system clock is before Unix epoch")
        .as_millis();
    format!("postrust_numeric_binding_{timestamp}_{id}")
}

#[tokio::test]
#[ignore = "requires a running PostgreSQL database"]
async fn graphql_variables_keep_float_type_after_integer_value() {
    // One connection makes prepared-statement reuse deterministic.
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database_url())
        .await
        .expect("failed to connect to PostgreSQL");

    let table_name = unique_table_name();
    let qualified_table = format!("public.{table_name}");

    sqlx::query(&format!(
        "CREATE TABLE {qualified_table} (\
            id BIGSERIAL PRIMARY KEY, \
            value DOUBLE PRECISION NOT NULL, \
            optional_value DOUBLE PRECISION, \
            whole_value BIGINT\
        )"
    ))
    .execute(&pool)
    .await
    .expect("failed to create regression-test table");

    let schema_cache = SchemaCache::load(&pool, &["public".to_string()])
        .await
        .expect("failed to load schema cache");
    let state = GraphQLState::new(
        pool.clone(),
        Arc::new(schema_cache.clone()),
        SchemaConfig::default(),
    )
    .expect("failed to build GraphQL state");

    let mutation_name = state
        .generated_schema
        .mutation_fields
        .iter()
        .find(|field| field.table_name == table_name && field.mutation_type == MutationType::Insert)
        .expect("generated insert mutation was not found")
        .name
        .clone();
    let mutation = format!(
        "mutation Insert($objects: [JSON!]) {{ {mutation_name}(objects: $objects) {{ value }} }}"
    );
    let update_name = state
        .generated_schema
        .mutation_fields
        .iter()
        .find(|field| field.table_name == table_name && field.mutation_type == MutationType::Update)
        .expect("generated update mutation was not found")
        .name
        .clone();
    let update_mutation = format!(
        "mutation Update($where: JSON, $set: JSON!) {{ {update_name}(where: $where, set: $set) {{ value }} }}"
    );
    let count_name = state
        .generated_schema
        .query_fields
        .iter()
        .find(|field| field.table_name == table_name && field.is_count)
        .expect("generated count query was not found")
        .name
        .clone();
    let count_query = format!("query Count($filter: JSON) {{ {count_name}(filter: $filter) }}");

    let database_role: String = sqlx::query_scalar("SELECT current_user")
        .fetch_one(&pool)
        .await
        .expect("failed to determine database role");

    let build_insert_request = |value: serde_json::Value| {
        Request::new(mutation.clone())
            .variables(Variables::from_json(json!({
                "objects": [{ "value": value }]
            })))
            .data(GraphQLContext::new(
                pool.clone(),
                SchemaCacheRef::from_static(schema_cache.clone()),
                AuthResult {
                    role: database_role.clone(),
                    claims: HashMap::new(),
                },
            ))
            .data(pool.clone())
    };

    let add_context = |request: Request| {
        request
            .data(GraphQLContext::new(
                pool.clone(),
                SchemaCacheRef::from_static(schema_cache.clone()),
                AuthResult {
                    role: database_role.clone(),
                    claims: HashMap::new(),
                },
            ))
            .data(pool.clone())
    };

    // The first request causes SQLx to cache the INSERT with an INT8 parameter.
    let integer_response = state.schema.execute(build_insert_request(json!(12))).await;
    // The same SQL is then executed with a FLOAT8 parameter.
    let fractional_response = state
        .schema
        .execute(build_insert_request(json!(12.5)))
        .await;

    let stored_values: Vec<f64> =
        sqlx::query_scalar(&format!("SELECT value FROM {qualified_table} ORDER BY id"))
            .fetch_all(&pool)
            .await
            .expect("failed to read inserted values");

    // Filters must also keep the column's FLOAT8 type when the JSON value
    // changes from an integer to a fraction.
    let integer_count = state
        .schema
        .execute(add_context(Request::new(count_query.clone()).variables(
            Variables::from_json(json!({
                "filter": { "value": { "eq": 12 } }
            })),
        )))
        .await;
    let fractional_count = state
        .schema
        .execute(add_context(Request::new(count_query).variables(
            Variables::from_json(json!({
                "filter": { "value": { "eq": 12.5 } }
            })),
        )))
        .await;

    // Updates exercise a statement with both a FLOAT8 SET parameter and an
    // INT8 primary-key filter parameter.
    let integer_update = state
        .schema
        .execute(add_context(
            Request::new(update_mutation.clone()).variables(Variables::from_json(json!({
                "where": { "id": { "eq": 1 } },
                "set": { "value": 20 }
            }))),
        ))
        .await;
    let fractional_update = state
        .schema
        .execute(add_context(Request::new(update_mutation).variables(
            Variables::from_json(json!({
                "where": { "id": { "eq": 1 } },
                "set": { "value": 20.5 }
            })),
        )))
        .await;
    let updated_value: f64 =
        sqlx::query_scalar(&format!("SELECT value FROM {qualified_table} WHERE id = 1"))
            .fetch_one(&pool)
            .await
            .expect("failed to read updated value");

    // Nulls must use the nullable column's FLOAT8 parameter type rather than
    // the previous untyped String fallback.
    let null_response = state
        .schema
        .execute(add_context(Request::new(mutation.clone()).variables(
            Variables::from_json(json!({
                "objects": [{ "value": 30, "optional_value": null }]
            })),
        )))
        .await;
    let value_after_null = state
        .schema
        .execute(add_context(Request::new(mutation.clone()).variables(
            Variables::from_json(json!({
                "objects": [{ "value": 31, "optional_value": 1.5 }]
            })),
        )))
        .await;
    let optional_values: Vec<Option<f64>> = sqlx::query_scalar(&format!(
        "SELECT optional_value FROM {qualified_table} WHERE value IN (30, 31) ORDER BY value"
    ))
    .fetch_all(&pool)
    .await
    .expect("failed to read nullable values");

    // Fractional input for an integer column must fail rather than being
    // truncated or rebound under a different parameter type.
    let invalid_integer_response = state
        .schema
        .execute(add_context(Request::new(mutation).variables(
            Variables::from_json(json!({
                "objects": [{ "value": 40, "whole_value": 12.5 }]
            })),
        )))
        .await;

    sqlx::query(&format!("DROP TABLE {qualified_table}"))
        .execute(&pool)
        .await
        .expect("failed to drop regression-test table");

    assert!(
        integer_response.errors.is_empty(),
        "integer insert failed: {:?}",
        integer_response.errors
    );
    assert!(
        fractional_response.errors.is_empty(),
        "fractional insert failed: {:?}",
        fractional_response.errors
    );
    assert_eq!(stored_values, vec![12.0, 12.5]);
    assert!(
        integer_count.errors.is_empty(),
        "integer filter failed: {:?}",
        integer_count.errors
    );
    assert!(
        fractional_count.errors.is_empty(),
        "fractional filter failed: {:?}",
        fractional_count.errors
    );
    assert_eq!(
        serde_json::to_value(integer_count.data).unwrap()[&count_name],
        1
    );
    assert_eq!(
        serde_json::to_value(fractional_count.data).unwrap()[&count_name],
        1
    );
    assert!(
        integer_update.errors.is_empty(),
        "integer update failed: {:?}",
        integer_update.errors
    );
    assert!(
        fractional_update.errors.is_empty(),
        "fractional update failed: {:?}",
        fractional_update.errors
    );
    assert_eq!(updated_value, 20.5);
    assert!(
        null_response.errors.is_empty(),
        "null insert failed: {:?}",
        null_response.errors
    );
    assert!(
        value_after_null.errors.is_empty(),
        "post-null insert failed: {:?}",
        value_after_null.errors
    );
    assert_eq!(optional_values, vec![None, Some(1.5)]);
    assert!(!invalid_integer_response.errors.is_empty());
}
