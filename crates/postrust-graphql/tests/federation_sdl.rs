//! Verifies that enabling federation makes Postrust expose a Federation v2
//! subgraph: the `_service { sdl }` field must be present and the emitted SDL
//! must carry the `@link` schema directive that Apollo Router requires to
//! treat the subgraph as Federation v2.
//!
//! Runs as an integration test (separate crate) so it exercises only the public
//! API and does not depend on the crate's in-module unit tests.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use std::collections::HashMap as StdHashMap;

use indexmap::IndexMap;
use postrust_auth::AuthResult;
use postrust_core::schema_cache::{Column, SchemaCache, SchemaCacheRef, Table};
use postrust_graphql::context::GraphQLContext;
use postrust_graphql::handler::GraphQLState;
use postrust_graphql::schema::SchemaConfig;
use sqlx::postgres::PgPoolOptions;
use sqlx::PgPool;

fn users_table() -> Table {
    let mut columns = IndexMap::new();
    columns.insert(
        "id".into(),
        Column {
            name: "id".into(),
            description: None,
            nullable: false,
            data_type: "uuid".into(),
            nominal_type: "uuid".into(),
            max_len: None,
            default: Some("gen_random_uuid()".into()),
            enum_values: vec![],
            is_pk: true,
            position: 1,
        },
    );
    columns.insert(
        "name".into(),
        Column {
            name: "name".into(),
            description: None,
            nullable: false,
            data_type: "text".into(),
            nominal_type: "text".into(),
            max_len: None,
            default: None,
            enum_values: vec![],
            is_pk: false,
            position: 2,
        },
    );

    Table {
        schema: "public".into(),
        name: "users".into(),
        description: None,
        is_view: false,
        insertable: true,
        updatable: true,
        deletable: true,
        pk_cols: vec!["id".into()],
        columns,
    }
}

/// A second table that is *not* a shared entity, used to check namespacing.
fn orders_table() -> Table {
    let mut columns = IndexMap::new();
    columns.insert(
        "id".into(),
        Column {
            name: "id".into(),
            description: None,
            nullable: false,
            data_type: "uuid".into(),
            nominal_type: "uuid".into(),
            max_len: None,
            default: Some("gen_random_uuid()".into()),
            enum_values: vec![],
            is_pk: true,
            position: 1,
        },
    );
    columns.insert(
        "amount".into(),
        Column {
            name: "amount".into(),
            description: None,
            nullable: false,
            data_type: "integer".into(),
            nominal_type: "int4".into(),
            max_len: None,
            default: None,
            enum_values: vec![],
            is_pk: false,
            position: 2,
        },
    );

    Table {
        schema: "public".into(),
        name: "orders".into(),
        description: None,
        is_view: false,
        insertable: true,
        updatable: true,
        deletable: true,
        pk_cols: vec!["id".into()],
        columns,
    }
}

fn cache_with(tables_vec: Vec<Table>) -> SchemaCache {
    let mut tables = HashMap::new();
    for t in tables_vec {
        tables.insert(t.qualified_identifier(), t);
    }

    SchemaCache {
        tables,
        relationships: HashMap::new(),
        routines: HashMap::new(),
        timezones: HashSet::new(),
        pg_version: 150000,
    }
}

fn schema_cache() -> SchemaCache {
    cache_with(vec![users_table()])
}

/// A pool that never opens a socket. Fine for schema/SDL/introspection work and
/// for resolver paths that error before touching the database.
fn lazy_pool() -> PgPool {
    PgPoolOptions::new()
        .connect_lazy("postgres://postgres@localhost/postgres")
        .expect("lazy pool should construct from a valid URL")
}

async fn sdl_for(cache: SchemaCache, config: SchemaConfig) -> String {
    let state = GraphQLState::new(lazy_pool(), Arc::new(cache), config)
        .expect("federated GraphQL schema should build");

    let res = state.schema.execute("{ _service { sdl } }").await;
    assert!(
        res.errors.is_empty(),
        "_service query errored: {:?}",
        res.errors
    );

    let json = res
        .data
        .into_json()
        .expect("response data should serialize to JSON");
    json["_service"]["sdl"]
        .as_str()
        .expect("_service.sdl should be a string")
        .to_string()
}

#[tokio::test]
async fn federation_sdl_is_emitted_as_v2() {
    // `connect_lazy` never opens a socket; `_service { sdl }` resolves purely
    // from the schema and never touches the pool.
    let sdl = sdl_for(
        schema_cache(),
        SchemaConfig::default().with_federation(true),
    )
    .await;
    eprintln!("---FEDERATION SDL---\n{sdl}\n---END SDL---");

    assert!(
        sdl.contains("@link"),
        "SDL is missing the @link directive (not Federation v2):\n{sdl}"
    );

    // The `users` table has a single-column primary key `id`, so its generated
    // type must be a federation entity keyed on that column.
    assert!(
        sdl.contains("@key(fields: \"id\")"),
        "SDL is missing @key on the entity type:\n{sdl}"
    );
}

#[tokio::test]
async fn type_prefix_namespaces_non_shared_but_shares_entities() {
    let config = SchemaConfig::default()
        .with_federation(true)
        .with_type_prefix(Some("Snowflake".into()))
        .with_shared_entities(vec!["users".into()]);
    let sdl = sdl_for(cache_with(vec![users_table(), orders_table()]), config).await;
    eprintln!("---FEDERATION SDL (prefixed)---\n{sdl}\n---END SDL---");

    // Non-shared table is namespaced with the prefix.
    assert!(
        sdl.contains("type SnowflakeOrders"),
        "orders type not namespaced:\n{sdl}"
    );

    // Shared entity keeps its bare name and stays a keyed entity.
    assert!(
        sdl.contains("type Users @key(fields: \"id\")"),
        "shared entity should keep its bare name + key:\n{sdl}"
    );

    // Root fields are namespaced for both tables so two subgraphs never collide.
    assert!(
        sdl.contains("snowflakeOrders"),
        "orders root field not namespaced:\n{sdl}"
    );
    assert!(
        sdl.contains("snowflakeUsers"),
        "users root field not namespaced:\n{sdl}"
    );

    // The shared entity's non-key field is @shareable (key fields are not).
    assert!(
        sdl.contains("name: String! @shareable"),
        "shared entity non-key field should be @shareable:\n{sdl}"
    );
}

#[tokio::test]
async fn keyed_types_are_federation_entities() {
    let state = GraphQLState::new(
        lazy_pool(),
        Arc::new(cache_with(vec![users_table(), orders_table()])),
        SchemaConfig::default().with_federation(true),
    )
    .expect("federated GraphQL schema should build");

    // Both keyed tables must be members of the `_Entity` union.
    let res = state
        .schema
        .execute(r#"{ __type(name: "_Entity") { possibleTypes { name } } }"#)
        .await;
    assert!(
        res.errors.is_empty(),
        "introspection errored: {:?}",
        res.errors
    );
    let json = res.data.into_json().unwrap();
    let names: Vec<String> = json["__type"]["possibleTypes"]
        .as_array()
        .expect("_Entity should be a union with possibleTypes")
        .iter()
        .map(|t| t["name"].as_str().unwrap().to_string())
        .collect();
    assert!(
        names.contains(&"Users".to_string()),
        "_Entity missing Users: {names:?}"
    );
    assert!(
        names.contains(&"Orders".to_string()),
        "_Entity missing Orders: {names:?}"
    );

    // The `_entities` root field must be present.
    let res = state
        .schema
        .execute(r#"{ __schema { queryType { fields { name } } } }"#)
        .await;
    let json = res.data.into_json().unwrap();
    let has_entities = json["__schema"]["queryType"]["fields"]
        .as_array()
        .unwrap()
        .iter()
        .any(|f| f["name"].as_str() == Some("_entities"));
    assert!(has_entities, "_entities root field is missing");
}

#[tokio::test]
async fn entity_resolver_rejects_unknown_typename() {
    let pool = lazy_pool();
    let state = GraphQLState::new(
        pool.clone(),
        Arc::new(schema_cache()),
        SchemaConfig::default().with_federation(true),
    )
    .expect("federated GraphQL schema should build");

    // A representation whose __typename isn't a known entity must be rejected
    // before any database access — so a lazy (never-connected) pool is fine.
    let ctx = GraphQLContext::new(
        pool.clone(),
        SchemaCacheRef::new(),
        AuthResult {
            role: "anon".into(),
            claims: StdHashMap::new(),
        },
    );
    let request = async_graphql::Request::new(
        r#"{ _entities(representations: [{ __typename: "Nonexistent", id: "x" }]) { __typename } }"#,
    )
    .data(pool)
    .data(ctx);

    let res = state.schema.execute(request).await;
    assert!(
        res.errors
            .iter()
            .any(|e| e.message.contains("unknown federation entity type")),
        "expected an unknown-entity error, got: {:?}",
        res.errors
    );
}
