//! Axum handler for the /graphql endpoint.
//!
//! Provides GraphQL request handling using async-graphql with dynamic schema
//! generation from the PostgreSQL schema cache.

use crate::context::GraphQLContext;
use crate::error::GraphQLError;
use crate::schema::object::TableObjectType;
use crate::schema::relationship::RelationshipField;
use crate::schema::{build_schema, GeneratedSchema, MutationType, SchemaConfig};
use crate::subscription::{
    generate_subscription_fields, NotifyBroker, SubscriptionField as SubField, TableChangePayload,
};
use async_graphql::dynamic::*;
use async_graphql::Value;
use async_graphql_axum::{GraphQLRequest, GraphQLResponse};
use axum::extract::State;
use axum::response::IntoResponse;
use futures::stream::StreamExt;
use postrust_core::schema_cache::Cardinality;
use postrust_core::{Column, QualifiedIdentifier, Relationship, SchemaCache, Table};
use sqlx::types::{BigDecimal, Json};
use sqlx::PgPool;
use uuid::Uuid;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::RwLock;
use tracing::{debug, info, trace};

/// GraphQL execution state shared across requests.
pub struct GraphQLState {
    /// Database connection pool
    pub pool: PgPool,
    /// Schema cache
    pub schema_cache: Arc<SchemaCache>,
    /// Generated GraphQL schema
    pub generated_schema: GeneratedSchema,
    /// async-graphql Schema (built dynamically)
    pub schema: Schema,
    /// Schema configuration
    pub config: SchemaConfig,
    /// Subscription fields
    pub subscription_fields: Vec<SubField>,
    /// Notification broker for subscriptions
    pub broker: Arc<RwLock<Option<NotifyBroker>>>,
}

impl GraphQLState {
    /// Create new GraphQL state from schema cache.
    pub fn new(
        pool: PgPool,
        schema_cache: Arc<SchemaCache>,
        config: SchemaConfig,
    ) -> Result<Self, GraphQLError> {
        let generated_schema = build_schema(&schema_cache, &config);
        let subscription_fields = if config.enable_subscriptions {
            generate_subscription_fields(&schema_cache, &generated_schema)
        } else {
            Vec::new()
        };
        let schema = build_dynamic_schema(
            &generated_schema,
            &schema_cache,
            if config.enable_subscriptions {
                Some(subscription_fields.as_slice())
            } else {
                None
            },
            &config,
        )?;

        Ok(Self {
            pool: pool.clone(),
            schema_cache,
            generated_schema,
            schema,
            config,
            subscription_fields,
            broker: Arc::new(RwLock::new(None)),
        })
    }

    /// Rebuild the schema (e.g., after schema cache refresh).
    pub fn rebuild(&mut self) -> Result<(), GraphQLError> {
        self.generated_schema = build_schema(&self.schema_cache, &self.config);
        self.subscription_fields = if self.config.enable_subscriptions {
            generate_subscription_fields(&self.schema_cache, &self.generated_schema)
        } else {
            Vec::new()
        };
        self.schema = build_dynamic_schema(
            &self.generated_schema,
            &self.schema_cache,
            if self.config.enable_subscriptions {
                Some(self.subscription_fields.as_slice())
            } else {
                None
            },
            &self.config,
        )?;
        Ok(())
    }

    /// Initialize the subscription broker.
    ///
    /// This should be called after creating the state to enable subscriptions.
    pub async fn init_subscriptions(&self) -> Result<(), crate::subscription::BrokerError> {
        if !self.config.enable_subscriptions {
            return Ok(());
        }

        let broker = NotifyBroker::new(self.pool.clone());

        // Collect all channels to listen on
        let channels: Vec<String> = self
            .subscription_fields
            .iter()
            .map(|f| f.channel_name())
            .collect();

        if !channels.is_empty() {
            broker.start(channels).await?;
            info!(
                "Subscription broker started with {} channels",
                self.subscription_fields.len()
            );
        }

        // Store the broker
        let mut broker_guard = self.broker.write().await;
        *broker_guard = Some(broker);

        Ok(())
    }

    /// Stop the subscription broker.
    pub async fn stop_subscriptions(&self) {
        let broker_guard = self.broker.read().await;
        if let Some(broker) = broker_guard.as_ref() {
            broker.stop().await;
        }
    }

    /// Get the notification broker.
    pub async fn get_broker(&self) -> Option<Arc<RwLock<Option<NotifyBroker>>>> {
        Some(Arc::clone(&self.broker))
    }
}

/// Handle a GraphQL request.
pub async fn graphql_handler(
    State(state): State<Arc<GraphQLState>>,
    ctx: GraphQLContext,
    req: GraphQLRequest,
) -> GraphQLResponse {
    let request = req
        .into_inner()
        .data(ctx)
        .data(state.pool.clone())
        .data(Arc::clone(&state.broker));
    state.schema.execute(request).await.into()
}

/// Handle GraphQL WebSocket subscription upgrade.
///
/// This should be called with a WebSocket upgrade request to enable
/// GraphQL subscriptions over WebSocket.
pub async fn graphql_ws_handler(
    State(state): State<Arc<GraphQLState>>,
    protocol: async_graphql_axum::GraphQLProtocol,
    ws: axum::extract::WebSocketUpgrade,
) -> impl IntoResponse {
    let schema = state.schema.clone();
    let pool = state.pool.clone();
    let broker = Arc::clone(&state.broker);

    ws.protocols(["graphql-transport-ws", "graphql-ws"])
        .on_upgrade(move |socket| async move {
            let mut data = async_graphql::Data::default();
            data.insert(pool);
            data.insert(broker);

            async_graphql_axum::GraphQLWebSocket::new(socket, schema, protocol)
                .with_data(data)
                .serve()
                .await
        })
}

/// Handle GraphQL playground request.
pub async fn graphql_playground() -> impl axum::response::IntoResponse {
    axum::response::Html(async_graphql::http::playground_source(
        async_graphql::http::GraphQLPlaygroundConfig::new("/graphql")
            .subscription_endpoint("/graphql/ws"),
    ))
}

/// Everything the federation `_entities` resolver needs to materialise one
/// entity type: where its rows live and how to interpret its `@key` columns.
#[derive(Clone)]
struct EntityInfo {
    schema_name: String,
    table_name: String,
    /// Primary-key columns in `@key` order, with type info for value coercion.
    key_columns: Vec<Column>,
}

/// Build the `__typename` -> [`EntityInfo`] map used by `_entities`. Only tables
/// with a primary key are entities (they are the ones that carry a `@key`); the
/// map is keyed by the generated (possibly prefixed) GraphQL type name so it
/// matches the `__typename` in incoming representations.
fn build_entity_lookup(generated: &GeneratedSchema) -> HashMap<String, EntityInfo> {
    let mut lookup = HashMap::new();
    for (type_name, obj) in &generated.object_types {
        if obj.table.pk_cols.is_empty() {
            continue;
        }
        let key_columns: Vec<Column> = obj
            .table
            .pk_cols
            .iter()
            .filter_map(|pk| obj.table.get_column(pk).cloned())
            .collect();
        // Defensive: skip if a PK column isn't in the column set.
        if key_columns.len() != obj.table.pk_cols.len() {
            continue;
        }
        lookup.insert(
            type_name.clone(),
            EntityInfo {
                schema_name: obj.table.schema.clone(),
                table_name: obj.table.name.clone(),
                key_columns,
            },
        );
    }
    lookup
}

/// Resolve the federation `_entities` query: for each representation, fetch the
/// backing row by its `@key` (honouring RLS via [`begin_request_tx`]) and return
/// it typed, so the entity's own field resolvers read the columns from the row.
async fn resolve_entities<'a>(
    ctx: &ResolverContext<'a>,
    lookup: &HashMap<String, EntityInfo>,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    let pool = ctx.data::<PgPool>()?;
    let gql_ctx = ctx.data::<GraphQLContext>()?;

    let representations = ctx.args.try_get("representations")?.list()?;
    let mut values = Vec::new();

    for repr in representations.iter() {
        let obj = repr.object()?;
        let typename = obj.try_get("__typename")?.string()?.to_string();

        let info = lookup.get(&typename).ok_or_else(|| {
            async_graphql::Error::new(format!("unknown federation entity type `{}`", typename))
        })?;

        // Assemble the key values in `@key` column order.
        let mut key = Vec::with_capacity(info.key_columns.len());
        for col in &info.key_columns {
            let accessor = obj.try_get(&col.name).map_err(|_| {
                async_graphql::Error::new(format!(
                    "entity `{}` representation is missing key field `{}`",
                    typename, col.name
                ))
            })?;
            let json = accessor_to_json(&accessor);
            key.push((col.name.clone(), coerce_key_value(col, &json)?));
        }

        let row = execute_by_key_one(pool, &info.schema_name, &info.table_name, &key, gql_ctx)
            .await?
            .into_iter()
            .next();

        match row {
            Some(v) => values.push(FieldValue::value(json_to_value(v)).with_type(typename)),
            // Row absent or hidden by RLS: `_entities` permits a null element.
            None => values.push(FieldValue::NULL),
        }
    }

    Ok(Some(FieldValue::list(values)))
}

/// Build the dynamic async-graphql schema from our generated schema.
fn build_dynamic_schema(
    generated: &GeneratedSchema,
    _schema_cache: &SchemaCache,
    subscription_fields: Option<&[SubField]>,
    config: &SchemaConfig,
) -> Result<Schema, GraphQLError> {
    let enable_federation = config.enable_federation;

    // Create object types for each table
    let mut object_types: HashMap<String, Object> = HashMap::new();

    for (type_name, obj) in &generated.object_types {
        let is_shared = config.is_shared_entity(&obj.table.name);
        let relationship_fields = generated
            .get_relationship_fields(type_name)
            .map(Vec::as_slice)
            .unwrap_or(&[]);
        let table_obj = create_object_type(obj, relationship_fields, enable_federation, is_shared);
        object_types.insert(type_name.clone(), table_obj);
    }

    // Create query type
    let query = create_query_type(generated, enable_federation);

    // Create mutation type
    let mutation = if !generated.mutation_fields.is_empty() {
        Some(create_mutation_type(generated))
    } else {
        None
    };

    // Create subscription type if enabled
    let subscription = subscription_fields.map(create_subscription_type);

    // Build schema
    let mut builder = Schema::build(
        "Query",
        mutation.as_ref().map(|_| "Mutation"),
        subscription.as_ref().map(|_| "Subscription"),
    );

    // Register all object types
    for (_, obj) in object_types {
        builder = builder.register(obj);
    }

    // Register query type
    builder = builder.register(query);

    // Register mutation type if present
    if let Some(mutation) = mutation {
        builder = builder.register(mutation);
    }

    // Register subscription type if present
    if let Some(subscription) = subscription {
        builder = builder.register(subscription);
    }

    // Register scalar types
    builder = builder.register(create_bigint_scalar());
    builder = builder.register(create_bigdecimal_scalar());
    builder = builder.register(create_json_scalar());
    builder = builder.register(create_uuid_scalar());
    builder = builder.register(create_date_scalar());
    builder = builder.register(create_datetime_scalar());
    builder = builder.register(create_time_scalar());

    // Register input types
    builder = register_filter_input_types(builder);

    // Emit Apollo Federation primitives (`_service { sdl }`, `_entities`) when
    // this instance is acting as a federated subgraph.
    if enable_federation {
        let entity_lookup = build_entity_lookup(generated);
        builder = builder.enable_federation().entity_resolver(move |ctx| {
            // `Fn` may be invoked per request, so clone the (small) lookup in.
            let entity_lookup = entity_lookup.clone();
            FieldFuture::new(async move { resolve_entities(&ctx, &entity_lookup).await })
        });
    }

    builder
        .finish()
        .map_err(|e| GraphQLError::SchemaError(e.to_string()))
}

/// Create an object type from a TableObjectType.
///
/// When `enable_federation` is set, tables with a primary key are marked as
/// federation entities via `@key`. Object field names match column names, so
/// the key selection is the space-separated list of PK columns (which is also
/// the Federation representation of a composite key).
///
/// `is_shared` marks a table that is federated as the *same* entity across
/// subgraphs; its non-key fields are emitted as `@shareable`.
fn create_object_type(
    obj: &TableObjectType,
    relationship_fields: &[RelationshipField],
    enable_federation: bool,
    is_shared: bool,
) -> Object {
    let mut object = Object::new(&obj.name);

    if let Some(desc) = obj.description() {
        object = object.description(desc);
    }

    if enable_federation && !obj.table.pk_cols.is_empty() {
        object = object.key(obj.table.pk_cols.join(" "));
    }

    for field in &obj.fields {
        let field_name = field.name.clone();
        let field_type = graphql_type_ref(&field.type_string());

        // Create field with resolver that extracts from parent async_graphql::Value
        // The query resolver stores rows as FieldValue::value(Value::Object)
        // so we use as_value() to get the Value and extract fields from the Object
        let gql_field = Field::new(&field.name, field_type, move |ctx| {
            let field_name = field_name.clone();
            FieldFuture::new(async move {
                // Get the parent value as async_graphql::Value using as_value()
                if let Some(Value::Object(map)) = ctx.parent_value.as_value() {
                    // Convert field name to async_graphql::Name for lookup
                    let key = async_graphql::Name::new(&field_name);
                    if let Some(val) = map.get(&key) {
                        return Ok(Some(FieldValue::value(val.clone())));
                    }
                }

                // Field not found or parent not a Value::Object
                Ok(None)
            })
        });

        let gql_field = if let Some(desc) = &field.description {
            gql_field.description(desc)
        } else {
            gql_field
        };

        // A shared entity is resolved by more than one subgraph, so its
        // non-key fields must be `@shareable` for the supergraph to compose.
        // Key fields are implicitly shareable, so they're left untouched.
        let gql_field = if is_shared && !field.is_pk {
            gql_field.shareable()
        } else {
            gql_field
        };

        object = object.field(gql_field);
    }

    for relationship in relationship_fields {
        if obj.has_field(&relationship.name) {
            continue;
        }

        let field_name = relationship.name.clone();
        let field_description = relationship.description.clone();
        let field_type = graphql_type_ref(&relationship.type_string());
        let relationship_for_resolver = relationship.clone();
        let gql_field = Field::new(&field_name, field_type, move |ctx| {
            let relationship = relationship_for_resolver.clone();
            FieldFuture::new(async move {
                resolve_relationship_field(&ctx, &relationship).await
            })
        });

        let gql_field = if let Some(desc) = field_description {
            gql_field.description(desc)
        } else {
            gql_field
        };

        object = object.field(gql_field);
    }

    object
}

async fn resolve_relationship_field<'a>(
    ctx: &ResolverContext<'a>,
    field: &RelationshipField,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    use sqlx::Row;

    let pool = ctx.data::<PgPool>()?;
    let gql_ctx = ctx.data::<GraphQLContext>()?;

    let Relationship::ForeignKey {
        foreign_table,
        cardinality,
        ..
    } = &field.relationship
    else {
        return Err(async_graphql::Error::new(
            "computed relationship fields are not supported yet",
        ));
    };

    if matches!(cardinality, Cardinality::M2M(_)) {
        return Err(async_graphql::Error::new(
            "many-to-many relationship fields are not supported yet",
        ));
    }

    let Some(Value::Object(parent)) = ctx.parent_value.as_value() else {
        return if field.is_list {
            Ok(Some(FieldValue::list(Vec::<FieldValue>::new())))
        } else {
            Ok(None)
        };
    };

    let target_table = table_metadata(gql_ctx, &foreign_table.schema, &foreign_table.name).await?;
    let join_columns = field.join_columns();
    if join_columns.is_empty() {
        return Err(async_graphql::Error::new(format!(
            "relationship field `{}` has no join columns",
            field.name
        )));
    }

    let mut values = Vec::with_capacity(join_columns.len());
    let mut conditions = Vec::with_capacity(join_columns.len());
    for (idx, (source_col, target_col)) in join_columns.iter().enumerate() {
        let Some(value) = parent.get(&async_graphql::Name::new(source_col)) else {
            return if field.is_list {
                Ok(Some(FieldValue::list(Vec::<FieldValue>::new())))
            } else {
                Ok(None)
            };
        };

        if matches!(value, Value::Null) {
            return if field.is_list {
                Ok(Some(FieldValue::list(Vec::<FieldValue>::new())))
            } else {
                Ok(None)
            };
        }

        values.push(BoundValue {
            column_name: target_col.clone(),
            value: value_to_json(value),
        });
        conditions.push(format!(
            "{} = ${}",
            postrust_sql::escape_ident(target_col),
            idx + 1
        ));
    }

    let sql = format!(
        "SELECT row_to_json(t) FROM (SELECT * FROM {}.{} WHERE {}) t",
        postrust_sql::escape_ident(&foreign_table.schema),
        postrust_sql::escape_ident(&foreign_table.name),
        conditions.join(" AND ")
    );

    trace!("Executing relationship SQL: {}", sql);

    let mut tx = begin_request_tx(pool, gql_ctx).await?;
    let query = bind_table_values(&sql, &target_table, &values)?;

    let rows = query.fetch_all(&mut *tx).await?;

    tx.commit().await?;

    let results: Vec<FieldValue> = rows
        .iter()
        .filter_map(|row| row.try_get::<serde_json::Value, _>(0).ok())
        .map(|v| FieldValue::value(json_to_value(v)))
        .collect();

    if field.is_list {
        Ok(Some(FieldValue::list(results)))
    } else {
        Ok(results.into_iter().next())
    }
}

/// Create the Query type with all table query fields.
fn create_query_type(generated: &GeneratedSchema, enable_federation: bool) -> Object {
    let mut query = Object::new("Query");

    for field in &generated.query_fields {
        let table_name = field.table_name.clone();
        let schema_name = field.schema_name.clone();
        let type_name = field.type_name.clone();
        let is_by_pk = field.is_by_pk;
        let is_count = field.is_count;
        let by_pk_id_type = field.by_pk_id_type.clone();
        let by_pk_column = field.by_pk_column.clone();
        let return_type = graphql_type_ref(&field.return_type);

        let mut gql_field = if is_count {
            let table_name_c = table_name.clone();
            let schema_name_c = schema_name.clone();
            Field::new(&field.name, return_type, move |ctx| {
                let table_name = table_name_c.clone();
                let schema_name = schema_name_c.clone();
                FieldFuture::new(async move {
                    resolve_count(&ctx, &schema_name, &table_name).await
                })
            })
        } else {
            let table_name_q = table_name.clone();
            let schema_name_q = schema_name.clone();
            let type_name_q = type_name.clone();
            let by_pk_id_type_q = by_pk_id_type.clone();
            let by_pk_column_q = by_pk_column.clone();
            Field::new(&field.name, return_type, move |ctx| {
                let table_name = table_name_q.clone();
                let schema_name = schema_name_q.clone();
                let type_name = type_name_q.clone();
                let by_pk_id_type = by_pk_id_type_q.clone();
                let by_pk_column = by_pk_column_q.clone();
                FieldFuture::new(async move {
                    resolve_query(
                        &ctx,
                        &schema_name,
                        &table_name,
                        &type_name,
                        is_by_pk,
                        by_pk_id_type,
                        by_pk_column,
                    )
                    .await
                })
            })
        };

        // Add arguments
        if is_count {
            gql_field = gql_field
                .argument(InputValue::new("filter", TypeRef::named("JSON")));
        } else if !is_by_pk {
            gql_field = gql_field
                .argument(InputValue::new("filter", TypeRef::named("JSON")))
                .argument(InputValue::new("orderBy", TypeRef::named_list("String")))
                .argument(InputValue::new("limit", TypeRef::named("Int")))
                .argument(InputValue::new("offset", TypeRef::named("Int")));
        } else {
            // Single PK column, matching react-admin + ra-data-graphql getOne (variable `id`)
            let id_scalar = field
                .by_pk_id_type
                .as_deref()
                .unwrap_or("Int");
            gql_field = gql_field.argument(InputValue::new("id", TypeRef::named_nn(id_scalar)));
        }

        if let Some(desc) = &field.description {
            gql_field = gql_field.description(desc);
        }

        query = query.field(gql_field);
    }

    // Add introspection queries
    let mut schema_field = Field::new("_schema", TypeRef::named("String"), |_| {
        FieldFuture::new(async move {
            Ok(Some(Value::String("Postrust GraphQL Schema".to_string())))
        })
    })
    .description("Schema introspection");
    // Emitted identically by every subgraph; mark @shareable so a federated
    // supergraph composes instead of failing on the duplicate root field.
    if enable_federation {
        schema_field = schema_field.shareable();
    }
    query = query.field(schema_field);

    query
}

/// Create the Mutation type with all mutation fields.
fn create_mutation_type(generated: &GeneratedSchema) -> Object {
    let mut mutation = Object::new("Mutation");

    for field in &generated.mutation_fields {
        let table_name = field.table_name.clone();
        let schema_name = field.schema_name.clone();
        let mutation_type = field.mutation_type;
        let return_type = graphql_type_ref(&field.return_type);

        let mut gql_field = Field::new(&field.name, return_type, move |ctx| {
            let table_name = table_name.clone();
            let schema_name = schema_name.clone();
            FieldFuture::new(async move {
                resolve_mutation(&ctx, &schema_name, &table_name, mutation_type).await
            })
        });

        // Add mutation-specific arguments
        match mutation_type {
            MutationType::Insert | MutationType::InsertOne => {
                gql_field = gql_field
                    .argument(InputValue::new("objects", TypeRef::named_nn_list("JSON")));
            }
            MutationType::Update | MutationType::UpdateByPk => {
                gql_field = gql_field
                    .argument(InputValue::new("where", TypeRef::named("JSON")))
                    .argument(InputValue::new("set", TypeRef::named_nn("JSON")));
            }
            MutationType::Delete | MutationType::DeleteByPk => {
                gql_field = gql_field.argument(InputValue::new("where", TypeRef::named("JSON")));
            }
        }

        if let Some(desc) = &field.description {
            gql_field = gql_field.description(desc);
        }

        mutation = mutation.field(gql_field);
    }

    mutation
}

/// Create the Subscription type with all subscription fields.
fn create_subscription_type(fields: &[SubField]) -> Subscription {
    let mut subscription = Subscription::new("Subscription");

    for field in fields {
        let channel_name = field.channel_name();
        let return_type = TypeRef::named(&field.return_type);
        let field_name = field.name.clone();
        let description = field.description.clone();

        let gql_field = SubscriptionField::new(&field_name, return_type, move |ctx| {
            let channel_name = channel_name.clone();
            SubscriptionFieldFuture::new(async move {
                let broker_arc = ctx.data::<Arc<RwLock<Option<NotifyBroker>>>>()?;
                let broker_guard = broker_arc.read().await;

                let broker = broker_guard
                    .as_ref()
                    .ok_or_else(|| async_graphql::Error::new("Subscription broker not initialized"))?;

                let stream = broker
                    .subscribe(&channel_name)
                    .await
                    .map_err(|e| async_graphql::Error::new(format!("Subscription error: {}", e)))?;

                // Transform notification stream to GraphQL values
                // Use FieldValue::value() so field resolvers can use as_value()
                let value_stream = stream.filter_map(|notification| async move {
                    match TableChangePayload::from_payload(&notification.payload) {
                        Ok(payload) => {
                            if let Some(data) = subscription_event_value(&payload) {
                                // Convert to async_graphql::Value so field resolvers can extract fields
                                Some(Ok(FieldValue::value(json_to_value(data))))
                            } else {
                                None
                            }
                        }
                        Err(e) => {
                            debug!("Failed to parse notification payload: {}", e);
                            None
                        }
                    }
                });

                Ok(value_stream)
            })
        });

        let gql_field = if let Some(desc) = description {
            gql_field.description(desc)
        } else {
            gql_field
        };

        subscription = subscription.field(gql_field);
    }

    subscription
}

/// Single-column primary key value, matching the generated by-PK GraphQL `id` argument.
enum ByPkParam {
    I64(i64),
    Uuid(Uuid),
    String(String),
}

/// Begin a per-request transaction with the caller's role and JWT claims applied
/// as transaction-local settings.
///
/// `SET LOCAL ROLE` and the `request.jwt.claims.*` GUCs only persist for the
/// lifetime of a transaction, so the role switch, the claims, and the actual
/// query must all share one transaction. Committing resets them automatically,
/// leaving the pooled connection clean for the next request. (Mirrors the REST
/// path in `postrust-server`.)
async fn begin_request_tx(
    pool: &PgPool,
    ctx: &GraphQLContext,
) -> Result<sqlx::Transaction<'static, sqlx::Postgres>, async_graphql::Error> {
    let mut tx = pool.begin().await?;

    sqlx::query(&format!(
        "SET LOCAL ROLE {}",
        postrust_sql::escape_ident(ctx.role())
    ))
    .execute(&mut *tx)
    .await?;

    // Expose JWT claims as transaction-local GUCs so RLS policies can read them
    // via current_setting('request.jwt.claims.<name>', true).
    for (key, value) in &ctx.auth.claims {
        let guc_key = format!("request.jwt.claims.{}", key);
        let guc_value = match value {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        };
        let _ = sqlx::query("SELECT set_config($1, $2, true)")
            .bind(&guc_key)
            .bind(&guc_value)
            .execute(&mut *tx)
            .await;
    }

    Ok(tx)
}

/// `SELECT * … WHERE <pk> = $1` with a typed parameter (single-column key).
async fn execute_by_pk_one(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    pk_col: &str,
    value: ByPkParam,
    ctx: &GraphQLContext,
) -> Result<Vec<serde_json::Value>, async_graphql::Error> {
    execute_by_key_one(pool, schema_name, table_name, &[(pk_col.to_string(), value)], ctx).await
}

/// `SELECT * … WHERE c1 = $1 AND c2 = $2 …` for a (possibly composite) key.
///
/// Runs inside a per-request transaction so the caller's role and JWT claims —
/// and therefore RLS — apply to the fetch. This is also the entry point the
/// federation `_entities` resolver uses to materialise an entity by its `@key`.
async fn execute_by_key_one(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    key: &[(String, ByPkParam)],
    ctx: &GraphQLContext,
) -> Result<Vec<serde_json::Value>, async_graphql::Error> {
    use sqlx::Row;

    if key.is_empty() {
        return Err(async_graphql::Error::new(
            "cannot fetch a row without any key columns",
        ));
    }

    let s = postrust_sql::escape_ident(schema_name);
    let t = postrust_sql::escape_ident(table_name);
    let where_sql = key
        .iter()
        .enumerate()
        .map(|(i, (col, _))| format!("{} = ${}", postrust_sql::escape_ident(col), i + 1))
        .collect::<Vec<_>>()
        .join(" AND ");
    let sql = format!("SELECT row_to_json(s) FROM (SELECT * FROM {s}.{t} WHERE {where_sql}) s");

    trace!("Executing by-key SQL: {}, role={}", sql, ctx.role());

    let mut tx = begin_request_tx(pool, ctx).await?;

    let mut query = sqlx::query(&sql);
    for (_, param) in key {
        query = match param {
            ByPkParam::I64(n) => query.bind(*n),
            ByPkParam::Uuid(u) => query.bind(*u),
            ByPkParam::String(s) => query.bind(s.clone()),
        };
    }

    let rows = query
        .fetch_all(&mut *tx)
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

    let results: Vec<serde_json::Value> = rows
        .iter()
        .filter_map(|row| row.try_get::<serde_json::Value, _>(0).ok())
        .collect();

    tx.commit()
        .await
        .map_err(|e| async_graphql::Error::new(e.to_string()))?;

    Ok(results)
}

/// Coerce a federation representation value into a typed key parameter based on
/// the column's Postgres type (mirrors the by-PK `id` coercion).
fn coerce_key_value(
    col: &Column,
    value: &serde_json::Value,
) -> Result<ByPkParam, async_graphql::Error> {
    let dt = col.data_type.to_lowercase();
    let nt = col.nominal_type.to_lowercase();

    if dt == "integer" || dt == "int4" || dt == "smallint" || dt == "int2" {
        let n = value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|u| i64::try_from(u).ok()))
            .ok_or_else(|| {
                async_graphql::Error::new(format!("entity key `{}` must be an integer", col.name))
            })?;
        Ok(ByPkParam::I64(n))
    } else if dt == "bigint" || dt == "int8" || nt == "int8" {
        // `BigInt` is a custom scalar that may arrive as a JSON number or a
        // numeric string; either way it must bind as int8, not text, or the
        // `WHERE bigint_col = $n` comparison fails to type-check in Postgres.
        let n = value
            .as_i64()
            .or_else(|| value.as_u64().and_then(|u| i64::try_from(u).ok()))
            .or_else(|| value.as_str().and_then(|s| s.parse::<i64>().ok()))
            .ok_or_else(|| {
                async_graphql::Error::new(format!("entity key `{}` must be a bigint", col.name))
            })?;
        Ok(ByPkParam::I64(n))
    } else if dt == "uuid" || nt == "uuid" {
        let s = value.as_str().ok_or_else(|| {
            async_graphql::Error::new(format!("entity key `{}` must be a UUID string", col.name))
        })?;
        let u = Uuid::parse_str(s).map_err(|e| {
            async_graphql::Error::new(format!("entity key `{}` is not a valid UUID: {e}", col.name))
        })?;
        Ok(ByPkParam::Uuid(u))
    } else {
        let s = if let Some(s) = value.as_str() {
            s.to_string()
        } else if let Some(n) = value.as_i64() {
            n.to_string()
        } else if let Some(n) = value.as_u64() {
            n.to_string()
        } else {
            return Err(async_graphql::Error::new(format!(
                "entity key `{}` value could not be interpreted",
                col.name
            )));
        };
        Ok(ByPkParam::String(s))
    }
}

/// Resolve a query field.
async fn resolve_query<'a>(
    ctx: &ResolverContext<'a>,
    schema_name: &str,
    table_name: &str,
    _type_name: &str,
    is_by_pk: bool,
    by_pk_id_type: Option<String>,
    by_pk_column: Option<String>,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    let pool = ctx.data::<PgPool>()?;
    let gql_ctx = ctx.data::<GraphQLContext>()?;

    debug!("Resolving query for table: {}", table_name);

    if is_by_pk {
        let id_type = by_pk_id_type.as_deref().unwrap_or("Int");
        let pk_col = by_pk_column.as_deref().unwrap_or("id");
        let v = ctx
            .args
            .try_get("id")
            .map_err(|_| async_graphql::Error::new("by-pk query requires an `id` argument"))?;
        let j = accessor_to_json(&v);
        let param = match id_type {
            "Int" => {
                let n = j
                    .as_i64()
                    .or_else(|| j.as_u64().and_then(|u| i64::try_from(u).ok()))
                    .ok_or_else(|| {
                        async_graphql::Error::new("by-pk `id` must be an integer")
                    })?;
                ByPkParam::I64(n)
            }
            "UUID" => {
                let s = j
                    .as_str()
                    .ok_or_else(|| {
                        async_graphql::Error::new("by-pk `id` must be a UUID string")
                    })?;
                let u = Uuid::parse_str(s).map_err(|e| {
                    async_graphql::Error::new(format!("by-pk `id` is not a valid UUID: {e}"))
                })?;
                ByPkParam::Uuid(u)
            }
            _ => {
                let s: String = if let Some(s) = j.as_str() {
                    s.to_string()
                } else if let Some(n) = j.as_i64() {
                    n.to_string()
                } else if let Some(n) = j.as_u64() {
                    n.to_string()
                } else {
                    return Err(async_graphql::Error::new(
                        "by-pk `id` value could not be interpreted for this scalar",
                    ));
                };
                ByPkParam::String(s)
            }
        };
        let result = execute_by_pk_one(pool, schema_name, table_name, pk_col, param, gql_ctx).await?;
        return Ok(result
            .into_iter()
            .next()
            .map(|v| FieldValue::value(json_to_value(v))));
    }

    let limit: Option<i64> = ctx
        .args
        .try_get("limit")
        .ok()
        .and_then(|v| v.i64().ok());

    let offset: Option<i64> = ctx
        .args
        .try_get("offset")
        .ok()
        .and_then(|v| v.i64().ok());

    let filter_value = ctx
        .args
        .try_get("filter")
        .ok()
        .map(|v| accessor_to_json(&v));

    let order_by: Option<Vec<String>> = ctx
        .args
        .try_get("orderBy")
        .ok()
        .and_then(|v| v.list().ok())
        .map(|list| {
            list.iter()
                .filter_map(|v| v.string().ok().map(|s| s.to_string()))
                .collect()
        });

    let (sql, where_values) = build_list_sql(schema_name, table_name, filter_value.as_ref(), order_by.as_deref(), limit, offset)?;
    let table = table_metadata(gql_ctx, schema_name, table_name).await?;

    let mut tx = begin_request_tx(pool, gql_ctx).await?;

    let query = bind_table_values(&sql, &table, &where_values)?;

    let result: Vec<serde_json::Value> = {
        use sqlx::Row;
        let rows = query.fetch_all(&mut *tx).await?;
        rows.iter()
            .filter_map(|row| row.try_get::<serde_json::Value, _>(0).ok())
            .collect()
    };

    tx.commit().await?;

    let items: Vec<FieldValue> = result
        .into_iter()
        .map(|v| FieldValue::value(json_to_value(v)))
        .collect();
    Ok(Some(FieldValue::list(items)))
}
/// Build the SQL for a list query with optional filter, ordering, limit, and offset.
fn build_list_sql(
    schema_name: &str,
    table_name: &str,
    filter_value: Option<&serde_json::Value>,
    order_by: Option<&[String]>,
    limit: Option<i64>,
    offset: Option<i64>,
) -> Result<(String, Vec<BoundValue>), async_graphql::Error> {
    let s = postrust_sql::escape_ident(schema_name);
    let t = postrust_sql::escape_ident(table_name);
    let (where_sql, where_values) = build_where_clause(filter_value, 1)?;

    let order_sql = match order_by {
        Some(fields) if !fields.is_empty() => {
            let clauses: Vec<String> = fields
                .iter()
                .filter_map(|s| {
                    // Parse "field_ASC" or "field_DESC"
                    if let Some(field) = s.strip_suffix("_ASC") {
                        Some(format!("{} ASC", postrust_sql::escape_ident(field)))
                    } else if let Some(field) = s.strip_suffix("_DESC") {
                        Some(format!("{} DESC", postrust_sql::escape_ident(field)))
                    } else {
                        None
                    }
                })
                .collect();
            if clauses.is_empty() {
                String::new()
            } else {
                format!(" ORDER BY {}", clauses.join(", "))
            }
        }
        _ => String::new(),
    };

    let mut sql = format!(
        "SELECT row_to_json(t) FROM (SELECT * FROM {s}.{t} {where_sql}{order_sql}) t",
        s = s,
        t = t,
    );

    if let Some(limit) = limit {
        sql.push_str(&format!(" LIMIT {limit}"));
    }
    if let Some(offset) = offset {
        sql.push_str(&format!(" OFFSET {offset}"));
    }

    Ok((sql, where_values))
}

/// Resolve a count query field (e.g., usersCount).
async fn resolve_count<'a>(
    ctx: &ResolverContext<'a>,
    schema_name: &str,
    table_name: &str,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    use sqlx::Row;

    let pool = ctx.data::<PgPool>()?;
    let gql_ctx = ctx.data::<GraphQLContext>()?;

    debug!("Resolving count for table: {}", table_name);

    let filter_value = ctx
        .args
        .try_get("filter")
        .ok()
        .map(|v| accessor_to_json(&v));

    let (where_sql, where_values) = build_where_clause(filter_value.as_ref(), 1)?;

    let sql = format!(
        "SELECT COUNT(*) AS cnt FROM {}.{} {}",
        postrust_sql::escape_ident(schema_name),
        postrust_sql::escape_ident(table_name),
        where_sql,
    );

    trace!("Executing COUNT SQL: {}", sql);

    let table = table_metadata(gql_ctx, schema_name, table_name).await?;
    let mut tx = begin_request_tx(pool, gql_ctx).await?;

    let query = bind_table_values(&sql, &table, &where_values)?;

    let row = query.fetch_one(&mut *tx).await?;
    let count: i64 = row.try_get("cnt")?;

    tx.commit().await?;

    Ok(Some(FieldValue::value(Value::Number(count.into()))))
}

/// Resolve a mutation field.
async fn resolve_mutation<'a>(
    ctx: &ResolverContext<'a>,
    schema_name: &str,
    table_name: &str,
    mutation_type: MutationType,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    let pool = ctx.data::<PgPool>()?;
    let gql_ctx = ctx.data::<GraphQLContext>()?;

    debug!("Resolving mutation for table: {} type: {:?}", table_name, mutation_type);

    let result = match mutation_type {
        MutationType::Insert | MutationType::InsertOne => {
            let objects = ctx
                .args
                .try_get("objects")
                .ok()
                .map(|v| accessor_to_json(&v))
                .unwrap_or_else(|| serde_json::Value::Array(vec![]));

            execute_insert(pool, schema_name, table_name, gql_ctx, objects, mutation_type).await?
        }
        MutationType::Update | MutationType::UpdateByPk => {
            let set_value = ctx
                .args
                .try_get("set")
                .ok()
                .map(|v| accessor_to_json(&v))
                .unwrap_or_else(|| serde_json::json!({}));

            let where_clause = ctx
                .args
                .try_get("where")
                .ok()
                .map(|v| accessor_to_json(&v));

            execute_update(pool, schema_name, table_name, gql_ctx, set_value, where_clause, mutation_type).await?
        }
        MutationType::Delete | MutationType::DeleteByPk => {
            let where_clause = ctx
                .args
                .try_get("where")
                .ok()
                .map(|v| accessor_to_json(&v));

            execute_delete(pool, schema_name, table_name, gql_ctx, where_clause, mutation_type).await?
        }
    };

    Ok(result)
}


/// Execute an insert mutation.
async fn execute_insert<'a>(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    ctx: &GraphQLContext,
    objects: serde_json::Value,
    mutation_type: MutationType,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    use sqlx::Row;

    trace!("Insert mutation for {}: {:?}", table_name, objects);

    // Handle both array and single object
    let objects_array = match objects {
        serde_json::Value::Array(arr) => arr,
        serde_json::Value::Object(obj) => vec![serde_json::Value::Object(obj)],
        _ => return Err(async_graphql::Error::new("objects must be an array or object")),
    };

    if objects_array.is_empty() {
        return Err(async_graphql::Error::new("objects cannot be empty"));
    }

    let table = table_metadata(ctx, schema_name, table_name).await?;
    let mut tx = begin_request_tx(pool, ctx).await?;

    let mut inserted: Vec<FieldValue> = Vec::new();

    for obj in objects_array {
        if let serde_json::Value::Object(map) = obj {
            // Build INSERT query
            let columns: Vec<&str> = map.keys().map(|k| k.as_str()).collect();
            let placeholders: Vec<String> = (1..=columns.len()).map(|i| format!("${}", i)).collect();

            let sql = format!(
                "INSERT INTO {}.{} ({}) VALUES ({}) RETURNING row_to_json({}.{}.*)",
                postrust_sql::escape_ident(schema_name),
                postrust_sql::escape_ident(table_name),
                columns.iter().map(|c| postrust_sql::escape_ident(c)).collect::<Vec<_>>().join(", "),
                placeholders.join(", "),
                postrust_sql::escape_ident(schema_name),
                postrust_sql::escape_ident(table_name)
            );

            trace!("Executing INSERT SQL: {}", sql);

            let values: Vec<BoundValue> = columns
                .iter()
                .map(|column| BoundValue {
                    column_name: (*column).to_string(),
                    value: map[*column].clone(),
                })
                .collect();
            let query = bind_table_values(&sql, &table, &values)?;

            let row = query.fetch_one(&mut *tx).await?;
            if let Ok(json_val) = row.try_get::<serde_json::Value, _>(0) {
                inserted.push(FieldValue::value(json_to_value(json_val)));
            }
        }
    }

    tx.commit().await?;

    // Return based on mutation type
    match mutation_type {
        MutationType::InsertOne => {
            // Return single item
            Ok(inserted.into_iter().next())
        }
        _ => {
            // Return list
            Ok(Some(FieldValue::list(inserted)))
        }
    }
}

async fn table_metadata(
    ctx: &GraphQLContext,
    schema_name: &str,
    table_name: &str,
) -> Result<Table, async_graphql::Error> {
    let cache = ctx
        .schema_cache
        .get()
        .await
        .map_err(|error| async_graphql::Error::new(error.to_string()))?;
    let identifier = QualifiedIdentifier::new(schema_name, table_name);
    cache
        .as_ref()
        .and_then(|cache| cache.get_table(&identifier))
        .cloned()
        .ok_or_else(|| {
            async_graphql::Error::new(format!(
                "table `{schema_name}.{table_name}` is missing from the schema cache"
            ))
        })
}

enum TypedColumnValue {
    I16(Option<i16>),
    I32(Option<i32>),
    I64(Option<i64>),
    F32(Option<f32>),
    F64(Option<f64>),
    Numeric(Option<BigDecimal>),
    Bool(Option<bool>),
    Uuid(Option<Uuid>),
    String(Option<String>),
    Json(Option<Json<serde_json::Value>>),
}

#[derive(Clone, Debug)]
struct BoundValue {
    column_name: String,
    value: serde_json::Value,
}

#[cfg(test)]
impl PartialEq<serde_json::Value> for BoundValue {
    fn eq(&self, other: &serde_json::Value) -> bool {
        self.value == *other
    }
}

fn bind_table_values<'q>(
    sql: &'q str,
    table: &Table,
    values: &[BoundValue],
) -> Result<
    sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    async_graphql::Error,
> {
    let columns: Vec<&Column> = values
        .iter()
        .map(|value| {
            table.get_column(&value.column_name).ok_or_else(|| {
                async_graphql::Error::new(format!(
                    "column `{}` does not exist on {}.{}",
                    value.column_name, table.schema, table.name
                ))
            })
        })
        .collect::<Result<_, _>>()?;
    let bindings: Vec<Option<TypedColumnValue>> = columns
        .iter()
        .zip(values)
        .map(|(column, value)| coerce_column_value(column, &value.value))
        .collect::<Result<_, _>>()?;

    let mut query = if bindings.iter().all(Option::is_some) {
        sqlx::query(sql)
    } else {
        dynamic_json_query(sql)
    };

    for ((column, value), binding) in columns.iter().zip(values).zip(bindings) {
        query = match binding {
            Some(binding) => bind_typed_column_value(query, binding),
            None => {
                debug!(
                    "No typed GraphQL binding for PostgreSQL type `{}` on column `{}.{}`; disabling statement persistence",
                    column.data_type,
                    table.name,
                    column.name,
                );
                bind_json_value(query, &value.value)
            }
        };
    }

    Ok(query)
}

fn coerce_column_value(
    column: &Column,
    value: &serde_json::Value,
) -> Result<Option<TypedColumnValue>, async_graphql::Error> {
    let type_name = column.data_type.to_ascii_lowercase();
    let invalid = || {
        async_graphql::Error::new(format!(
            "value `{value}` is not valid for column `{}` of PostgreSQL type `{}`",
            column.name, column.data_type
        ))
    };

    if value.is_null() {
        if !column.nullable {
            return Err(async_graphql::Error::new(format!(
                "column `{}` does not accept null",
                column.name
            )));
        }
        return Ok(match type_name.as_str() {
            "smallint" | "int2" => Some(TypedColumnValue::I16(None)),
            "integer" | "int" | "int4" => Some(TypedColumnValue::I32(None)),
            "bigint" | "int8" => Some(TypedColumnValue::I64(None)),
            "real" | "float4" => Some(TypedColumnValue::F32(None)),
            "double precision" | "float8" => Some(TypedColumnValue::F64(None)),
            "numeric" | "decimal" => Some(TypedColumnValue::Numeric(None)),
            "boolean" | "bool" => Some(TypedColumnValue::Bool(None)),
            "uuid" => Some(TypedColumnValue::Uuid(None)),
            "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" => {
                Some(TypedColumnValue::String(None))
            }
            "json" | "jsonb" => Some(TypedColumnValue::Json(None)),
            _ => None,
        });
    }

    let binding = match type_name.as_str() {
        "smallint" | "int2" => TypedColumnValue::I16(Some(
            value
                .as_i64()
                .and_then(|value| i16::try_from(value).ok())
                .ok_or_else(invalid)?,
        )),
        "integer" | "int" | "int4" => TypedColumnValue::I32(Some(
            value
                .as_i64()
                .and_then(|value| i32::try_from(value).ok())
                .ok_or_else(invalid)?,
        )),
        "bigint" | "int8" => {
            TypedColumnValue::I64(Some(value.as_i64().ok_or_else(invalid)?))
        }
        "real" | "float4" => {
            TypedColumnValue::F32(Some(value.as_f64().ok_or_else(invalid)? as f32))
        }
        "double precision" | "float8" => {
            TypedColumnValue::F64(Some(value.as_f64().ok_or_else(invalid)?))
        }
        "numeric" | "decimal" => TypedColumnValue::Numeric(Some(
            value
                .as_number()
                .ok_or_else(invalid)?
                .to_string()
                .parse::<BigDecimal>()
                .map_err(|_| invalid())?,
        )),
        "boolean" | "bool" => {
            TypedColumnValue::Bool(Some(value.as_bool().ok_or_else(invalid)?))
        }
        "uuid" => TypedColumnValue::Uuid(Some(
            value
                .as_str()
                .ok_or_else(invalid)?
                .parse::<Uuid>()
                .map_err(|_| invalid())?,
        )),
        "text" | "varchar" | "character varying" | "char" | "character" | "bpchar" => {
            TypedColumnValue::String(Some(
                value.as_str().ok_or_else(invalid)?.to_string(),
            ))
        }
        "json" | "jsonb" => TypedColumnValue::Json(Some(Json(value.clone()))),
        _ => return Ok(None),
    };

    Ok(Some(binding))
}

fn bind_typed_column_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: TypedColumnValue,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        TypedColumnValue::I16(value) => query.bind(value),
        TypedColumnValue::I32(value) => query.bind(value),
        TypedColumnValue::I64(value) => query.bind(value),
        TypedColumnValue::F32(value) => query.bind(value),
        TypedColumnValue::F64(value) => query.bind(value),
        TypedColumnValue::Numeric(value) => query.bind(value),
        TypedColumnValue::Bool(value) => query.bind(value),
        TypedColumnValue::Uuid(value) => query.bind(value),
        TypedColumnValue::String(value) => query.bind(value),
        TypedColumnValue::Json(value) => query.bind(value),
    }
}

/// Build a query whose parameter types are selected dynamically from JSON values.
///
/// These statements cannot safely be persisted because SQLx caches them by SQL
/// text while PostgreSQL fixes each parameter's type when the statement is first
/// prepared. A later JSON value may select a different Rust/PostgreSQL type for
/// the same placeholder.
fn dynamic_json_query<'q>(
    sql: &'q str,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    sqlx::query(sql).persistent(false)
}

/// Bind a JSON value to a sqlx query.
fn bind_json_value<'q>(
    query: sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments>,
    value: &serde_json::Value,
) -> sqlx::query::Query<'q, sqlx::Postgres, sqlx::postgres::PgArguments> {
    match value {
        serde_json::Value::Null => query.bind(None::<String>),
        serde_json::Value::Bool(b) => query.bind(*b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                query.bind(i)
            } else if let Some(f) = n.as_f64() {
                query.bind(f)
            } else {
                query.bind(n.to_string())
            }
        }
        serde_json::Value::String(s) => {
            // Bind UUID-formatted strings as uuid so comparisons against
            // uuid-typed columns succeed. Notably
            // demand_demo.time_series_demand.data_center_uid was migrated from
            // text -> uuid (EOS-457); binding it as text produced
            // `operator does not exist: uuid = text`. Non-UUID strings still
            // bind as text.
            if let Ok(u) = uuid::Uuid::parse_str(s) {
                query.bind(u)
            } else {
                query.bind(s.clone())
            }
        }
        _ => query.bind(value.to_string()),
    }
}

/// Execute an update mutation.
async fn execute_update<'a>(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    ctx: &GraphQLContext,
    set_value: serde_json::Value,
    where_clause: Option<serde_json::Value>,
    mutation_type: MutationType,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    use sqlx::Row;

    trace!("Update mutation for {}: {:?}", table_name, set_value);

    let set_map = match set_value {
        serde_json::Value::Object(map) => map,
        _ => return Err(async_graphql::Error::new("set must be an object")),
    };

    if set_map.is_empty() {
        return Err(async_graphql::Error::new("set cannot be empty"));
    }

    let table = table_metadata(ctx, schema_name, table_name).await?;
    let mut tx = begin_request_tx(pool, ctx).await?;

    // Build SET clause
    let mut set_parts: Vec<String> = Vec::new();
    let mut param_idx = 1;
    for key in set_map.keys() {
        set_parts.push(format!("{} = ${}", postrust_sql::escape_ident(key), param_idx));
        param_idx += 1;
    }

    // Build WHERE clause
    let (where_sql, where_values) = build_where_clause(where_clause.as_ref(), param_idx)?;

    let sql = format!(
        "UPDATE {}.{} SET {} {} RETURNING row_to_json({}.{}.*)",
        postrust_sql::escape_ident(schema_name),
        postrust_sql::escape_ident(table_name),
        set_parts.join(", "),
        where_sql,
        postrust_sql::escape_ident(schema_name),
        postrust_sql::escape_ident(table_name)
    );

    trace!("Executing UPDATE SQL: {}", sql);

    let mut values: Vec<BoundValue> = set_map
        .into_iter()
        .map(|(column_name, value)| BoundValue { column_name, value })
        .collect();
    values.extend(where_values);
    let query = bind_table_values(&sql, &table, &values)?;

    let rows = query.fetch_all(&mut *tx).await?;

    let updated: Vec<FieldValue> = rows
        .iter()
        .filter_map(|row| row.try_get::<serde_json::Value, _>(0).ok())
        .map(|v| FieldValue::value(json_to_value(v)))
        .collect();

    tx.commit().await?;

    // Return based on mutation type
    match mutation_type {
        MutationType::UpdateByPk => {
            Ok(updated.into_iter().next())
        }
        _ => {
            Ok(Some(FieldValue::list(updated)))
        }
    }
}

/// Execute a delete mutation.
async fn execute_delete<'a>(
    pool: &PgPool,
    schema_name: &str,
    table_name: &str,
    ctx: &GraphQLContext,
    where_clause: Option<serde_json::Value>,
    mutation_type: MutationType,
) -> Result<Option<FieldValue<'a>>, async_graphql::Error> {
    use sqlx::Row;

    trace!("Delete mutation for {}", table_name);

    let mut tx = begin_request_tx(pool, ctx).await?;

    // Build WHERE clause
    let (where_sql, where_values) = build_where_clause(where_clause.as_ref(), 1)?;

    let sql = format!(
        "DELETE FROM {}.{} {} RETURNING row_to_json({}.{}.*)",
        postrust_sql::escape_ident(schema_name),
        postrust_sql::escape_ident(table_name),
        where_sql,
        postrust_sql::escape_ident(schema_name),
        postrust_sql::escape_ident(table_name)
    );

    trace!("Executing DELETE SQL: {}", sql);

    let table = table_metadata(ctx, schema_name, table_name).await?;
    let query = bind_table_values(&sql, &table, &where_values)?;

    let rows = query.fetch_all(&mut *tx).await?;

    let deleted: Vec<FieldValue> = rows
        .iter()
        .filter_map(|row| row.try_get::<serde_json::Value, _>(0).ok())
        .map(|v| FieldValue::value(json_to_value(v)))
        .collect();

    tx.commit().await?;

    // Return based on mutation type
    match mutation_type {
        MutationType::DeleteByPk => {
            Ok(deleted.into_iter().next())
        }
        _ => {
            Ok(Some(FieldValue::list(deleted)))
        }
    }
}

/// Build a WHERE clause from a JSON filter object.
fn build_where_clause(
    where_value: Option<&serde_json::Value>,
    start_param_idx: usize,
) -> Result<(String, Vec<BoundValue>), async_graphql::Error> {
    let mut conditions: Vec<String> = Vec::new();
    let mut values: Vec<BoundValue> = Vec::new();
    let mut param_idx = start_param_idx;

    if let Some(serde_json::Value::Object(map)) = where_value {
        for (key, val) in map {
            match val {
                serde_json::Value::Object(op_map) => {
                    // Handle operators like {eq: value}, {gt: value}, etc.
                    for (op, op_val) in op_map {
                        match op.as_str() {
                            "in" | "_in" => {
                                if let serde_json::Value::Array(arr) = op_val {
                                    if arr.is_empty() {
                                        conditions.push("FALSE".to_string());
                                    } else {
                                        let col = postrust_sql::escape_ident(key);
                                        let parts: Vec<String> = arr.iter().map(|v| {
                                            let placeholder = format!("${}", param_idx);
                                            values.push(BoundValue {
                                                column_name: key.clone(),
                                                value: v.clone(),
                                            });
                                            param_idx += 1;
                                            format!("{} = {}", col, placeholder)
                                        }).collect();
                                        if parts.len() == 1 {
                                            conditions.push(parts.into_iter().next().unwrap());
                                        } else {
                                            conditions.push(format!("({})", parts.join(" OR ")));
                                        }
                                    }
                                }
                            }
                            "is_null" | "_is_null" => {
                                if op_val.as_bool().unwrap_or(false) {
                                    conditions.push(format!("{} IS NULL", postrust_sql::escape_ident(key)));
                                } else {
                                    conditions.push(format!("{} IS NOT NULL", postrust_sql::escape_ident(key)));
                                }
                            }
                            _ => {
                                let condition = match op.as_str() {
                                    "eq" | "_eq" => format!("{} = ${}", postrust_sql::escape_ident(key), param_idx),
                                    "neq" | "_neq" => format!("{} != ${}", postrust_sql::escape_ident(key), param_idx),
                                    "gt" | "_gt" => format!("{} > ${}", postrust_sql::escape_ident(key), param_idx),
                                    "gte" | "_gte" => format!("{} >= ${}", postrust_sql::escape_ident(key), param_idx),
                                    "lt" | "_lt" => format!("{} < ${}", postrust_sql::escape_ident(key), param_idx),
                                    "lte" | "_lte" => format!("{} <= ${}", postrust_sql::escape_ident(key), param_idx),
                                    "like" | "_like" => format!("{} LIKE ${}", postrust_sql::escape_ident(key), param_idx),
                                    "ilike" | "_ilike" => format!("{} ILIKE ${}", postrust_sql::escape_ident(key), param_idx),
                                    _ => continue,
                                };
                                conditions.push(condition);
                                values.push(BoundValue {
                                    column_name: key.clone(),
                                    value: op_val.clone(),
                                });
                                param_idx += 1;
                            }
                        }
                    }
                }
                _ => {
                    // Direct equality: {field: value}
                    conditions.push(format!("{} = ${}", postrust_sql::escape_ident(key), param_idx));
                    values.push(BoundValue {
                        column_name: key.clone(),
                        value: val.clone(),
                    });
                    param_idx += 1;
                }
            }
        }
    }

    let where_sql = if conditions.is_empty() {
        String::new()
    } else {
        format!("WHERE {}", conditions.join(" AND "))
    };

    Ok((where_sql, values))
}

/// Convert a GraphQL type string to a TypeRef.
fn graphql_type_ref(type_str: &str) -> TypeRef {
    // Parse type string like "[Users!]!" or "String" or "Int!"
    let is_list = type_str.starts_with('[');
    let is_nn = type_str.ends_with('!');

    // Strip outer modifiers: first the trailing !, then the brackets
    let inner = if is_list {
        let stripped = type_str
            .trim_end_matches('!')  // Remove outer !
            .trim_start_matches('[')  // Remove [
            .trim_end_matches(']');   // Remove ]
        stripped
    } else {
        type_str.trim_end_matches('!')
    };

    let inner_nn = inner.ends_with('!');
    let base_type = inner.trim_end_matches('!');

    if is_list {
        if is_nn {
            if inner_nn {
                TypeRef::named_nn_list_nn(base_type)
            } else {
                TypeRef::named_list_nn(base_type)
            }
        } else if inner_nn {
            TypeRef::named_nn_list(base_type)
        } else {
            TypeRef::named_list(base_type)
        }
    } else if is_nn {
        TypeRef::named_nn(base_type)
    } else {
        TypeRef::named(base_type)
    }
}

/// Convert ValueAccessor to JSON.
fn accessor_to_json(accessor: &ValueAccessor<'_>) -> serde_json::Value {
    // Use the deserialize method if available, or convert manually
    if accessor.is_null() {
        serde_json::Value::Null
    } else if let Ok(b) = accessor.boolean() {
        serde_json::Value::Bool(b)
    } else if let Ok(i) = accessor.i64() {
        serde_json::Value::Number(i.into())
    } else if let Ok(f) = accessor.f64() {
        serde_json::Number::from_f64(f)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null)
    } else if let Ok(s) = accessor.string() {
        serde_json::Value::String(s.to_string())
    } else if let Ok(list) = accessor.list() {
        serde_json::Value::Array(
            list.iter()
                .map(|v| accessor_to_json(&v))
                .collect()
        )
    } else if let Ok(obj) = accessor.object() {
        let map: serde_json::Map<String, serde_json::Value> = obj
            .iter()
            .map(|(k, v)| (k.to_string(), accessor_to_json(&v)))
            .collect();
        serde_json::Value::Object(map)
    } else {
        serde_json::Value::Null
    }
}

/// Convert async-graphql Value to JSON.
#[allow(dead_code)]
fn value_to_json(value: &Value) -> serde_json::Value {
    match value {
        Value::Null => serde_json::Value::Null,
        Value::Boolean(b) => serde_json::Value::Bool(*b),
        Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                serde_json::Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                serde_json::Value::Number(serde_json::Number::from_f64(f).unwrap())
            } else {
                serde_json::Value::Null
            }
        }
        Value::String(s) => serde_json::Value::String(s.clone()),
        Value::List(arr) => {
            serde_json::Value::Array(arr.iter().map(value_to_json).collect())
        }
        Value::Object(obj) => {
            let map: serde_json::Map<String, serde_json::Value> = obj
                .iter()
                .map(|(k, v)| (k.to_string(), value_to_json(v)))
                .collect();
            serde_json::Value::Object(map)
        }
        Value::Binary(b) => serde_json::Value::String(base64::Engine::encode(
            &base64::engine::general_purpose::STANDARD,
            b,
        )),
        Value::Enum(e) => serde_json::Value::String(e.to_string()),
    }
}

/// Convert JSON to async-graphql Value.
fn json_to_value(json: serde_json::Value) -> Value {
    match json {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Boolean(b),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Value::Number(i.into())
            } else if let Some(f) = n.as_f64() {
                Value::Number(async_graphql::Number::from_f64(f).unwrap())
            } else {
                Value::Null
            }
        }
        serde_json::Value::String(s) => Value::String(s),
        serde_json::Value::Array(arr) => {
            Value::List(arr.into_iter().map(json_to_value).collect())
        }
        serde_json::Value::Object(obj) => {
            let map: indexmap::IndexMap<async_graphql::Name, Value> = obj
                .into_iter()
                .map(|(k, v)| (async_graphql::Name::new(k), json_to_value(v)))
                .collect();
            Value::Object(map)
        }
    }
}

fn subscription_event_value(payload: &TableChangePayload) -> Option<serde_json::Value> {
    match payload.data() {
        Some(value) => Some(value.clone()),
        None => None,
    }
}

/// Create BigInt scalar type.
fn create_bigint_scalar() -> Scalar {
    Scalar::new("BigInt")
        .description("64-bit integer")
        .specified_by_url("https://spec.graphql.org/draft/#sec-Int")
}

/// Create BigDecimal scalar type.
fn create_bigdecimal_scalar() -> Scalar {
    Scalar::new("BigDecimal")
        .description("Arbitrary precision decimal number")
}

/// Create JSON scalar type.
fn create_json_scalar() -> Scalar {
    Scalar::new("JSON")
        .description("Arbitrary JSON value")
        .specified_by_url("https://spec.graphql.org/draft/#sec-Scalars")
}

/// Create UUID scalar type.
fn create_uuid_scalar() -> Scalar {
    Scalar::new("UUID").description("UUID string")
}

/// Create Date scalar type.
fn create_date_scalar() -> Scalar {
    Scalar::new("Date").description("ISO 8601 date string (YYYY-MM-DD)")
}

/// Create DateTime scalar type.
fn create_datetime_scalar() -> Scalar {
    Scalar::new("DateTime").description("ISO 8601 datetime string")
}

/// Create Time scalar type.
fn create_time_scalar() -> Scalar {
    Scalar::new("Time").description("ISO 8601 time string (HH:MM:SS)")
}

/// Register filter input types.
fn register_filter_input_types(builder: SchemaBuilder) -> SchemaBuilder {
    let string_filter = InputObject::new("StringFilterInput")
        .field(InputValue::new("eq", TypeRef::named("String")))
        .field(InputValue::new("neq", TypeRef::named("String")))
        .field(InputValue::new("like", TypeRef::named("String")))
        .field(InputValue::new("ilike", TypeRef::named("String")))
        .field(InputValue::new("in", TypeRef::named_list("String")))
        .field(InputValue::new("isNull", TypeRef::named("Boolean")));

    let int_filter = InputObject::new("IntFilterInput")
        .field(InputValue::new("eq", TypeRef::named("Int")))
        .field(InputValue::new("neq", TypeRef::named("Int")))
        .field(InputValue::new("gt", TypeRef::named("Int")))
        .field(InputValue::new("gte", TypeRef::named("Int")))
        .field(InputValue::new("lt", TypeRef::named("Int")))
        .field(InputValue::new("lte", TypeRef::named("Int")))
        .field(InputValue::new("in", TypeRef::named_list("Int")));

    let boolean_filter = InputObject::new("BooleanFilterInput")
        .field(InputValue::new("eq", TypeRef::named("Boolean")));

    builder
        .register(string_filter)
        .register(int_filter)
        .register(boolean_filter)
}

#[cfg(test)]
mod tests {
    use super::*;
    use indexmap::IndexMap;
    use postrust_core::schema_cache::{Column, Table};
    use std::collections::{HashMap, HashSet};

    fn create_test_table(name: &str) -> Table {
        let mut columns = IndexMap::new();
        columns.insert(
            "id".into(),
            Column {
                name: "id".into(),
                description: None,
                nullable: false,
                data_type: "integer".into(),
                nominal_type: "int4".into(),
                max_len: None,
                default: Some("nextval('id_seq')".into()),
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
            name: name.into(),
            description: None,
            is_view: false,
            insertable: true,
            updatable: true,
            deletable: true,
            pk_cols: vec!["id".into()],
            columns,
        }
    }

    fn create_test_schema_cache() -> SchemaCache {
        let mut tables = HashMap::new();
        let users = create_test_table("users");
        tables.insert(users.qualified_identifier(), users);

        SchemaCache {
            tables,
            relationships: HashMap::new(),
            routines: HashMap::new(),
            timezones: HashSet::new(),
            pg_version: 150000,
        }
    }

    fn column_with_type(data_type: &str, nullable: bool) -> Column {
        Column {
            name: "value".into(),
            description: None,
            nullable,
            data_type: data_type.into(),
            nominal_type: data_type.into(),
            max_len: None,
            default: None,
            enum_values: vec![],
            is_pk: false,
            position: 1,
        }
    }

    #[test]
    fn test_integer_json_value_uses_float_binding_for_float_column() {
        let column = column_with_type("double precision", false);
        let binding = coerce_column_value(&column, &serde_json::json!(12)).unwrap();

        assert!(matches!(binding, Some(TypedColumnValue::F64(Some(12.0)))));
    }

    #[test]
    fn test_fractional_json_value_is_rejected_for_integer_column() {
        let column = column_with_type("bigint", false);
        let result = coerce_column_value(&column, &serde_json::json!(12.5));

        assert!(result.is_err());
    }

    #[test]
    fn test_null_uses_column_specific_float_binding() {
        let column = column_with_type("double precision", true);
        let binding = coerce_column_value(&column, &serde_json::Value::Null).unwrap();

        assert!(matches!(binding, Some(TypedColumnValue::F64(None))));
    }

    // ============================================================================
    // Type Reference Tests
    // ============================================================================

    #[test]
    fn test_graphql_type_ref_simple() {
        let _type_ref = graphql_type_ref("String");
        // TypeRef doesn't implement PartialEq, so we just test it doesn't panic
    }

    #[test]
    fn test_graphql_type_ref_non_null() {
        let _type_ref = graphql_type_ref("String!");
    }

    #[test]
    fn test_graphql_type_ref_list() {
        let _type_ref = graphql_type_ref("[String]");
    }

    #[test]
    fn test_graphql_type_ref_list_non_null() {
        let _type_ref = graphql_type_ref("[String!]!");
    }

    // ============================================================================
    // Value Conversion Tests
    // ============================================================================

    #[test]
    fn test_value_to_json_null() {
        let value = Value::Null;
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::Value::Null);
    }

    #[test]
    fn test_value_to_json_boolean() {
        let value = Value::Boolean(true);
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::Value::Bool(true));
    }

    #[test]
    fn test_value_to_json_number() {
        let value = Value::Number(42.into());
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::json!(42));
    }

    #[test]
    fn test_value_to_json_string() {
        let value = Value::String("hello".to_string());
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::Value::String("hello".to_string()));
    }

    #[test]
    fn test_value_to_json_list() {
        let value = Value::List(vec![Value::Number(1.into()), Value::Number(2.into())]);
        let json = value_to_json(&value);
        assert_eq!(json, serde_json::json!([1, 2]));
    }

    #[test]
    fn test_json_to_value_null() {
        let json = serde_json::Value::Null;
        let value = json_to_value(json);
        assert!(matches!(value, Value::Null));
    }

    #[test]
    fn test_json_to_value_boolean() {
        let json = serde_json::Value::Bool(false);
        let value = json_to_value(json);
        assert!(matches!(value, Value::Boolean(false)));
    }

    #[test]
    fn test_json_to_value_number() {
        let json = serde_json::json!(123);
        let value = json_to_value(json);
        assert!(matches!(value, Value::Number(_)));
    }

    #[test]
    fn test_json_to_value_string() {
        let json = serde_json::Value::String("test".to_string());
        let value = json_to_value(json);
        assert!(matches!(value, Value::String(_)));
    }

    #[test]
    fn test_json_to_value_array() {
        let json = serde_json::json!([1, 2, 3]);
        let value = json_to_value(json);
        assert!(matches!(value, Value::List(_)));
    }

    #[test]
    fn test_json_to_value_object() {
        let json = serde_json::json!({"key": "value"});
        let value = json_to_value(json);
        assert!(matches!(value, Value::Object(_)));
    }

    #[test]
    fn test_subscription_event_value_returns_data() {
        let payload = TableChangePayload {
            operation: "DELETE".to_string(),
            table: "users".to_string(),
            schema: "public".to_string(),
            old: Some(serde_json::json!({
                "id": 6,
                "name": "Alice2"
            })),
            new: None,
        };

        let value = subscription_event_value(&payload).unwrap();
        assert_eq!(value["id"], 6);
        assert_eq!(value["name"], "Alice2");
    }

    // ============================================================================
    // Schema Building Tests
    // ============================================================================

    #[test]
    fn test_build_dynamic_schema() {
        let cache = create_test_schema_cache();
        let config = SchemaConfig::default();
        let generated = build_schema(&cache, &config);

        let result = build_dynamic_schema(&generated, &cache, None, &config);
        if let Err(ref e) = result {
            eprintln!("Schema build error: {:?}", e);
        }
        assert!(result.is_ok(), "Schema build failed: {:?}", result.err());
    }

    #[test]
    fn test_create_object_type() {
        let table = create_test_table("users");
        let obj = TableObjectType::from_table(&table);
        let _gql_obj = create_object_type(&obj, &[], false, false);
    }

    #[test]
    fn test_create_query_type() {
        let cache = create_test_schema_cache();
        let config = SchemaConfig::default();
        let generated = build_schema(&cache, &config);

        let _query = create_query_type(&generated, false);
    }

    #[test]
    fn test_create_mutation_type() {
        let cache = create_test_schema_cache();
        let config = SchemaConfig::default();
        let generated = build_schema(&cache, &config);

        let _mutation = create_mutation_type(&generated);
    }

    // ============================================================================
    // Scalar Tests
    // ============================================================================

    #[test]
    fn test_create_scalars() {
        let _bigint = create_bigint_scalar();
        let _json = create_json_scalar();
        let _uuid = create_uuid_scalar();
        let _datetime = create_datetime_scalar();
    }

    // ============================================================================
    // Filter Input Type Tests
    // ============================================================================

    #[test]
    fn test_register_filter_input_types() {
        let cache = create_test_schema_cache();
        let config = SchemaConfig::default();
        let _generated = build_schema(&cache, &config);

        // Build a minimal schema with filter types
        let query = Object::new("Query").field(Field::new(
            "test",
            TypeRef::named("String"),
            |_| FieldFuture::new(async { Ok(None::<FieldValue>) }),
        ));

        let mut builder = Schema::build("Query", None::<&str>, None);
        builder = builder.register(query);
        builder = register_filter_input_types(builder);

        let result = builder.finish();
        assert!(result.is_ok());
    }

    // ============================================================================
    // Subscription Tests
    // ============================================================================

    #[test]
    fn test_build_schema_with_subscriptions() {
        let cache = create_test_schema_cache();
        let config = SchemaConfig {
            enable_subscriptions: true,
            ..SchemaConfig::default()
        };
        let generated = build_schema(&cache, &config);

        // Generate subscription fields
        let sub_fields = generate_subscription_fields(&cache, &generated);
        assert!(!sub_fields.is_empty(), "Should have subscription fields");

        // Build schema with subscriptions
        let result = build_dynamic_schema(&generated, &cache, Some(&sub_fields), &config);
        assert!(result.is_ok(), "Schema with subscriptions should build");
    }

    #[test]
    fn test_subscription_field_generation() {
        let cache = create_test_schema_cache();
        let config = SchemaConfig::default();
        let generated = build_schema(&cache, &config);

        let fields = generate_subscription_fields(&cache, &generated);

        // Should have one subscription field for the users table
        assert_eq!(fields.len(), 1);
        assert_eq!(fields[0].name, "users");
        assert_eq!(fields[0].table_name, "users");
        assert_eq!(fields[0].channel_name(), "postrust_public_users");
    }

    #[test]
    fn test_create_subscription_type() {
        use crate::subscription::SubscriptionField as SubField;

        let fields = vec![
            SubField::for_table("public", "users", "Users"),
            SubField::for_table("public", "orders", "Orders"),
        ];

        let _subscription = create_subscription_type(&fields);
        // Just test that it doesn't panic
    }

    // ============================================================================
    // build_list_sql Tests
    // ============================================================================

    #[test]
    fn test_build_list_sql_no_args() {
        let (sql, values) = build_list_sql("public", "users", None, None, None, None).unwrap();
        assert_eq!(
            sql,
            r#"SELECT row_to_json(t) FROM (SELECT * FROM "public"."users" ) t"#
        );
        assert!(values.is_empty());
    }

    #[test]
    fn test_build_list_sql_with_limit_and_offset() {
        let (sql, values) =
            build_list_sql("public", "users", None, None, Some(10), Some(20)).unwrap();
        assert!(sql.contains("LIMIT 10"));
        assert!(sql.contains("OFFSET 20"));
        assert!(values.is_empty());
    }

    #[test]
    fn test_build_list_sql_with_order_by_asc() {
        let order = vec!["name_ASC".to_string()];
        let (sql, _) =
            build_list_sql("public", "users", None, Some(&order), None, None).unwrap();
        assert!(
            sql.contains(r#"ORDER BY "name" ASC"#),
            "Expected ORDER BY clause in SQL: {}",
            sql
        );
    }

    #[test]
    fn test_build_list_sql_with_order_by_desc() {
        let order = vec!["createdAt_DESC".to_string()];
        let (sql, _) =
            build_list_sql("public", "users", None, Some(&order), None, None).unwrap();
        assert!(
            sql.contains(r#"ORDER BY "createdAt" DESC"#),
            "Expected ORDER BY clause in SQL: {}",
            sql
        );
    }

    #[test]
    fn test_build_list_sql_with_multiple_order_by() {
        let order = vec!["name_ASC".to_string(), "id_DESC".to_string()];
        let (sql, _) =
            build_list_sql("public", "users", None, Some(&order), None, None).unwrap();
        assert!(
            sql.contains(r#"ORDER BY "name" ASC, "id" DESC"#),
            "Expected multi-column ORDER BY in SQL: {}",
            sql
        );
    }

    #[test]
    fn test_build_list_sql_with_filter_eq() {
        let filter = serde_json::json!({ "status": { "eq": "active" } });
        let (sql, values) =
            build_list_sql("public", "users", Some(&filter), None, None, None).unwrap();
        assert!(
            sql.contains("WHERE"),
            "Expected WHERE clause in SQL: {}",
            sql
        );
        assert!(
            sql.contains(r#""status" = $1"#),
            "Expected parameterized equality in SQL: {}",
            sql
        );
        assert_eq!(values.len(), 1);
        assert_eq!(values[0], serde_json::json!("active"));
    }

    #[test]
    fn test_build_list_sql_with_filter_in() {
        let filter = serde_json::json!({ "role": { "in": ["admin", "editor"] } });
        let (sql, values) =
            build_list_sql("public", "users", Some(&filter), None, None, None).unwrap();
        assert!(
            sql.contains("WHERE"),
            "Expected WHERE clause in SQL: {}",
            sql
        );
        assert_eq!(values.len(), 2);
        assert_eq!(values[0], serde_json::json!("admin"));
        assert_eq!(values[1], serde_json::json!("editor"));
    }

    #[test]
    fn test_build_list_sql_with_filter_and_order_and_paging() {
        let filter = serde_json::json!({ "status": { "eq": "active" } });
        let order = vec!["name_ASC".to_string()];
        let (sql, values) = build_list_sql(
            "public",
            "users",
            Some(&filter),
            Some(&order),
            Some(25),
            Some(50),
        )
        .unwrap();
        assert!(sql.contains("WHERE"), "Missing WHERE: {}", sql);
        assert!(sql.contains(r#"ORDER BY "name" ASC"#), "Missing ORDER BY: {}", sql);
        assert!(sql.contains("LIMIT 25"), "Missing LIMIT: {}", sql);
        assert!(sql.contains("OFFSET 50"), "Missing OFFSET: {}", sql);
        assert_eq!(values.len(), 1);
    }

    #[test]
    fn test_build_list_sql_escapes_table_name() {
        let (sql, _) =
            build_list_sql("public", "user accounts", None, None, None, None).unwrap();
        assert!(
            sql.contains(r#""public"."user accounts""#),
            "Table name not escaped: {}",
            sql
        );
    }

    // ============================================================================
    // build_where_clause Tests
    // ============================================================================

    #[test]
    fn test_build_where_clause_none() {
        let (sql, values) = build_where_clause(None, 1).unwrap();
        assert_eq!(sql, "");
        assert!(values.is_empty());
    }

    #[test]
    fn test_build_where_clause_eq() {
        let filter = serde_json::json!({ "name": { "eq": "Alice" } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE "name" = $1"#);
        assert_eq!(values, vec![serde_json::json!("Alice")]);
    }

    #[test]
    fn test_build_where_clause_neq() {
        let filter = serde_json::json!({ "status": { "neq": "inactive" } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE "status" != $1"#);
        assert_eq!(values, vec![serde_json::json!("inactive")]);
    }

    #[test]
    fn test_build_where_clause_gt_lt() {
        let filter = serde_json::json!({ "age": { "gt": 18, "lt": 65 } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert!(sql.contains("WHERE"));
        assert_eq!(values.len(), 2);
    }

    #[test]
    fn test_build_where_clause_in_single() {
        let filter = serde_json::json!({ "id": { "in": [42] } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE "id" = $1"#);
        assert_eq!(values, vec![serde_json::json!(42)]);
    }

    #[test]
    fn test_build_where_clause_in_multiple() {
        let filter = serde_json::json!({ "id": { "in": [1, 2, 3] } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE ("id" = $1 OR "id" = $2 OR "id" = $3)"#);
        assert_eq!(values.len(), 3);
    }

    #[test]
    fn test_build_where_clause_in_empty() {
        let filter = serde_json::json!({ "id": { "in": [] } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, "WHERE FALSE");
        assert!(values.is_empty());
    }

    #[test]
    fn test_build_where_clause_is_null() {
        let filter = serde_json::json!({ "email": { "is_null": true } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE "email" IS NULL"#);
        assert!(values.is_empty());
    }

    #[test]
    fn test_build_where_clause_direct_equality() {
        let filter = serde_json::json!({ "name": "Bob" });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE "name" = $1"#);
        assert_eq!(values, vec![serde_json::json!("Bob")]);
    }

    #[test]
    fn test_build_where_clause_param_idx_offset() {
        let filter = serde_json::json!({ "name": { "eq": "Alice" } });
        let (sql, _) = build_where_clause(Some(&filter), 5).unwrap();
        assert_eq!(sql, r#"WHERE "name" = $5"#);
    }

    #[test]
    fn test_build_where_clause_like() {
        let filter = serde_json::json!({ "name": { "like": "%test%" } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE "name" LIKE $1"#);
        assert_eq!(values, vec![serde_json::json!("%test%")]);
    }

    #[test]
    fn test_build_where_clause_ilike() {
        let filter = serde_json::json!({ "name": { "ilike": "%TEST%" } });
        let (sql, values) = build_where_clause(Some(&filter), 1).unwrap();
        assert_eq!(sql, r#"WHERE "name" ILIKE $1"#);
        assert_eq!(values, vec![serde_json::json!("%TEST%")]);
    }
}
