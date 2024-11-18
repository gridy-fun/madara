use std::sync::Arc;

use jsonrpsee::server::ServerHandle;
use tokio::task::JoinSet;

use mc_db::DatabaseService;
use mc_rpc::{providers::AddTransactionProvider, rpc_api_external, rpc_api_internal, Starknet};
use mp_utils::service::Service;

use metrics::RpcMetrics;
use server::{start_server, ServerConfig};

use crate::cli::{RpcMethods, RpcParams};

use self::server::rpc_api_build;

mod metrics;
mod middleware;
mod server;

pub struct RpcService {
    server_config_user: Option<ServerConfig>,
    server_config_admin: Option<ServerConfig>,
    server_handle_user: Option<ServerHandle>,
    server_handle_admin: Option<ServerHandle>,
}
impl RpcService {
    pub fn new(
        config: &RpcParams,
        db: &DatabaseService,
        add_txs_method_provider: Arc<dyn AddTransactionProvider>,
    ) -> anyhow::Result<Self> {
        if config.rpc_disabled {
            return Ok(Self {
                server_config_user: None,
                server_config_admin: None,
                server_handle_user: None,
                server_handle_admin: None,
            });
        }

        let (rpcs, node_operator) = match (config.rpc_methods, config.rpc_external) {
            (RpcMethods::Safe, _) => (true, false),
            (RpcMethods::Unsafe, _) => (true, true),
            (RpcMethods::Auto, false) => (true, true),
            (RpcMethods::Auto, true) => {
                tracing::warn!(
                    "Option `--rpc-external` will hide node operator endpoints. To enable them, please pass \
                     `--rpc-methods unsafe`."
                );
                (true, false)
            }
        };
        let (read, write, trace, admin, ws) = (rpcs, rpcs, rpcs, node_operator, rpcs);
        let starknet = Starknet::new(Arc::clone(db.backend()), add_txs_method_provider);
        let metrics = RpcMetrics::register()?;

        let api_rpc_user = rpc_api_external(&starknet, read, write, trace, ws)?;
        let methods_user = rpc_api_build("rpc", api_rpc_user).into();

        let server_config_user = Some(ServerConfig {
            name: "JSON-RPC".to_string(),
            addr: config.addr_user(),
            batch_config: config.batch_config(),
            max_connections: config.rpc_max_connections,
            max_payload_in_mb: config.rpc_max_request_size,
            max_payload_out_mb: config.rpc_max_response_size,
            max_subs_per_conn: config.rpc_max_subscriptions_per_connection,
            message_buffer_capacity: config.rpc_message_buffer_capacity_per_connection,
            methods: methods_user,
            metrics: metrics.clone(),
            cors: config.cors(),
        });

        let server_config_admin = if admin {
            let api_rpc_admin = rpc_api_internal(&starknet)?;
            let methods_admin = rpc_api_build("admin", api_rpc_admin).into();

            Some(ServerConfig {
                name: "JSON-RPC (Admin)".to_string(),
                addr: config.addr_admin(),
                batch_config: config.batch_config(),
                max_connections: config.rpc_max_connections,
                max_payload_in_mb: config.rpc_max_request_size,
                max_payload_out_mb: config.rpc_max_response_size,
                max_subs_per_conn: config.rpc_max_subscriptions_per_connection,
                message_buffer_capacity: config.rpc_message_buffer_capacity_per_connection,
                methods: methods_admin,
                metrics,
                cors: config.cors(),
            })
        } else {
            None
        };

        Ok(Self { server_config_user, server_config_admin, server_handle_user: None, server_handle_admin: None })
    }
}

#[async_trait::async_trait]
impl Service for RpcService {
    async fn start(&mut self, join_set: &mut JoinSet<anyhow::Result<()>>) -> anyhow::Result<()> {
        if let Some(server_config) = &self.server_config_user {
            // rpc enabled
            self.server_handle_user = Some(start_server(server_config.clone(), join_set).await?);
        }

        if let Some(server_config) = &self.server_config_admin {
            // rpc enabled (admin)
            self.server_handle_admin = Some(start_server(server_config.clone(), join_set).await?);
        }

        Ok(())
    }
}