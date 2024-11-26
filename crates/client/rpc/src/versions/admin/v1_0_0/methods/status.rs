use std::time::Duration;

use jsonrpsee::core::async_trait;

use crate::{
    errors::ErrorExtWs,
    versions::admin::v1_0_0::{unix_now, MadaraStatusRpcApiV1_0_0Server},
    Starknet,
};

#[async_trait]
impl MadaraStatusRpcApiV1_0_0Server for Starknet {
    /// Can be used to check node availability and network latency
    ///
    /// # Returns
    ///
    /// * Ping time in unix time.
    async fn ping(&self) -> jsonrpsee::core::RpcResult<u64> {
        Ok(unix_now())
    }

    /// Stops the node by gracefully shutting down each of its services.
    ///
    /// # Returns
    ///
    /// * Time of shutdown in unix time.
    #[tracing::instrument(skip(self), fields(module = "Admin"))]
    async fn shutdown(&self) -> jsonrpsee::core::RpcResult<u64> {
        self.ctx.cancel_global();
        tracing::info!("🔌 Shutting down node...");
        Ok(unix_now())
    }

    /// Periodically sends a signal that the node is alive.
    ///
    /// # Sends
    ///
    /// * Current time in unix time
    async fn pulse(
        &self,
        subscription_sink: jsonrpsee::PendingSubscriptionSink,
    ) -> jsonrpsee::core::SubscriptionResult {
        let sink =
            subscription_sink.accept().await.or_internal_server_error("Failed to establish websocket connection")?;

        while !self.ctx.is_cancelled() {
            let now = unix_now();
            let msg = jsonrpsee::SubscriptionMessage::from_json(&now)
                .or_else_internal_server_error(|| format!("Failed to create response message at unix time {now}"))?;
            sink.send(msg).await.or_internal_server_error("Failed to respond to websocket request")?;

            tokio::time::sleep(Duration::from_secs(10)).await;
        }

        Ok(())
    }
}