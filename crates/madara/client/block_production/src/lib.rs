//! Block production service.
//!
//! # Testing
//!
//! Testing is done in a few places:
//! - devnet has a few tests for declare transactions and basic transfers as of now. This is proobably
//!   the simplest place where we could add more tests about block-time, mempool saving to db and such.
//! - e2e tests test a few transactions, with the rpc/gateway in scope too.
//! - js-tests in the CI, not that much in depth
//! - higher level, block production is more hreavily tested (currently outside of the CI) by running the
//!   bootstrapper and the kakarot test suite. This is the only place where L1-L2 messaging is really tested
//!   as of now. We should make better tests around this area.
//!
//! There are no tests in this crate because they would require a proper genesis block. Devnet provides that,
//! so that's where block-production integration tests are the simplest to add.
//! L1-L2 testing is a bit harder to setup, but we should definitely make the testing more comprehensive here.

use crate::close_block::close_block;
use crate::metrics::BlockProductionMetrics;
use blockifier::blockifier::transaction_executor::{
    TransactionExecutor, TransactionExecutorError, BLOCK_STATE_ACCESS_ERR,
};
use blockifier::bouncer::BouncerWeights;
use blockifier::transaction::errors::TransactionExecutionError;
use blockifier::transaction::objects::TransactionExecutionInfo;
use finalize_execution_state::StateDiffToStateMapError;
use mc_block_import::{BlockImportError, BlockImporter};
use mc_db::db_block_id::DbBlockId;
use mc_db::{MadaraBackend, MadaraStorageError};
use mc_devnet::{Call, Multicall, Selector};
use mc_exec::{BlockifierStateAdapter, ExecutionContext};
use mc_mempool::header::make_pending_header;
use mc_mempool::{transaction_hash, L1DataProvider, MempoolProvider};
use mp_block::{BlockId, BlockTag, MadaraPendingBlock, VisitedSegments};
use mp_class::compile::ClassCompilationError;
use mp_class::ConvertedClass;
use mp_convert::ToFelt;
use mp_receipt::from_blockifier_execution_info;
use mp_state_update::{ContractStorageDiffItem, DeclaredClassItem, NonceUpdate, StateDiff, StorageEntry};
use mp_transactions::TransactionWithHash;
use mp_utils::service::ServiceContext;
use opentelemetry::KeyValue;
use rand::{thread_rng, Rng};
use starknet::signers::SigningKey;
use starknet_api::transaction::EventKey;
use starknet_types_core::felt::Felt;
use starknet_types_rpc::{BroadcastedInvokeTxn, BroadcastedTxn, InvokeTxnV1};
use std::borrow::Cow;
use std::collections::VecDeque;
use std::mem;
use std::str::FromStr as _;
use std::sync::Arc;
use std::time::Instant;

mod close_block;
mod finalize_execution_state;
pub mod metrics;

#[derive(Default, Clone)]
struct ContinueBlockStats {
    /// Number of batches executed before reaching the bouncer capacity.
    pub n_batches: usize,
    /// Number of transactions included into the block
    pub n_added_to_block: usize,
    pub n_re_added_to_mempool: usize,
    pub n_reverted: usize,
    /// Rejected are txs that were unsucessful and but that were not revertible.
    pub n_rejected: usize,
}

// TODO: might wanna remove the 0 in the start
const SPAWNED_BOT_EVENT_SELECTOR: &str = "0x2cd0383e81a65036ae8acc94ac89e891d1385ce01ae6cc127c27615f5420fa3";
const BOMB_FOUND_EVENT_SELECTOR: &str = "0x111861367b42e77c11a98efb6d09a14c2dc470eee1a4d2c3c1e8c54015da2e5";
const DIAMOND_FOUND_EVENT_SELECTOR: &str = "0x14528085c8fd64b9210572c5b6015468f8352c17c9c22f5b7aa62a55a56d8d7";
const TILE_MINED_SELECTOR: &str = "0xd5efc9cfb6a4f6bb9eae0ce39d32480473877bb3f7a4eaa3944c881a2c8d25";
const TILE_ALREADY_MINED_SELECTOR: &str = "0x1b74d97806c93468070e49a1626aba00f8e89dfb07246492af4566f898de982";
const SUSPEND_BOT_SELECTOR: &str = "0x1dcca826eea45d96bfbf26e9aabf510e94c6de62d0ce5e5b6e60c51c7640af8";
const REVIVE_BOT_SELECTOR: &str = "0x1d6a6a42fd13b206a721dbca3ae720621707ef3016850e2c5536244e5a7858a";

#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("Storage error: {0:#}")]
    StorageError(#[from] MadaraStorageError),
    #[error("Execution error: {0:#}")]
    Execution(#[from] TransactionExecutionError),
    #[error(transparent)]
    ExecutionContext(#[from] mc_exec::Error),
    #[error("Import error: {0:#}")]
    Import(#[from] mc_block_import::BlockImportError),
    #[error("Unexpected error: {0:#}")]
    Unexpected(Cow<'static, str>),
    #[error("Class compilation error when continuing the pending block: {0:#}")]
    PendingClassCompilationError(#[from] ClassCompilationError),
    #[error("State diff error when continuing the pending block: {0:#}")]
    PendingStateDiff(#[from] StateDiffToStateMapError),
}

/// Result of a block continuation operation, containing the updated state and execution statistics.
/// This is returned by [`BlockProductionTask::continue_block`] when processing a batch of transactions.
struct ContinueBlockResult {
    /// The accumulated state changes from executing transactions in this continuation
    state_diff: StateDiff,

    /// Tracks which segments of Cairo program code were accessed during transaction execution,
    /// organized by class hash. This information is used as input for SNOS (Starknet OS)
    /// when generating proofs of execution.
    visited_segments: VisitedSegments,

    /// The current state of resource consumption tracked by the bouncer
    bouncer_weights: BouncerWeights,

    /// Statistics about transaction processing during this continuation
    stats: ContinueBlockStats,

    /// Indicates whether the block reached its resource limits during this continuation.
    /// When true, no more transactions can be added to the current block.
    block_now_full: bool,
}

/// The block production task consumes transactions from the mempool in batches.
/// This is to allow optimistic concurrency. However, the block may get full during batch execution,
/// and we need to re-add the transactions back into the mempool.
///
/// To understand block production in madara, you should probably start with the [`mp_chain_config::ChainConfig`]
/// documentation.
pub struct BlockProductionTask<Mempool: MempoolProvider> {
    importer: Arc<BlockImporter>,
    backend: Arc<MadaraBackend>,
    mempool: Arc<Mempool>,
    block: MadaraPendingBlock,
    declared_classes: Vec<ConvertedClass>,
    pub(crate) executor: TransactionExecutor<BlockifierStateAdapter>,
    l1_data_provider: Arc<dyn L1DataProvider>,
    current_pending_tick: usize,
    metrics: Arc<BlockProductionMetrics>,
}

impl<Mempool: MempoolProvider> BlockProductionTask<Mempool> {
    #[cfg(any(test, feature = "testing"))]
    #[tracing::instrument(skip(self), fields(module = "BlockProductionTask"))]
    pub fn set_current_pending_tick(&mut self, n: usize) {
        self.current_pending_tick = n;
    }

    /// Closes the last pending block store in db (if any).
    ///
    /// This avoids re-executing transaction by re-adding them to the [Mempool],
    /// as was done before.
    pub async fn close_pending_block(
        backend: &MadaraBackend,
        importer: &BlockImporter,
        metrics: &BlockProductionMetrics,
    ) -> Result<(), Cow<'static, str>> {
        let err_pending_block = |err| format!("Getting pending block: {err:#}");
        let err_pending_state_diff = |err| format!("Getting pending state update: {err:#}");
        let err_pending_visited_segments = |err| format!("Getting pending visited segments: {err:#}");
        let err_pending_clear = |err| format!("Clearing pending block: {err:#}");
        let err_latest_block_n = |err| format!("Failed to get latest block number: {err:#}");

        let start_time = Instant::now();

        // We cannot use `backend.get_block` to check for the existence of the
        // pending block as it will ALWAYS return a pending block, even if there
        // is none in db (it uses the Default::default in that case).
        if !backend.has_pending_block().map_err(err_pending_block)? {
            return Ok(());
        }

        let pending_block = backend
            .get_block(&DbBlockId::Pending)
            .map_err(err_pending_block)?
            .map(|block| MadaraPendingBlock::try_from(block).expect("Ready block stored in place of pending"))
            .expect("Checked above");

        let pending_state_diff = backend.get_pending_block_state_update().map_err(err_pending_state_diff)?;
        let pending_visited_segments =
            backend.get_pending_block_segments().map_err(err_pending_visited_segments)?.unwrap_or_default();

        let mut classes = pending_state_diff
            .deprecated_declared_classes
            .iter()
            .chain(pending_state_diff.declared_classes.iter().map(|DeclaredClassItem { class_hash, .. }| class_hash));
        let capacity = pending_state_diff.deprecated_declared_classes.len() + pending_state_diff.declared_classes.len();

        let declared_classes = classes.try_fold(Vec::with_capacity(capacity), |mut acc, class_hash| {
            match backend.get_converted_class(&BlockId::Tag(BlockTag::Pending), class_hash) {
                Ok(Some(class)) => {
                    acc.push(class);
                    Ok(acc)
                }
                Ok(None) => {
                    Err(format!("Failed to retrieve pending declared class at hash {class_hash:x?}: not found in db"))
                }
                Err(err) => Err(format!("Failed to retrieve pending declared class at hash {class_hash:x?}: {err:#}")),
            }
        })?;

        // NOTE: we disabled the Write Ahead Log when clearing the pending block
        // so this will be done atomically at the same time as we close the next
        // block, after we manually flush the db.
        backend.clear_pending_block().map_err(err_pending_clear)?;

        let block_n = backend.get_latest_block_n().map_err(err_latest_block_n)?.map(|n| n + 1).unwrap_or(0);
        let n_txs = pending_block.inner.transactions.len();

        // Close and import the pending block
        close_block(
            importer,
            pending_block,
            &pending_state_diff,
            backend.chain_config().chain_id.clone(),
            block_n,
            declared_classes,
            pending_visited_segments,
        )
        .await
        .map_err(|err| format!("Failed to close pending block: {err:#}"))?;

        // Flush changes to disk, pending block removal and adding the next
        // block happens atomically
        backend.flush().map_err(|err| format!("DB flushing error: {err:#}"))?;

        let end_time = start_time.elapsed();
        tracing::info!("⛏️  Closed block #{} with {} transactions - {:?}", block_n, n_txs, end_time);

        // Record metrics
        let attributes = [
            KeyValue::new("transactions_added", n_txs.to_string()),
            KeyValue::new("closing_time", end_time.as_secs_f32().to_string()),
        ];

        metrics.block_counter.add(1, &[]);
        metrics.block_gauge.record(block_n, &attributes);
        metrics.transaction_counter.add(n_txs as u64, &[]);

        Ok(())
    }

    pub async fn new(
        backend: Arc<MadaraBackend>,
        importer: Arc<BlockImporter>,
        mempool: Arc<Mempool>,
        metrics: Arc<BlockProductionMetrics>,
        l1_data_provider: Arc<dyn L1DataProvider>,
    ) -> Result<Self, Error> {
        if let Err(err) = Self::close_pending_block(&backend, &importer, &metrics).await {
            // This error should not stop block production from working. If it happens, that's too bad. We drop the pending state and start from
            // a fresh one.
            tracing::error!("Failed to continue the pending block state: {err:#}");
        }

        let parent_block_hash = backend
            .get_block_hash(&BlockId::Tag(BlockTag::Latest))?
            .unwrap_or(/* genesis block's parent hash */ Felt::ZERO);

        let pending_block = MadaraPendingBlock::new_empty(make_pending_header(
            parent_block_hash,
            backend.chain_config(),
            l1_data_provider.as_ref(),
        ));

        let executor = ExecutionContext::new_at_block_start(Arc::clone(&backend), &pending_block.info.clone().into())?
            .tx_executor();

        Ok(Self {
            importer,
            backend,
            mempool,
            executor,
            current_pending_tick: 0,
            block: pending_block,
            declared_classes: Default::default(),
            l1_data_provider,
            metrics,
        })
    }

    #[tracing::instrument(skip(self), fields(module = "BlockProductionTask"))]
    fn continue_block(&mut self, bouncer_cap: BouncerWeights) -> Result<ContinueBlockResult, Error> {
        let mut stats = ContinueBlockStats::default();
        let mut block_now_full = false;

        self.executor.bouncer.bouncer_config.block_max_capacity = bouncer_cap;
        let batch_size = self.backend.chain_config().execution_batch_size;

        let mut txs_to_process = VecDeque::with_capacity(batch_size);
        let mut txs_to_process_blockifier = Vec::with_capacity(batch_size);
        // This does not need to be outside the loop, but that saves an allocation
        let mut executed_txs = Vec::with_capacity(batch_size);

        // Cloning transactions: That's a lot of cloning, but we're kind of forced to do that because blockifier takes
        // a `&[Transaction]` slice. In addition, declare transactions have their class behind an Arc.
        loop {
            // Take transactions from mempool.
            let to_take = batch_size.saturating_sub(txs_to_process.len());
            let cur_len = txs_to_process.len();
            if to_take > 0 {
                self.mempool.txs_take_chunk(/* extend */ &mut txs_to_process, batch_size);

                txs_to_process_blockifier.extend(txs_to_process.iter().skip(cur_len).map(|tx| tx.clone_tx()));
            }

            if txs_to_process.is_empty() {
                // Not enough transactions in mempool to make a new batch.
                break;
            }

            stats.n_batches += 1;

            // Execute the transactions.
            let all_results = self.executor.execute_txs(&txs_to_process_blockifier);

            // println!(">>> Execution returned with : {:?} within {:?} ", all_results.len(), end);

            // println!(">>> TXNS TO PROCESS BLOCKFIER LENGTH :  {:?} ", txs_to_process_blockifier.len());
            // println!(">>> TXNS TO PROCESS BLOCKFIER :  {:?} ", txs_to_process_blockifier);

            // let result: &Vec<Result<TransactionExecutionInfo, TransactionExecutorError>> = &all_results.as_ref();
            // let x = result.iter().map(|x| x.as_ref().unwrap()).collect::<Vec<&TransactionExecutionInfo>>()[0];
            // let n_steps = x.transaction_receipt.resources.vm_resources.n_steps;
            // println!(">>> N_STEPS  {:?}", n_steps);

            let _ress = self.listen_for_bot_events(&all_results).expect("Couldn't ingest Bot events");

            // When the bouncer cap is reached, blockifier will return fewer results than what we asked for.
            block_now_full = all_results.len() < txs_to_process_blockifier.len();

            txs_to_process_blockifier.drain(..all_results.len()); // remove the used txs

            for exec_result in all_results {
                let mut mempool_tx =
                    txs_to_process.pop_front().ok_or_else(|| Error::Unexpected("Vector length mismatch".into()))?;

                // Remove tx from mempool
                self.backend.remove_mempool_transaction(&mempool_tx.tx_hash().to_felt())?;

                match exec_result {
                    Ok(execution_info) => {
                        // Reverted transactions appear here as Ok too.
                        tracing::debug!("Successful execution of transaction {:#x}", mempool_tx.tx_hash().to_felt());

                        stats.n_added_to_block += 1;
                        if execution_info.is_reverted() {
                            stats.n_reverted += 1;
                        }

                        if let Some(class) = mem::take(&mut mempool_tx.converted_class) {
                            self.declared_classes.push(class);
                        }

                        // TODO: add here the event listening logic

                        self.block
                            .inner
                            .receipts
                            .push(from_blockifier_execution_info(&execution_info, &mempool_tx.clone_tx()));
                        let converted_tx = TransactionWithHash::from(mempool_tx.clone_tx());
                        self.block.info.tx_hashes.push(converted_tx.hash);
                        self.block.inner.transactions.push(converted_tx.transaction);
                    }
                    Err(err) => {
                        // These are the transactions that have errored but we can't revert them. It can be because of an internal server error, but
                        // errors during the execution of Declare and DeployAccount also appear here as they cannot be reverted.
                        // We reject them.
                        // Note that this is a big DoS vector.
                        tracing::error!(
                            "Rejected transaction {:#x} for unexpected error: {err:#}",
                            mempool_tx.tx_hash().to_felt()
                        );
                        stats.n_rejected += 1;
                    }
                }

                executed_txs.push(mempool_tx)
            }

            if block_now_full {
                break;
            }
        }

        let on_top_of = self.executor.block_state.as_ref().expect(BLOCK_STATE_ACCESS_ERR).state.on_top_of_block_id;

        let (state_diff, visited_segments, bouncer_weights) =
            finalize_execution_state::finalize_execution_state(&mut self.executor, &self.backend, &on_top_of)?;

        // Add back the unexecuted transactions to the mempool.
        stats.n_re_added_to_mempool = txs_to_process.len();
        self.mempool
            .txs_re_add(txs_to_process, executed_txs)
            .map_err(|err| Error::Unexpected(format!("Mempool error: {err:#}").into()))?;

        tracing::debug!(
            "Finished tick with {} new transactions, now at {} - re-adding {} txs to mempool",
            stats.n_added_to_block,
            self.block.inner.transactions.len(),
            stats.n_re_added_to_mempool
        );

        Ok(ContinueBlockResult { state_diff, visited_segments, bouncer_weights, stats, block_now_full })
    }

    pub fn listen_for_bot_events(
        &mut self,
        all_txns: &Vec<
            Result<
                blockifier::transaction::objects::TransactionExecutionInfo,
                blockifier::blockifier::transaction_executor::TransactionExecutorError,
            >,
        >,
    ) -> Result<(), Error> {
        // search through all_txns and get SpawnedBot or BombFound events

        let mut spawned_bots: Vec<String> = Vec::new();
        let mut killed_bots: Vec<String> = Vec::new();

        // events: [OrderedEvent { order: 0, event: EventContent { keys: [EventKey(0x2cd0383e81a65036ae8acc94ac89e891d1385ce01ae6cc127c27615f5420fa3)],
        // data: EventData([0x7484e8e3af210b2ead47fa08c96f8d18b616169b350a8b75fe0dc4d2e01d493, 0x1c9, 0x66dbd884899534c3ba7216743e8d0a683e3c5b5b8cac37441f55c1b43a8019c]) } }],

        // The logic below is written assuming that the event will have one key and 3 values.

        for tx in all_txns {
            if tx.is_err() {
                continue;
            }
            let tx = tx.as_ref().unwrap();

            let execute_call_info = &tx.execute_call_info.as_ref();

            if let Some(execute_call) = execute_call_info {
                let inner_calls: &Vec<blockifier::execution::call_info::CallInfo> = execute_call.inner_calls.as_ref();

                for values in inner_calls {
                    let ordered_events: &Vec<blockifier::execution::call_info::OrderedEvent> =
                        values.execution.events.as_ref();

                    for ordered_event in ordered_events.iter() {
                        let event = ordered_event.event.to_owned();

                        let spawned_bot_felt = Felt::from_str(SPAWNED_BOT_EVENT_SELECTOR)
                            .expect("Unable to convert selector string to felt"); // Or however you get your Felt value

                        let bomb_found_felt = Felt::from_str(BOMB_FOUND_EVENT_SELECTOR)
                            .expect("Unable to convert selector string to felt"); // Or however you get your Felt value

                        let diamond_found_felt = Felt::from_str(DIAMOND_FOUND_EVENT_SELECTOR)
                            .expect("Unable to convert selector string to felt"); // Or however you get your Felt value

                        let tile_mined_felt =
                            Felt::from_str(TILE_MINED_SELECTOR).expect("Unable to convert selector string to felt"); // Or however you get your Felt value

                        let tile_already_mined_felt = Felt::from_str(TILE_ALREADY_MINED_SELECTOR)
                            .expect("Unable to convert selector string to felt"); // Or however you get your Felt value

                        let suspend_bot_felt =
                            Felt::from_str(SUSPEND_BOT_SELECTOR).expect("Unable to convert selector string to felt"); // Or however you get your Felt value

                        let revive_bot_felt =
                            Felt::from_str(REVIVE_BOT_SELECTOR).expect("Unable to convert selector string to felt"); // Or however you get your Felt value

                        for key in event.keys {
                            // BombFound
                            if key == EventKey(bomb_found_felt) {
                                let bot_address = event.data.0[0].to_string();
                                let bot_location = event.data.0[1].to_string();
                                println!(
                                    ">>> Event : BombFound by {:?} at {:?}",
                                    Felt::from_str(bot_address.as_str()).expect("Could not get address"),
                                    bot_location
                                );
                                killed_bots.push(bot_address);
                            }
                            // DiamondFound
                            else if key == EventKey(diamond_found_felt) {
                                let bot_address = event.data.0[0].to_string();
                                let bot_points = event.data.0[1].to_string();
                                let bot_location = event.data.0[2].to_string();
                                println!(
                                    ">>> Event : DiamondFound by {:?} at {:?} for {:?}",
                                    Felt::from_str(bot_address.as_str()).expect("Could not get address"),
                                    bot_location,
                                    bot_points
                                );
                            }
                            // TileMined
                            else if key == EventKey(tile_mined_felt) {
                                let bot_address = event.data.0[0].to_string();
                                let points = event.data.0[1].to_string();
                                let location = event.data.0[2].to_string();
                                println!(
                                    ">>> Event : TileMined by {:?} at {:?} for {:?}",
                                    Felt::from_str(bot_address.as_str()).expect("Could not get address"),
                                    location,
                                    points
                                );
                                self.backend
                                    .game_update_metadata(|meta| {
                                        meta.tiles_mined += 1;
                                    })
                                    .expect("could not update the tiles mined number");
                            }
                            // TileAlreadyMined
                            else if key == EventKey(tile_already_mined_felt) {
                                let bot_address = event.data.0[0].to_string();
                                let bot_location = event.data.0[1].to_string();
                                println!(
                                    ">>> Event : TileAlreadyMined {:?} at {:?}",
                                    Felt::from_str(bot_address.as_str()).expect("Could not get address"),
                                    bot_location
                                );
                            }
                            // SpawnedBot
                            else if key == EventKey(spawned_bot_felt) {
                                let bot_address = event.data.0[0].to_string();
                                let player = event.data.0[1].to_string();
                                let bot_location = event.data.0[2].to_string();
                                println!(
                                    ">>> Event : SpawnedBot {:?} at {:?} by {:?}",
                                    Felt::from_str(bot_address.as_str()).expect("Could not get address"),
                                    bot_location,
                                    Felt::from_str(player.as_str()).expect("Could not get address")
                                );
                                spawned_bots.push(bot_address);
                            }
                            // SuspendBot
                            else if key == EventKey(suspend_bot_felt) {
                                let bot_address = event.data.0[0].to_string();
                                println!(
                                    ">>> Event : SuspendBot {:?}",
                                    Felt::from_str(bot_address.as_str()).expect("Could not get address")
                                );
                                // TODO: add kill bot here if needed.
                            }
                            // ReviveBot
                            else if key == EventKey(revive_bot_felt) {
                                let bot_address = event.data.0[0].to_string();
                                println!(
                                    ">>> Event : ReviveBot {:?}",
                                    Felt::from_str(bot_address.as_str()).expect("Could not get address")
                                );
                                // TODO: add spawned bot here if needed.
                            }
                        }
                    }
                }
            }
        }

        // Kill all the bots !
        // TODO: these are multiple DB operations, can it be clubbed to a single operation
        for killed_bot in killed_bots {
            let _ = self.backend.game_delete_bot_address(&killed_bot.as_str()).expect("Could not remove the bot");
        }

        // Add new bots !
        for spawned_bot in spawned_bots {
            let _ = self.backend.game_add_bot_address(&spawned_bot.as_str()).expect("Could not add the bot");
        }

        Ok(())
    }

    /// Closes the current block and prepares for the next one
    #[tracing::instrument(skip(self), fields(module = "BlockProductionTask"))]
    async fn close_and_prepare_next_block(
        &mut self,
        state_diff: StateDiff,
        visited_segments: VisitedSegments,
        start_time: Instant,
    ) -> Result<(), Error> {
        let block_n = self.block_n();
        // Convert the pending block to a closed block and save to db
        let parent_block_hash = Felt::ZERO; // temp parent block hash
        let new_empty_block = MadaraPendingBlock::new_empty(make_pending_header(
            parent_block_hash,
            self.backend.chain_config(),
            self.l1_data_provider.as_ref(),
        ));

        let block_to_close = mem::replace(&mut self.block, new_empty_block);
        let declared_classes = mem::take(&mut self.declared_classes);

        let n_txs = block_to_close.inner.transactions.len();

        // Close and import the block
        let import_result = close_block(
            &self.importer,
            block_to_close,
            &state_diff,
            self.backend.chain_config().chain_id.clone(),
            block_n,
            declared_classes,
            visited_segments,
        )
        .await?;

        // Removes nonces in the mempool nonce cache which have been included
        // into the current block.
        for NonceUpdate { contract_address, .. } in state_diff.nonces.iter() {
            self.mempool.tx_mark_included(contract_address);
        }

        // Flush changes to disk
        self.backend.flush().map_err(|err| BlockImportError::Internal(format!("DB flushing error: {err:#}").into()))?;

        // Update parent hash for new pending block
        self.block.info.header.parent_block_hash = import_result.block_hash;

        // Prepare executor for next block
        self.executor =
            ExecutionContext::new_at_block_start(Arc::clone(&self.backend), &self.block.info.clone().into())?
                .tx_executor();
        self.current_pending_tick = 0;

        let end_time = start_time.elapsed();
        tracing::info!("⛏️  Closed block #{} with {} transactions - {:?}", block_n, n_txs, end_time);

        // Record metrics
        let attributes = [
            KeyValue::new("transactions_added", n_txs.to_string()),
            KeyValue::new("closing_time", end_time.as_secs_f32().to_string()),
        ];

        self.metrics.block_counter.add(1, &[]);
        self.metrics.block_gauge.record(block_n, &attributes);
        self.metrics.transaction_counter.add(n_txs as u64, &[]);

        Ok(())
    }

    /// Updates the state diff to store a block hash at the special address 0x1, which serves as
    /// Starknet's block hash registry.
    ///
    /// # Purpose
    /// Address 0x1 in Starknet is a special contract address that maintains a mapping of block numbers
    /// to their corresponding block hashes. This storage is used by the `get_block_hash` system call
    /// and is essential for block hash verification within the Starknet protocol.
    ///
    /// # Storage Structure at Address 0x1
    /// - Keys: Block numbers
    /// - Values: Corresponding block hashes
    /// - Default: 0 for all other block numbers
    ///
    /// # Implementation Details
    /// For each block N ≥ 10, this function stores the hash of block (N-10) at address 0x1
    /// with the block number as the key.
    ///
    /// For more details, see the [official Starknet documentation on special addresses]
    /// (https://docs.starknet.io/architecture-and-concepts/network-architecture/starknet-state/#address_0x1)
    ///
    /// It is also required by SNOS for PIEs creation of the block.
    fn update_block_hash_registry(&self, state_diff: &mut StateDiff, block_n: u64) -> Result<(), Error> {
        if block_n >= 10 {
            let prev_block_number = block_n - 10;
            let prev_block_hash = self
                .backend
                .get_block_hash(&BlockId::Number(prev_block_number))
                .map_err(|err| {
                    Error::Unexpected(
                        format!("Error fetching block hash for block {prev_block_number}: {err:#}").into(),
                    )
                })?
                .ok_or_else(|| {
                    Error::Unexpected(format!("No block hash found for block number {prev_block_number}").into())
                })?;

            state_diff.storage_diffs.push(ContractStorageDiffItem {
                address: Felt::ONE, // Address 0x1
                storage_entries: vec![StorageEntry { key: Felt::from(prev_block_number), value: prev_block_hash }],
            });
        }
        Ok(())
    }

    #[tracing::instrument(skip(self), fields(module = "BlockProductionTask"))]
    pub async fn on_pending_time_tick(&mut self) -> Result<bool, Error> {
        dotenv().ok();
        let start = Instant::now();
        let current_pending_tick = self.current_pending_tick;
        if current_pending_tick == 0 {
            return Ok(false);
        }

        let start_time = Instant::now();

        let ContinueBlockResult {
            state_diff: mut new_state_diff,
            visited_segments,
            bouncer_weights,
            stats,
            block_now_full,
        } = self.continue_block(self.backend.chain_config().bouncer_config.block_max_capacity)?;

        if stats.n_added_to_block > 0 {
            tracing::info!(
                "🧮 Executed and added {} transaction(s) to the pending block at height {} - {:?}",
                stats.n_added_to_block,
                self.block_n(),
                start_time.elapsed(),
            );
        }

        // Check if block is full
        if block_now_full {
            let block_n = self.block_n();
            self.update_block_hash_registry(&mut new_state_diff, block_n)?;

            tracing::info!("Resource limits reached, closing block early");
            self.close_and_prepare_next_block(new_state_diff, visited_segments, start_time).await?;
            return Ok(true);
        }

        // Store pending block
        // todo, prefer using the block import pipeline?
        self.backend.store_block(
            self.block.clone().into(),
            new_state_diff,
            self.declared_classes.clone(),
            Some(visited_segments),
            Some(bouncer_weights),
        )?;
        // do not forget to flush :)
        self.backend.flush().map_err(|err| BlockImportError::Internal(format!("DB flushing error: {err:#}").into()))?;

        // TODO: Measure the transactions time --------------------------------------------------
        // TODO: Do all of it inside a function
        // =========================================================================================
        // Execute BOT transactions :

        let game_width = env::var("MADARA_GAME_WIDTH").expect("MADARA_GAME_WIDTH not set").parse::<u64>().unwrap();
        let game_height = env::var("MADARA_GAME_HEIGHT").expect("MADARA_GAME_HEIGHT not set").parse::<u64>().unwrap();

        println!(">>> Game width : {:?} and height : {:?}", game_width, game_height);

        let area = game_width * game_height;

        let game_metadata = self.backend.game_get_metadata().expect("Unable to fetch last start index");
        let bot_addresses = self.backend.game_get_bots_list().expect("Could not get bots' list");
        if game_metadata.tiles_mined < area && bot_addresses.len() > 0 {
            println!(">>> Triggering bot transactions");
            println!(">>> Current Tiles mined : {:?} vs total to be mined : {:?}", game_metadata.tiles_mined, area);
            let addresses_clone = bot_addresses.clone();

            // let c = bot_addresses
            //     .iter()
            //     .map(|x| Felt::from_str(x).expect("could not convert string to felt"))
            //     .collect::<Vec<_>>();

            // println!(">>> DB bots list : {:?}", c);
            // Do nothing if 0 bots to execute
            if bot_addresses.is_empty() {
                return Ok(false);
            }
            // TODO: check if game is active or not
            // TODO: what is bot is disabled ?

            let txns = self.generate_txns(addresses_clone);
            println!(">>> Number of txns generated : {:?}", txns.len());
            txns.iter().for_each(|txn| {
                self.mempool.accept_invoke_tx(txn.clone()).expect("Unable to accept invoke tx");
            });
            // self.mempool.accept_invoke_tx(txn).expect("Unable to accept invoke tx");
            println!(">>> Time taken to run on_pending_tick: {:?}", start.elapsed().as_millis());

            // =========================================================================================
        }
        Ok(false)
    }

    /// This creates a block, continuing the current pending block state up to the full bouncer limit.
    #[tracing::instrument(skip(self), fields(module = "BlockProductionTask"))]
    pub(crate) async fn on_block_time(&mut self) -> Result<(), Error> {
        let block_n = self.block_n();
        tracing::debug!("closing block #{}", block_n);

        // Complete the block with full bouncer capacity
        let start_time = Instant::now();
        let ContinueBlockResult {
            state_diff: mut new_state_diff,
            visited_segments,
            bouncer_weights: _weights,
            stats: _stats,
            block_now_full: _block_now_full,
        } = self.continue_block(self.backend.chain_config().bouncer_config.block_max_capacity)?;

        self.update_block_hash_registry(&mut new_state_diff, block_n)?;

        self.close_and_prepare_next_block(new_state_diff, visited_segments, start_time).await
    }

    #[tracing::instrument(skip(self, ctx), fields(module = "BlockProductionTask"))]
    pub async fn block_production_task(mut self, mut ctx: ServiceContext) -> Result<(), anyhow::Error> {
        let start = tokio::time::Instant::now();

        let mut interval_block_time = tokio::time::interval_at(start, self.backend.chain_config().block_time);
        interval_block_time.reset(); // do not fire the first tick immediately
        interval_block_time.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        let mut interval_pending_block_update =
            tokio::time::interval_at(start, self.backend.chain_config().pending_block_update_time);
        interval_pending_block_update.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

        self.backend.chain_config().precheck_block_production()?; // check chain config for invalid config

        tracing::info!("⛏️  Starting block production at block #{}", self.block_n());

        loop {
            tokio::select! {
                instant = interval_block_time.tick() => {
                    if let Err(err) = self.on_block_time().await {
                        tracing::error!("Block production task has errored: {err:#}");
                        // Clear pending block. The reason we do this is because
                        // if the error happened because the closed block is
                        // invalid or has not been saved properly, we want to
                        // avoid redoing the same error in the next block. So we
                        // drop all the transactions in the pending block just
                        // in case. If the problem happened after the block was
                        // closed and saved to the db, this will do nothing.
                        if let Err(err) = self.backend.clear_pending_block() {
                            tracing::error!("Error while clearing the pending block in recovery of block production error: {err:#}");
                        }
                    }
                    // ensure the pending block tick and block time match up
                    interval_pending_block_update.reset_at(instant + interval_pending_block_update.period());
                },
                instant = interval_pending_block_update.tick() => {
                    let n_pending_ticks_per_block = self.backend.chain_config().n_pending_ticks_per_block();

                    if self.current_pending_tick == 0 || self.current_pending_tick >= n_pending_ticks_per_block {
                        // First tick is ignored. Out of range ticks are also
                        // ignored.
                        self.current_pending_tick += 1;
                        continue
                    }

                    match self.on_pending_time_tick().await {
                        Ok(block_closed) => {
                            if block_closed {
                                interval_pending_block_update.reset_at(instant + interval_pending_block_update.period());
                                interval_block_time.reset_at(instant + interval_block_time.period());
                                self.current_pending_tick = 0;
                            } else {
                                self.current_pending_tick += 1;
                            }
                        }
                        Err(err) => {
                            tracing::error!("Pending block update task has errored: {err:#}");
                        }
                    }
                },
                _ = ctx.cancelled() => break,
            }
        }

        Ok(())
    }

    fn generate_txns(&self, contract_addresses: Vec<String>) -> Vec<BroadcastedInvokeTxn<Felt>> {
        let x = env::var("MADARA_GAME_SEQUENCER_ADDRESS").unwrap();
        let y = env::var("MADARA_GAME_SEQUENCER_PRIVATE_KEY").unwrap();
        let z = env::var("MADARA_GAME_CONTRACT_ADDRESS").unwrap();

        println!(">>> SEQUENCER ADDRESS {:?}", x);
        println!(">>> SEQUENCER PRIVATE KEY {:?}", y);
        println!(">>> GAME ADDRESS {:?}", z);

        let sequencer_address = Felt::from_hex(&x.as_str()).expect("Unable to extract public key from hex");
        let sequencer_priv_key = Felt::from_hex(&y.as_str()).expect("Unable to extract priv key from hex");
        let game_address = Felt::from_hex(&z.as_str()).expect("Unable to extract public key from hex");

        let signing_key = SigningKey::from_secret_scalar(sequencer_priv_key);

        let nonce = self
            .backend
            .get_contract_nonce_at(&DbBlockId::Pending, &sequencer_address)
            .expect("Unable to fetch nonce from the block.")
            // if nonce is not found, use 0
            .unwrap_or(Felt::from(0));

        println!(">>> TXN NONCE {:?}", nonce);

        let mut call_vec = Vec::new();
        for address in contract_addresses {
            let random_seed: u64 = thread_rng().gen();
            call_vec.push(Call {
                to: game_address,
                selector: Selector::from("mine"),
                calldata: vec![Felt::from_str(&address).unwrap(), Felt::from(random_seed)],
            })
        }

        let txns = self.create_txns(sequencer_address, nonce, call_vec, signing_key);
        txns
    }

    fn create_txns(
        &self,
        sequencer_address: Felt,
        starting_nonce: Felt,
        call_vec: Vec<Call>,
        signing_key: SigningKey,
    ) -> Vec<BroadcastedInvokeTxn<Felt>> {
        let mut internal_nonce = starting_nonce.clone().to_bigint();
        let mut txns: Vec<BroadcastedInvokeTxn<Felt>> = Vec::new();

        let chunk_size = env::var("MADARA_GAME_MULTICALL_CHUNK_SIZE")
            .expect("MADARA_GAME_MULTICALL_CHUNK_SIZE not set")
            .parse::<usize>()
            .unwrap();
        // Convert the original Vec into an iterator of chunks and collect each chunk into a Vec
        let chunks: Vec<Vec<Call>> =
            call_vec.into_iter().collect::<Vec<_>>().chunks(chunk_size).map(|chunk| chunk.to_vec()).collect();

        for chunk in chunks {
            let txn_internal = BroadcastedTxn::Invoke(BroadcastedInvokeTxn::V1(InvokeTxnV1 {
                sender_address: sequencer_address,
                calldata: Multicall::with_vec(chunk).flatten().collect(),
                max_fee: Felt::from_str("100000000").unwrap(),
                signature: vec![],
                nonce: Felt::from(internal_nonce.clone()),
            }));

            let signed_transaction =
                self.sign_tx(txn_internal, signing_key.clone()).expect("Not able to sign the transaction.");

            let final_txn = match signed_transaction {
                BroadcastedTxn::Invoke(tx) => tx,
                _ => panic!("Invalid Txn"),
            };
            internal_nonce += 1;
            txns.push(final_txn);
        }
        txns
    }

    fn sign_tx(&self, mut tx: BroadcastedTxn<Felt>, signing_key: SigningKey) -> anyhow::Result<BroadcastedTxn<Felt>> {
        let (blockifier_tx, _) = BroadcastedTxn::into_blockifier(
            tx.clone(),
            self.backend.chain_config().chain_id.to_felt(),
            self.backend.chain_config().latest_protocol_version,
        )?;
        let signature = signing_key.sign(&transaction_hash(&blockifier_tx))?;
        let tx_signature = match &mut tx {
            BroadcastedTxn::Invoke(tx) => match tx {
                BroadcastedInvokeTxn::V1(tx) => &mut tx.signature,
                _ => panic!("Invalid Txn"),
            },
            _ => panic!("Invalid Txn"),
        };
        *tx_signature = vec![signature.r, signature.s];
        Ok(tx)
    }

    fn block_n(&self) -> u64 {
        self.executor.block_context.block_info().block_number.0
    }
}

#[cfg(test)]
mod tests {
    use std::str::FromStr;

    use super::*;
    use starknet_types_core::felt::Felt;

    // Helper function to create test bots
    fn create_test_bots(count: i64) -> Vec<Felt> {
        // format : 0x<number>
        (0..count).map(|i| Felt::from_str(&format!("0x{i}")).unwrap()).collect()
    }

    // Helper function to verify distribution properties
    fn verify_distribution(result: &Vec<Felt>, expected_len: i64, original_bots: &Vec<Felt>) {
        assert_eq!(result.len(), expected_len as usize, "Result length should match MINES_PER_TRANSACTION");

        for bot in result {
            assert!(original_bots.contains(bot), "Every bot in result should exist in original list");
        }
    }

    // These tests can be more stressing
    // #[test]
    // fn test_less_bots_than_mines() {
    //     let num_bots: i64 = 137;
    //     let bots = create_test_bots(num_bots); // 100 bots < 500 MINES_PER_TRANSACTION
    //     let start_index = 124;

    //     // Array : 51 to 100, then 1 to 100 4 times, then 1 to 50

    //     let (result, new_start_index) = get_bots_list();

    //     verify_distribution(&result, MINES_PER_TRANSACTION, &bots);

    //     // Each bot should appear multiple times
    //     let repetitions = MINES_PER_TRANSACTION / num_bots;
    //     let mut count_map = std::collections::HashMap::new();
    //     for bot in &result {
    //         *count_map.entry(bot).or_insert(0) += 1;
    //     }

    //     println!("test_less_bots_than_mines : {:?}", result);
    //     println!("test_less_bots_than_mines last index : {:?}", new_start_index);

    //     // Check if each bot appears at least the minimum number of times
    //     for count in count_map.values() {
    //         assert!(*count >= repetitions, "Each bot should appear at least {} times", repetitions);
    //     }
    // }

    // #[test]
    // fn test_equal_bots_to_mines() {
    //     let bots = create_test_bots(MINES_PER_TRANSACTION);
    //     let start_index = 36;

    //     let (result, new_start_index) = get_bots_list(bots.clone(), start_index);

    //     verify_distribution(&result, MINES_PER_TRANSACTION, &bots);

    //     // Each bot should appear exactly once
    //     let mut count_map = std::collections::HashMap::new();
    //     for bot in &result {
    //         *count_map.entry(bot).or_insert(0) += 1;
    //     }

    //     println!("test_equal_bots_to_mines : {:?}", result);
    //     println!("test_equal_bots_to_mines last index : {:?}", new_start_index);

    //     for count in count_map.values() {
    //         assert_eq!(*count, 1, "Each bot should appear exactly once");
    //     }
    // }

    // #[test]
    // fn test_more_bots_than_mines() {
    //     let bots = create_test_bots(3110); // 1000 bots > 500 MINES_PER_TRANSACTION
    //     let start_index = 457;

    //     let (result, new_start_index) = get_bots_list(bots.clone(), start_index);

    //     verify_distribution(&result, MINES_PER_TRANSACTION, &bots);

    //     // Each bot should appear at most once
    //     let mut count_map = std::collections::HashMap::new();
    //     for bot in &result {
    //         *count_map.entry(bot).or_insert(0) += 1;
    //     }

    //     println!("test_more_bots_than_mines : {:?}", result);
    //     println!("test_more_bots_than_mines last index : {:?}", new_start_index);

    //     for count in count_map.values() {
    //         assert_eq!(*count, 1, "Each bot should appear exactly once");
    //     }

    //     // Verify that we start from last_index
    //     assert_eq!(result[0], bots[start_index as usize], "First bot should match the last_index position");
    // }

    // #[test]
    // fn test_empty_bots_list() {
    //     let bots = Vec::new();
    //     let last_index = 0;

    //     let (result, new_index) = get_bots_list(bots, last_index);

    //     assert_eq!(result.len(), 0, "Result should be empty for empty input");
    //     assert_eq!(new_index, 0, "New index should be 0 for empty input");
    // }

    // #[test]
    // fn test_index_wrapping() {
    //     let bots = create_test_bots(200);
    //     let last_index = 150; // Near the end of the list

    //     let (result, new_index) = get_bots_list(bots.clone(), last_index);

    //     verify_distribution(&result, MINES_PER_TRANSACTION, &bots);

    //     // Verify that the list wraps around correctly
    //     assert!(new_index < bots.len() as i64, "New index should wrap around");
    // }

    use std::{collections::HashMap, sync::Arc};

    use blockifier::{
        bouncer::BouncerWeights, compiled_class_hash, nonce, state::cached_state::StateMaps, storage_key,
    };
    use mc_db::MadaraBackend;
    use mc_mempool::Mempool;
    use mp_block::VisitedSegments;
    use mp_chain_config::ChainConfig;
    use mp_convert::ToFelt;
    use mp_state_update::{
        ContractStorageDiffItem, DeclaredClassItem, DeployedContractItem, NonceUpdate, ReplacedClassItem, StateDiff,
        StorageEntry,
    };
    use starknet_api::{
        class_hash, contract_address,
        core::{ClassHash, ContractAddress, PatriciaKey},
        felt, patricia_key,
    };

    use crate::{
        finalize_execution_state::state_map_to_state_diff, metrics::BlockProductionMetrics, BlockProductionTask,
    };

    type TxFixtureInfo = (mp_transactions::Transaction, mp_receipt::TransactionReceipt);

    #[rstest::fixture]
    fn backend() -> Arc<MadaraBackend> {
        MadaraBackend::open_for_testing(Arc::new(ChainConfig::madara_test()))
    }

    #[rstest::fixture]
    fn setup(
        backend: Arc<MadaraBackend>,
    ) -> (Arc<MadaraBackend>, Arc<mc_block_import::BlockImporter>, Arc<BlockProductionMetrics>) {
        (
            Arc::clone(&backend),
            Arc::new(mc_block_import::BlockImporter::new(Arc::clone(&backend), None).unwrap()),
            Arc::new(BlockProductionMetrics::register()),
        )
    }

    #[rstest::fixture]
    fn tx_invoke_v0(#[default(Felt::ZERO)] contract_address: Felt) -> TxFixtureInfo {
        (
            mp_transactions::Transaction::Invoke(mp_transactions::InvokeTransaction::V0(
                mp_transactions::InvokeTransactionV0 { contract_address, ..Default::default() },
            )),
            mp_receipt::TransactionReceipt::Invoke(mp_receipt::InvokeTransactionReceipt::default()),
        )
    }

    #[rstest::fixture]
    fn tx_l1_handler(#[default(Felt::ZERO)] contract_address: Felt) -> TxFixtureInfo {
        (
            mp_transactions::Transaction::L1Handler(mp_transactions::L1HandlerTransaction {
                contract_address,
                ..Default::default()
            }),
            mp_receipt::TransactionReceipt::L1Handler(mp_receipt::L1HandlerTransactionReceipt::default()),
        )
    }

    #[rstest::fixture]
    fn tx_declare_v0(#[default(Felt::ZERO)] sender_address: Felt) -> TxFixtureInfo {
        (
            mp_transactions::Transaction::Declare(mp_transactions::DeclareTransaction::V0(
                mp_transactions::DeclareTransactionV0 { sender_address, ..Default::default() },
            )),
            mp_receipt::TransactionReceipt::Declare(mp_receipt::DeclareTransactionReceipt::default()),
        )
    }

    #[rstest::fixture]
    fn tx_deploy() -> TxFixtureInfo {
        (
            mp_transactions::Transaction::Deploy(mp_transactions::DeployTransaction::default()),
            mp_receipt::TransactionReceipt::Deploy(mp_receipt::DeployTransactionReceipt::default()),
        )
    }

    #[rstest::fixture]
    fn tx_deploy_account() -> TxFixtureInfo {
        (
            mp_transactions::Transaction::DeployAccount(mp_transactions::DeployAccountTransaction::V1(
                mp_transactions::DeployAccountTransactionV1::default(),
            )),
            mp_receipt::TransactionReceipt::DeployAccount(mp_receipt::DeployAccountTransactionReceipt::default()),
        )
    }

    #[rstest::fixture]
    fn converted_class_legacy(#[default(Felt::ZERO)] class_hash: Felt) -> mp_class::ConvertedClass {
        mp_class::ConvertedClass::Legacy(mp_class::LegacyConvertedClass {
            class_hash,
            info: mp_class::LegacyClassInfo {
                contract_class: Arc::new(mp_class::CompressedLegacyContractClass {
                    program: vec![],
                    entry_points_by_type: mp_class::LegacyEntryPointsByType {
                        constructor: vec![],
                        external: vec![],
                        l1_handler: vec![],
                    },
                    abi: None,
                }),
            },
        })
    }

    #[rstest::fixture]
    fn converted_class_sierra(
        #[default(Felt::ZERO)] class_hash: Felt,
        #[default(Felt::ZERO)] compiled_class_hash: Felt,
    ) -> mp_class::ConvertedClass {
        mp_class::ConvertedClass::Sierra(mp_class::SierraConvertedClass {
            class_hash,
            info: mp_class::SierraClassInfo {
                contract_class: Arc::new(mp_class::FlattenedSierraClass {
                    sierra_program: vec![],
                    contract_class_version: "".to_string(),
                    entry_points_by_type: mp_class::EntryPointsByType {
                        constructor: vec![],
                        external: vec![],
                        l1_handler: vec![],
                    },
                    abi: "".to_string(),
                }),
                compiled_class_hash,
            },
            compiled: Arc::new(mp_class::CompiledSierra("".to_string())),
        })
    }

    #[rstest::fixture]
    fn visited_segments() -> mp_block::VisitedSegments {
        mp_block::VisitedSegments(vec![
            mp_block::VisitedSegmentEntry { class_hash: Felt::ONE, segments: vec![0, 1, 2] },
            mp_block::VisitedSegmentEntry { class_hash: Felt::TWO, segments: vec![0, 1, 2] },
            mp_block::VisitedSegmentEntry { class_hash: Felt::THREE, segments: vec![0, 1, 2] },
        ])
    }

    #[rstest::fixture]
    fn bouncer_weights() -> BouncerWeights {
        BouncerWeights {
            builtin_count: blockifier::bouncer::BuiltinCount {
                add_mod: 0,
                bitwise: 1,
                ecdsa: 2,
                ec_op: 3,
                keccak: 4,
                mul_mod: 5,
                pedersen: 6,
                poseidon: 7,
                range_check: 8,
                range_check96: 9,
            },
            gas: 10,
            message_segment_length: 11,
            n_events: 12,
            n_steps: 13,
            state_diff_size: 14,
        }
    }

    #[rstest::rstest]
    fn block_prod_state_map_to_state_diff(backend: Arc<MadaraBackend>) {
        let mut nonces = HashMap::new();
        nonces.insert(contract_address!(1u32), nonce!(1));
        nonces.insert(contract_address!(2u32), nonce!(2));
        nonces.insert(contract_address!(3u32), nonce!(3));

        let mut class_hashes = HashMap::new();
        class_hashes.insert(contract_address!(1u32), class_hash!("0xc1a551"));
        class_hashes.insert(contract_address!(2u32), class_hash!("0xc1a552"));
        class_hashes.insert(contract_address!(3u32), class_hash!("0xc1a553"));

        let mut storage = HashMap::new();
        storage.insert((contract_address!(1u32), storage_key!(1u32)), felt!(1u32));
        storage.insert((contract_address!(1u32), storage_key!(2u32)), felt!(2u32));
        storage.insert((contract_address!(1u32), storage_key!(3u32)), felt!(3u32));

        storage.insert((contract_address!(2u32), storage_key!(1u32)), felt!(1u32));
        storage.insert((contract_address!(2u32), storage_key!(2u32)), felt!(2u32));
        storage.insert((contract_address!(2u32), storage_key!(3u32)), felt!(3u32));

        storage.insert((contract_address!(3u32), storage_key!(1u32)), felt!(1u32));
        storage.insert((contract_address!(3u32), storage_key!(2u32)), felt!(2u32));
        storage.insert((contract_address!(3u32), storage_key!(3u32)), felt!(3u32));

        let mut compiled_class_hashes = HashMap::new();
        // "0xc1a553" is marked as deprecated by not having a compiled
        // class hashe
        compiled_class_hashes.insert(class_hash!("0xc1a551"), compiled_class_hash!(0x1));
        compiled_class_hashes.insert(class_hash!("0xc1a552"), compiled_class_hash!(0x2));

        let mut declared_contracts = HashMap::new();
        declared_contracts.insert(class_hash!("0xc1a551"), true);
        declared_contracts.insert(class_hash!("0xc1a552"), true);
        declared_contracts.insert(class_hash!("0xc1a553"), true);

        let state_map = StateMaps { nonces, class_hashes, storage, compiled_class_hashes, declared_contracts };

        let storage_diffs = vec![
            ContractStorageDiffItem {
                address: felt!(1u32),
                storage_entries: vec![
                    StorageEntry { key: felt!(1u32), value: Felt::ONE },
                    StorageEntry { key: felt!(2u32), value: Felt::TWO },
                    StorageEntry { key: felt!(3u32), value: Felt::THREE },
                ],
            },
            ContractStorageDiffItem {
                address: felt!(2u32),
                storage_entries: vec![
                    StorageEntry { key: felt!(1u32), value: Felt::ONE },
                    StorageEntry { key: felt!(2u32), value: Felt::TWO },
                    StorageEntry { key: felt!(3u32), value: Felt::THREE },
                ],
            },
            ContractStorageDiffItem {
                address: felt!(3u32),
                storage_entries: vec![
                    StorageEntry { key: felt!(1u32), value: Felt::ONE },
                    StorageEntry { key: felt!(2u32), value: Felt::TWO },
                    StorageEntry { key: felt!(3u32), value: Felt::THREE },
                ],
            },
        ];

        let deprecated_declared_classes = vec![class_hash!("0xc1a553").to_felt()];

        let declared_classes = vec![
            DeclaredClassItem {
                class_hash: class_hash!("0xc1a551").to_felt(),
                compiled_class_hash: compiled_class_hash!(0x1).to_felt(),
            },
            DeclaredClassItem {
                class_hash: class_hash!("0xc1a552").to_felt(),
                compiled_class_hash: compiled_class_hash!(0x2).to_felt(),
            },
        ];

        let nonces = vec![
            NonceUpdate { contract_address: felt!(1u32), nonce: felt!(1u32) },
            NonceUpdate { contract_address: felt!(2u32), nonce: felt!(2u32) },
            NonceUpdate { contract_address: felt!(3u32), nonce: felt!(3u32) },
        ];

        let deployed_contracts = vec![
            DeployedContractItem { address: felt!(1u32), class_hash: class_hash!("0xc1a551").to_felt() },
            DeployedContractItem { address: felt!(2u32), class_hash: class_hash!("0xc1a552").to_felt() },
            DeployedContractItem { address: felt!(3u32), class_hash: class_hash!("0xc1a553").to_felt() },
        ];

        let replaced_classes = vec![];

        let expected = StateDiff {
            storage_diffs,
            deprecated_declared_classes,
            declared_classes,
            nonces,
            deployed_contracts,
            replaced_classes,
        };

        let mut actual = state_map_to_state_diff(&backend, &Option::<_>::None, state_map).unwrap();

        actual.storage_diffs.sort_by(|a, b| a.address.cmp(&b.address));
        actual.storage_diffs.iter_mut().for_each(|s| s.storage_entries.sort_by(|a, b| a.key.cmp(&b.key)));
        actual.deprecated_declared_classes.sort();
        actual.declared_classes.sort_by(|a, b| a.class_hash.cmp(&b.class_hash));
        actual.nonces.sort_by(|a, b| a.contract_address.cmp(&b.contract_address));
        actual.deployed_contracts.sort_by(|a, b| a.address.cmp(&b.address));
        actual.replaced_classes.sort_by(|a, b| a.contract_address.cmp(&b.contract_address));

        assert_eq!(
            actual,
            expected,
            "actual: {}\nexpected: {}",
            serde_json::to_string_pretty(&actual).unwrap_or_default(),
            serde_json::to_string_pretty(&expected).unwrap_or_default()
        );
    }

    /// This test makes sure that if a pending block is already present in db
    /// at startup, then it is closed and stored in db.
    ///
    /// This happens if a full node is shutdown (gracefully or not) midway
    /// during block production.
    #[rstest::rstest]
    #[tokio::test]
    #[allow(clippy::too_many_arguments)]
    async fn block_prod_pending_close_on_startup_pass(
        setup: (Arc<MadaraBackend>, Arc<mc_block_import::BlockImporter>, Arc<BlockProductionMetrics>),
        #[with(Felt::ONE)] tx_invoke_v0: TxFixtureInfo,
        #[with(Felt::TWO)] tx_l1_handler: TxFixtureInfo,
        #[with(Felt::THREE)] tx_declare_v0: TxFixtureInfo,
        tx_deploy: TxFixtureInfo,
        tx_deploy_account: TxFixtureInfo,
        #[from(converted_class_legacy)]
        #[with(Felt::ZERO)]
        converted_class_legacy_0: mp_class::ConvertedClass,
        #[from(converted_class_sierra)]
        #[with(Felt::ONE, Felt::ONE)]
        converted_class_sierra_1: mp_class::ConvertedClass,
        #[from(converted_class_sierra)]
        #[with(Felt::TWO, Felt::TWO)]
        converted_class_sierra_2: mp_class::ConvertedClass,
        visited_segments: VisitedSegments,
        bouncer_weights: BouncerWeights,
    ) {
        let (backend, importer, metrics) = setup;

        // ================================================================== //
        //                  PART 1: we prepare the pending block              //
        // ================================================================== //

        let pending_inner = mp_block::MadaraBlockInner {
            transactions: vec![tx_invoke_v0.0, tx_l1_handler.0, tx_declare_v0.0, tx_deploy.0, tx_deploy_account.0],
            receipts: vec![tx_invoke_v0.1, tx_l1_handler.1, tx_declare_v0.1, tx_deploy.1, tx_deploy_account.1],
        };

        let pending_state_diff = mp_state_update::StateDiff {
            storage_diffs: vec![
                ContractStorageDiffItem {
                    address: Felt::ONE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::TWO,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::THREE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
            ],
            deprecated_declared_classes: vec![Felt::ZERO],
            declared_classes: vec![
                DeclaredClassItem { class_hash: Felt::ONE, compiled_class_hash: Felt::ONE },
                DeclaredClassItem { class_hash: Felt::TWO, compiled_class_hash: Felt::TWO },
            ],
            deployed_contracts: vec![DeployedContractItem { address: Felt::THREE, class_hash: Felt::THREE }],
            replaced_classes: vec![ReplacedClassItem { contract_address: Felt::TWO, class_hash: Felt::TWO }],
            nonces: vec![
                NonceUpdate { contract_address: Felt::ONE, nonce: Felt::ONE },
                NonceUpdate { contract_address: Felt::TWO, nonce: Felt::TWO },
                NonceUpdate { contract_address: Felt::THREE, nonce: Felt::THREE },
            ],
        };

        let converted_classes =
            vec![converted_class_legacy_0.clone(), converted_class_sierra_1.clone(), converted_class_sierra_2.clone()];

        // ================================================================== //
        //                   PART 2: storing the pending block                //
        // ================================================================== //

        // This simulates a node restart after shutting down midway during block
        // production.
        //
        // Block production functions by storing un-finalized blocks as pending.
        // This is the only form of data we can recover without re-execution as
        // everything else is stored in RAM (mempool transactions which have not
        // been polled yet are also stored in db for retrieval, but these
        // haven't been executed anyways). This means that if ever the node
        // crashes, we will only be able to retrieve whatever data was stored in
        // the pending block. This is done atomically so we never commit partial
        // data to the database and only a full pending block can ever be
        // stored.
        //
        // We are therefore simulating stopping and restarting the node, since:
        //
        // - This is the only pending data that can persist a node restart, and
        //   it cannot be partially valid (we still test failing cases though).
        //
        // - Upon restart, this is what the block production would be looking to
        //   seal.

        backend
            .store_block(
                mp_block::MadaraMaybePendingBlock {
                    info: mp_block::MadaraMaybePendingBlockInfo::Pending(mp_block::MadaraPendingBlockInfo {
                        header: mp_block::header::PendingHeader::default(),
                        tx_hashes: vec![Felt::ONE, Felt::TWO, Felt::THREE],
                    }),
                    inner: pending_inner.clone(),
                },
                pending_state_diff.clone(),
                converted_classes.clone(),
                Some(visited_segments.clone()),
                Some(bouncer_weights),
            )
            .expect("Failed to store pending block");

        // ================================================================== //
        //        PART 3: init block production and seal pending block        //
        // ================================================================== //

        // This should load the pending block from db and close it
        BlockProductionTask::<Mempool>::close_pending_block(&backend, &importer, &metrics)
            .await
            .expect("Failed to close pending block");

        // Now we check this was the case.
        assert_eq!(backend.get_latest_block_n().unwrap().unwrap(), 0);

        let block_inner = backend
            .get_block(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest))
            .expect("Failed to retrieve latest block from db")
            .expect("Missing latest block")
            .inner;
        assert_eq!(block_inner, pending_inner);

        let state_diff = backend
            .get_block_state_diff(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest))
            .expect("Failed to retrieve latest state diff from db")
            .expect("Missing latest state diff");
        assert_eq!(state_diff, pending_state_diff);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::ZERO)
            .expect("Failed to retrieve class at hash 0x0 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_legacy_0);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::ONE)
            .expect("Failed to retrieve class at hash 0x1 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_sierra_1);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::TWO)
            .expect("Failed to retrieve class at hash 0x2 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_sierra_2);

        // visited segments and bouncer weights are currently not stored for in
        // ready blocks
    }

    /// This test makes sure that if a pending block is already present in db
    /// at startup, then it is closed and stored in db on top of the latest
    /// block.
    #[rstest::rstest]
    #[tokio::test]
    #[allow(clippy::too_many_arguments)]
    async fn block_prod_pending_close_on_startup_pass_on_top(
        setup: (Arc<MadaraBackend>, Arc<mc_block_import::BlockImporter>, Arc<BlockProductionMetrics>),

        // Transactions
        #[from(tx_invoke_v0)]
        #[with(Felt::ZERO)]
        tx_invoke_v0_0: TxFixtureInfo,
        #[from(tx_invoke_v0)]
        #[with(Felt::ONE)]
        tx_invoke_v0_1: TxFixtureInfo,
        #[from(tx_l1_handler)]
        #[with(Felt::ONE)]
        tx_l1_handler_1: TxFixtureInfo,
        #[from(tx_l1_handler)]
        #[with(Felt::TWO)]
        tx_l1_handler_2: TxFixtureInfo,
        #[from(tx_declare_v0)]
        #[with(Felt::TWO)]
        tx_declare_v0_2: TxFixtureInfo,
        #[from(tx_declare_v0)]
        #[with(Felt::THREE)]
        tx_declare_v0_3: TxFixtureInfo,
        tx_deploy: TxFixtureInfo,
        tx_deploy_account: TxFixtureInfo,

        // Converted classes
        #[from(converted_class_legacy)]
        #[with(Felt::ZERO)]
        converted_class_legacy_0: mp_class::ConvertedClass,
        #[from(converted_class_sierra)]
        #[with(Felt::ONE, Felt::ONE)]
        converted_class_sierra_1: mp_class::ConvertedClass,
        #[from(converted_class_sierra)]
        #[with(Felt::TWO, Felt::TWO)]
        converted_class_sierra_2: mp_class::ConvertedClass,

        // Pending data
        visited_segments: VisitedSegments,
        bouncer_weights: BouncerWeights,
    ) {
        let (backend, importer, metrics) = setup;

        // ================================================================== //
        //                   PART 1: we prepare the ready block               //
        // ================================================================== //

        let ready_inner = mp_block::MadaraBlockInner {
            transactions: vec![tx_invoke_v0_0.0, tx_l1_handler_1.0, tx_declare_v0_2.0],
            receipts: vec![tx_invoke_v0_0.1, tx_l1_handler_1.1, tx_declare_v0_2.1],
        };

        let ready_state_diff = mp_state_update::StateDiff {
            storage_diffs: vec![
                ContractStorageDiffItem {
                    address: Felt::ONE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::TWO,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::THREE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
            ],
            deprecated_declared_classes: vec![],
            declared_classes: vec![],
            deployed_contracts: vec![DeployedContractItem { address: Felt::THREE, class_hash: Felt::THREE }],
            replaced_classes: vec![ReplacedClassItem { contract_address: Felt::TWO, class_hash: Felt::TWO }],
            nonces: vec![
                NonceUpdate { contract_address: Felt::ONE, nonce: Felt::ONE },
                NonceUpdate { contract_address: Felt::TWO, nonce: Felt::TWO },
                NonceUpdate { contract_address: Felt::THREE, nonce: Felt::THREE },
            ],
        };

        let ready_converted_classes = vec![];

        // ================================================================== //
        //                   PART 2: storing the ready block                  //
        // ================================================================== //

        // Simulates block closure before the shutdown
        backend
            .store_block(
                mp_block::MadaraMaybePendingBlock {
                    info: mp_block::MadaraMaybePendingBlockInfo::NotPending(mp_block::MadaraBlockInfo {
                        header: mp_block::Header::default(),
                        block_hash: Felt::ZERO,
                        tx_hashes: vec![Felt::ZERO, Felt::ONE, Felt::TWO],
                    }),
                    inner: ready_inner.clone(),
                },
                ready_state_diff.clone(),
                ready_converted_classes.clone(),
                Some(visited_segments.clone()),
                Some(bouncer_weights),
            )
            .expect("Failed to store pending block");

        // ================================================================== //
        //                  PART 3: we prepare the pending block              //
        // ================================================================== //

        let pending_inner = mp_block::MadaraBlockInner {
            transactions: vec![
                tx_invoke_v0_1.0,
                tx_l1_handler_2.0,
                tx_declare_v0_3.0,
                tx_deploy.0,
                tx_deploy_account.0,
            ],
            receipts: vec![tx_invoke_v0_1.1, tx_l1_handler_2.1, tx_declare_v0_3.1, tx_deploy.1, tx_deploy_account.1],
        };

        let pending_state_diff = mp_state_update::StateDiff {
            storage_diffs: vec![
                ContractStorageDiffItem {
                    address: Felt::ONE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::TWO,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::THREE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
            ],
            deprecated_declared_classes: vec![Felt::ZERO],
            declared_classes: vec![
                DeclaredClassItem { class_hash: Felt::ONE, compiled_class_hash: Felt::ONE },
                DeclaredClassItem { class_hash: Felt::TWO, compiled_class_hash: Felt::TWO },
            ],
            deployed_contracts: vec![DeployedContractItem { address: Felt::THREE, class_hash: Felt::THREE }],
            replaced_classes: vec![ReplacedClassItem { contract_address: Felt::TWO, class_hash: Felt::TWO }],
            nonces: vec![
                NonceUpdate { contract_address: Felt::ONE, nonce: Felt::ONE },
                NonceUpdate { contract_address: Felt::TWO, nonce: Felt::TWO },
                NonceUpdate { contract_address: Felt::THREE, nonce: Felt::THREE },
            ],
        };

        let pending_converted_classes =
            vec![converted_class_legacy_0.clone(), converted_class_sierra_1.clone(), converted_class_sierra_2.clone()];

        // ================================================================== //
        //                   PART 4: storing the pending block                //
        // ================================================================== //

        // This simulates a node restart after shutting down midway during block
        // production.
        backend
            .store_block(
                mp_block::MadaraMaybePendingBlock {
                    info: mp_block::MadaraMaybePendingBlockInfo::Pending(mp_block::MadaraPendingBlockInfo {
                        header: mp_block::header::PendingHeader::default(),
                        tx_hashes: vec![Felt::ONE, Felt::TWO, Felt::THREE],
                    }),
                    inner: pending_inner.clone(),
                },
                pending_state_diff.clone(),
                pending_converted_classes.clone(),
                Some(visited_segments.clone()),
                Some(bouncer_weights),
            )
            .expect("Failed to store pending block");

        // ================================================================== //
        //        PART 5: init block production and seal pending block        //
        // ================================================================== //

        // This should load the pending block from db and close it on top of the
        // previous block.
        BlockProductionTask::<Mempool>::close_pending_block(&backend, &importer, &metrics)
            .await
            .expect("Failed to close pending block");

        // Now we check this was the case.
        assert_eq!(backend.get_latest_block_n().unwrap().unwrap(), 1);

        // Block 0 should not have been overridden!
        let block = backend
            .get_block(&mp_block::BlockId::Number(0))
            .expect("Failed to retrieve block 0 from db")
            .expect("Missing block 0");

        assert_eq!(block.info.as_nonpending().unwrap().header.parent_block_hash, Felt::ZERO);
        assert_eq!(block.inner, ready_inner);

        let block = backend
            .get_block(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest))
            .expect("Failed to retrieve latest block from db")
            .expect("Missing latest block");

        assert_eq!(block.info.as_nonpending().unwrap().header.parent_block_hash, Felt::ZERO);
        assert_eq!(block.inner, pending_inner);

        // Block 0 should not have been overridden!
        let state_diff = backend
            .get_block_state_diff(&mp_block::BlockId::Number(0))
            .expect("Failed to retrieve state diff at block 0 from db")
            .expect("Missing state diff at block 0");
        assert_eq!(ready_state_diff, state_diff);

        let state_diff = backend
            .get_block_state_diff(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest))
            .expect("Failed to retrieve latest state diff from db")
            .expect("Missing latest state diff");
        assert_eq!(pending_state_diff, state_diff);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::ZERO)
            .expect("Failed to retrieve class at hash 0x0 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_legacy_0);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::ONE)
            .expect("Failed to retrieve class at hash 0x1 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_sierra_1);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::TWO)
            .expect("Failed to retrieve class at hash 0x2 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_sierra_2);

        // visited segments and bouncer weights are currently not stored for in
        // ready blocks
    }

    /// This test makes sure that it is possible to start the block production
    /// task even if there is no pending block in db at the time of startup.
    #[rstest::rstest]
    #[tokio::test]
    async fn block_prod_pending_close_on_startup_no_pending(
        setup: (Arc<MadaraBackend>, Arc<mc_block_import::BlockImporter>, Arc<BlockProductionMetrics>),
    ) {
        let (backend, importer, metrics) = setup;

        // Simulates starting block production without a pending block in db
        BlockProductionTask::<Mempool>::close_pending_block(&backend, &importer, &metrics)
            .await
            .expect("Failed to close pending block");

        // Now we check no block was added to the db
        assert_eq!(backend.get_latest_block_n().unwrap(), None);
    }

    /// This test makes sure that if a pending block is already present in db
    /// at startup, then it is closed and stored in db, event if has no visited
    /// segments.
    ///
    /// This will arise if switching from a full node to a sequencer with the
    /// same db.
    #[rstest::rstest]
    #[tokio::test]
    #[allow(clippy::too_many_arguments)]
    async fn block_prod_pending_close_on_startup_no_visited_segments(
        setup: (Arc<MadaraBackend>, Arc<mc_block_import::BlockImporter>, Arc<BlockProductionMetrics>),
        #[with(Felt::ONE)] tx_invoke_v0: TxFixtureInfo,
        #[with(Felt::TWO)] tx_l1_handler: TxFixtureInfo,
        #[with(Felt::THREE)] tx_declare_v0: TxFixtureInfo,
        tx_deploy: TxFixtureInfo,
        tx_deploy_account: TxFixtureInfo,
        #[from(converted_class_legacy)]
        #[with(Felt::ZERO)]
        converted_class_legacy_0: mp_class::ConvertedClass,
        #[from(converted_class_sierra)]
        #[with(Felt::ONE, Felt::ONE)]
        converted_class_sierra_1: mp_class::ConvertedClass,
        #[from(converted_class_sierra)]
        #[with(Felt::TWO, Felt::TWO)]
        converted_class_sierra_2: mp_class::ConvertedClass,
        bouncer_weights: BouncerWeights,
    ) {
        let (backend, importer, metrics) = setup;

        // ================================================================== //
        //                  PART 1: we prepare the pending block              //
        // ================================================================== //

        let pending_inner = mp_block::MadaraBlockInner {
            transactions: vec![tx_invoke_v0.0, tx_l1_handler.0, tx_declare_v0.0, tx_deploy.0, tx_deploy_account.0],
            receipts: vec![tx_invoke_v0.1, tx_l1_handler.1, tx_declare_v0.1, tx_deploy.1, tx_deploy_account.1],
        };

        let pending_state_diff = mp_state_update::StateDiff {
            storage_diffs: vec![
                ContractStorageDiffItem {
                    address: Felt::ONE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::TWO,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::THREE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
            ],
            deprecated_declared_classes: vec![Felt::ZERO],
            declared_classes: vec![
                DeclaredClassItem { class_hash: Felt::ONE, compiled_class_hash: Felt::ONE },
                DeclaredClassItem { class_hash: Felt::TWO, compiled_class_hash: Felt::TWO },
            ],
            deployed_contracts: vec![DeployedContractItem { address: Felt::THREE, class_hash: Felt::THREE }],
            replaced_classes: vec![ReplacedClassItem { contract_address: Felt::TWO, class_hash: Felt::TWO }],
            nonces: vec![
                NonceUpdate { contract_address: Felt::ONE, nonce: Felt::ONE },
                NonceUpdate { contract_address: Felt::TWO, nonce: Felt::TWO },
                NonceUpdate { contract_address: Felt::THREE, nonce: Felt::THREE },
            ],
        };

        let converted_classes =
            vec![converted_class_legacy_0.clone(), converted_class_sierra_1.clone(), converted_class_sierra_2.clone()];

        // ================================================================== //
        //                   PART 2: storing the pending block                //
        // ================================================================== //

        // This simulates a node restart after shutting down midway during block
        // production.
        backend
            .store_block(
                mp_block::MadaraMaybePendingBlock {
                    info: mp_block::MadaraMaybePendingBlockInfo::Pending(mp_block::MadaraPendingBlockInfo {
                        header: mp_block::header::PendingHeader::default(),
                        tx_hashes: vec![Felt::ONE, Felt::TWO, Felt::THREE],
                    }),
                    inner: pending_inner.clone(),
                },
                pending_state_diff.clone(),
                converted_classes.clone(),
                None, // No visited segments!
                Some(bouncer_weights),
            )
            .expect("Failed to store pending block");

        // ================================================================== //
        //        PART 3: init block production and seal pending block        //
        // ================================================================== //

        // This should load the pending block from db and close it
        BlockProductionTask::<Mempool>::close_pending_block(&backend, &importer, &metrics)
            .await
            .expect("Failed to close pending block");

        // Now we check this was the case.
        assert_eq!(backend.get_latest_block_n().unwrap().unwrap(), 0);

        let block_inner = backend
            .get_block(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest))
            .expect("Failed to retrieve latest block from db")
            .expect("Missing latest block")
            .inner;
        assert_eq!(block_inner, pending_inner);

        let state_diff = backend
            .get_block_state_diff(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest))
            .expect("Failed to retrieve latest state diff from db")
            .expect("Missing latest state diff");
        assert_eq!(state_diff, pending_state_diff);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::ZERO)
            .expect("Failed to retrieve class at hash 0x0 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_legacy_0);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::ONE)
            .expect("Failed to retrieve class at hash 0x1 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_sierra_1);

        let class = backend
            .get_converted_class(&mp_block::BlockId::Tag(mp_block::BlockTag::Latest), &Felt::TWO)
            .expect("Failed to retrieve class at hash 0x2 from db")
            .expect("Missing class at index 0x0");
        assert_eq!(class, converted_class_sierra_2);

        // visited segments and bouncer weights are currently not stored for in
        // ready blocks
    }

    /// This test makes sure that closing the pending block from db will fail if
    /// the pending state diff references a non-existing class.
    #[rstest::rstest]
    #[tokio::test]
    #[allow(clippy::too_many_arguments)]
    async fn block_prod_pending_close_on_startup_fail_missing_class(
        setup: (Arc<MadaraBackend>, Arc<mc_block_import::BlockImporter>, Arc<BlockProductionMetrics>),
        #[with(Felt::ONE)] tx_invoke_v0: TxFixtureInfo,
        #[with(Felt::TWO)] tx_l1_handler: TxFixtureInfo,
        #[with(Felt::THREE)] tx_declare_v0: TxFixtureInfo,
        tx_deploy: TxFixtureInfo,
        tx_deploy_account: TxFixtureInfo,
        visited_segments: VisitedSegments,
        bouncer_weights: BouncerWeights,
    ) {
        let (backend, importer, metrics) = setup;

        // ================================================================== //
        //                  PART 1: we prepare the pending block              //
        // ================================================================== //

        let pending_inner = mp_block::MadaraBlockInner {
            transactions: vec![tx_invoke_v0.0, tx_l1_handler.0, tx_declare_v0.0, tx_deploy.0, tx_deploy_account.0],
            receipts: vec![tx_invoke_v0.1, tx_l1_handler.1, tx_declare_v0.1, tx_deploy.1, tx_deploy_account.1],
        };

        let pending_state_diff = mp_state_update::StateDiff {
            storage_diffs: vec![
                ContractStorageDiffItem {
                    address: Felt::ONE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::TWO,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::THREE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
            ],
            deprecated_declared_classes: vec![],
            declared_classes: vec![DeclaredClassItem { class_hash: Felt::ONE, compiled_class_hash: Felt::ONE }],
            deployed_contracts: vec![DeployedContractItem { address: Felt::THREE, class_hash: Felt::THREE }],
            replaced_classes: vec![ReplacedClassItem { contract_address: Felt::TWO, class_hash: Felt::TWO }],
            nonces: vec![
                NonceUpdate { contract_address: Felt::ONE, nonce: Felt::ONE },
                NonceUpdate { contract_address: Felt::TWO, nonce: Felt::TWO },
                NonceUpdate { contract_address: Felt::THREE, nonce: Felt::THREE },
            ],
        };

        let converted_classes = vec![];

        // ================================================================== //
        //                   PART 2: storing the pending block                //
        // ================================================================== //

        backend
            .store_block(
                mp_block::MadaraMaybePendingBlock {
                    info: mp_block::MadaraMaybePendingBlockInfo::Pending(mp_block::MadaraPendingBlockInfo {
                        header: mp_block::header::PendingHeader::default(),
                        tx_hashes: vec![Felt::ONE, Felt::TWO, Felt::THREE],
                    }),
                    inner: pending_inner.clone(),
                },
                pending_state_diff.clone(),
                converted_classes.clone(),
                Some(visited_segments.clone()),
                Some(bouncer_weights),
            )
            .expect("Failed to store pending block");

        // ================================================================== //
        //        PART 3: init block production and seal pending block        //
        // ================================================================== //

        // This should fail since the pending state update references a
        // non-existent declared class at address 0x1
        let err = BlockProductionTask::<Mempool>::close_pending_block(&backend, &importer, &metrics)
            .await
            .expect_err("Should error");

        assert!(err.contains("Failed to retrieve pending declared class at hash"));
        assert!(err.contains("not found in db"));
    }

    /// This test makes sure that closing the pending block from db will fail if
    /// the pending state diff references a non-existing legacy class.
    #[rstest::rstest]
    #[tokio::test]
    #[allow(clippy::too_many_arguments)]
    async fn block_prod_pending_close_on_startup_fail_missing_class_legacy(
        setup: (Arc<MadaraBackend>, Arc<mc_block_import::BlockImporter>, Arc<BlockProductionMetrics>),
        #[with(Felt::ONE)] tx_invoke_v0: TxFixtureInfo,
        #[with(Felt::TWO)] tx_l1_handler: TxFixtureInfo,
        #[with(Felt::THREE)] tx_declare_v0: TxFixtureInfo,
        tx_deploy: TxFixtureInfo,
        tx_deploy_account: TxFixtureInfo,
        visited_segments: VisitedSegments,
        bouncer_weights: BouncerWeights,
    ) {
        let (backend, importer, metrics) = setup;

        // ================================================================== //
        //                  PART 1: we prepare the pending block              //
        // ================================================================== //

        let pending_inner = mp_block::MadaraBlockInner {
            transactions: vec![tx_invoke_v0.0, tx_l1_handler.0, tx_declare_v0.0, tx_deploy.0, tx_deploy_account.0],
            receipts: vec![tx_invoke_v0.1, tx_l1_handler.1, tx_declare_v0.1, tx_deploy.1, tx_deploy_account.1],
        };

        let pending_state_diff = mp_state_update::StateDiff {
            storage_diffs: vec![
                ContractStorageDiffItem {
                    address: Felt::ONE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::TWO,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
                ContractStorageDiffItem {
                    address: Felt::THREE,
                    storage_entries: vec![
                        StorageEntry { key: Felt::ZERO, value: Felt::ZERO },
                        StorageEntry { key: Felt::ONE, value: Felt::ONE },
                        StorageEntry { key: Felt::TWO, value: Felt::TWO },
                    ],
                },
            ],
            deprecated_declared_classes: vec![Felt::ZERO],
            declared_classes: vec![],
            deployed_contracts: vec![DeployedContractItem { address: Felt::THREE, class_hash: Felt::THREE }],
            replaced_classes: vec![ReplacedClassItem { contract_address: Felt::TWO, class_hash: Felt::TWO }],
            nonces: vec![
                NonceUpdate { contract_address: Felt::ONE, nonce: Felt::ONE },
                NonceUpdate { contract_address: Felt::TWO, nonce: Felt::TWO },
                NonceUpdate { contract_address: Felt::THREE, nonce: Felt::THREE },
            ],
        };

        let converted_classes = vec![];

        // ================================================================== //
        //                   PART 2: storing the pending block                //
        // ================================================================== //

        backend
            .store_block(
                mp_block::MadaraMaybePendingBlock {
                    info: mp_block::MadaraMaybePendingBlockInfo::Pending(mp_block::MadaraPendingBlockInfo {
                        header: mp_block::header::PendingHeader::default(),
                        tx_hashes: vec![Felt::ONE, Felt::TWO, Felt::THREE],
                    }),
                    inner: pending_inner.clone(),
                },
                pending_state_diff.clone(),
                converted_classes.clone(),
                Some(visited_segments.clone()),
                Some(bouncer_weights),
            )
            .expect("Failed to store pending block");

        // ================================================================== //
        //        PART 3: init block production and seal pending block        //
        // ================================================================== //

        // This should fail since the pending state update references a
        // non-existent declared class at address 0x0
        let err = BlockProductionTask::<Mempool>::close_pending_block(&backend, &importer, &metrics)
            .await
            .expect_err("Should error");

        assert!(err.contains("Failed to retrieve pending declared class at hash"));
        assert!(err.contains("not found in db"));
    }
}
