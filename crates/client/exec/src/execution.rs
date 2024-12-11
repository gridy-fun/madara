use blockifier::fee::fee_utils::get_fee_by_gas_vector;
use blockifier::fee::gas_usage::estimate_minimal_gas_vector;
use blockifier::state::cached_state::TransactionalState;
use blockifier::transaction::errors::TransactionExecutionError;
use blockifier::transaction::objects::{HasRelatedFeeType, TransactionExecutionInfo};
use blockifier::transaction::transaction_execution::Transaction;
use blockifier::transaction::transaction_types::TransactionType;
use blockifier::transaction::transactions::{ExecutableTransaction, ExecutionFlags};
use starknet_api::block::FeeType;
use starknet_api::transaction::fields::GasVectorComputationMode;
use starknet_api::transaction::TransactionHash;

use crate::{Error, ExecutionContext, ExecutionResult, TxReexecError};

impl ExecutionContext {
    /// Execute transactions. The returned `ExecutionResult`s are the results of the `transactions_to_trace`. The results of `transactions_before` are discarded.
    /// This function is useful for tracing trasaction execution, by reexecuting the block.
    pub fn re_execute_transactions(
        &self,
        transactions_before: impl IntoIterator<Item = Transaction>,
        transactions_to_trace: impl IntoIterator<Item = Transaction>,
        charge_fee: bool,
        validate: bool,
    ) -> Result<Vec<ExecutionResult>, Error> {
        let mut cached_state = self.init_cached_state();

        let mut executed_prev = 0;
        for (index, tx) in transactions_before.into_iter().enumerate() {
            let hash = tx.tx_hash();
            tracing::debug!("executing {hash:#}");
            tx.execute(&mut cached_state, &self.block_context, charge_fee, validate).map_err(|err| TxReexecError {
                block_n: self.db_id,
                hash,
                index,
                err,
            })?;
            executed_prev += 1;
        }

        transactions_to_trace
            .into_iter()
            .enumerate()
            .map(|(index, tx): (_, Transaction)| {
                let hash = tx.tx_hash();
                tracing::debug!("executing {hash:#} (trace)");
                let tx_type = tx.tx_type();
                let fee_type = tx.fee_type();

                // We need to estimate gas too.
                let minimal_l1_gas = match &tx {
                    Transaction::Account(tx) => {
                        Some(estimate_minimal_gas_vector(&self.block_context, tx, &GasVectorComputationMode::NoL2Gas))
                    }
                    Transaction::L1Handler(_) => None, // There is no minimal_l1_gas field for L1 handler transactions.
                };

                let make_reexec_error =
                    |err| TxReexecError { block_n: self.db_id, hash, index: executed_prev + index, err };

                let mut transactional_state = TransactionalState::create_transactional(&mut cached_state);
                let execution_flags = ExecutionFlags { charge_fee, validate, concurrency_mode: false };
                // NB: We use execute_raw because execute already does transaactional state.
                let execution_info = tx
                    .execute_raw(&mut transactional_state, &self.block_context, execution_flags)
                    .and_then(|mut tx_info: TransactionExecutionInfo| {
                        // TODO: why was this here again?
                        if tx_info.receipt.fee.0 == 0 {
                            let gas_vector = tx_info.receipt.resources.to_gas_vector(
                                self.block_context.versioned_constants(),
                                self.block_context.block_info().use_kzg_da,
                                &GasVectorComputationMode::NoL2Gas,
                            );
                            let real_fees =
                                get_fee_by_gas_vector(self.block_context.block_info(), gas_vector, &fee_type);

                            tx_info.receipt.fee = real_fees;
                        }
                        Ok(tx_info)
                    })
                    .map_err(make_reexec_error)?;

                let state_changes = transactional_state
                    .to_state_diff()
                    .map_err(TransactionExecutionError::StateError)
                    .map_err(make_reexec_error)?;
                transactional_state.commit();

                Ok(ExecutionResult {
                    hash,
                    tx_type,
                    fee_type,
                    minimal_l1_gas,
                    execution_info,
                    state_diff: state_changes.state_maps.into(),
                })
            })
            .collect::<Result<Vec<_>, _>>()
    }
}

pub trait TxInfo {
    fn tx_hash(&self) -> TransactionHash;
    fn tx_type(&self) -> TransactionType;
    fn fee_type(&self) -> FeeType;
}

impl TxInfo for Transaction {
    fn tx_hash(&self) -> TransactionHash {
        Transaction::tx_hash(self)
    }

    fn tx_type(&self) -> TransactionType {
        match self {
            Self::Account(tx) => tx.tx_type(),
            Self::L1Handler(_) => TransactionType::L1Handler,
        }
    }

    fn fee_type(&self) -> FeeType {
        match self {
            Self::Account(tx) => tx.fee_type(),
            Self::L1Handler(tx) => tx.fee_type(),
        }
    }
}
