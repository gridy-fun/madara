use std::sync::Arc;

use blockifier::transaction::transaction_execution as btx;
use mc_db::MadaraBackend;
use mp_block::BlockId;
use mp_convert::ToFelt;
use starknet_api::contract_class::{
    ClassInfo as ApiClassInfo, ContractClass as ApiContractClass, SierraVersion as ApiSierraVersion,
};
use starknet_api::transaction::{Transaction, TransactionHash};

use crate::errors::{StarknetRpcApiError, StarknetRpcResult};

/// Convert an starknet-api Transaction to a blockifier Transaction
///
/// **note:** this function does not support deploy transaction
/// because it is not supported by blockifier
pub fn to_blockifier_transaction(
    backend: Arc<MadaraBackend>,
    block_id: BlockId,
    transaction: mp_transactions::Transaction,
    tx_hash: &TransactionHash,
) -> StarknetRpcResult<btx::Transaction> {
    let transaction: Transaction = transaction.try_into().map_err(|_| StarknetRpcApiError::InternalServerError)?;

    let paid_fee_on_l1 = match transaction {
        Transaction::L1Handler(_) => Some(starknet_api::transaction::fields::Fee(1_000_000_000_000)),
        _ => None,
    };

    let class_info = match transaction {
        Transaction::Declare(ref declare_tx) => {
            let class_hash = declare_tx.class_hash();

            let Ok(Some(class_info)) = backend.get_class_info(&block_id, &class_hash.to_felt()) else {
                tracing::error!("Failed to retrieve class from class_hash '{class_hash}'");
                return Err(StarknetRpcApiError::ContractNotFound);
            };

            match class_info {
                mp_class::ClassInfo::Sierra(info) => {
                    let compiled_class = backend
                        .get_sierra_compiled(&block_id, &info.compiled_class_hash)
                        .map_err(|e| {
                            tracing::error!(
                                "Failed to retrieve sierra compiled class from class_hash '{class_hash}': {e}"
                            );
                            StarknetRpcApiError::InternalServerError
                        })?
                        .ok_or_else(|| {
                            tracing::error!(
                                "Inconsistent state: compiled sierra class from class_hash '{class_hash}' not found"
                            );
                            StarknetRpcApiError::InternalServerError
                        })?;
                    let contract_class = ApiContractClass::V1(compiled_class.to_casm().map_err(|e| {
                        tracing::error!("Failed to convert contract class to starknet-api contract class: {e}");
                        StarknetRpcApiError::InternalServerError
                    })?);
                    Some(ApiClassInfo {
                        contract_class,
                        sierra_program_length: info.contract_class.program_length(),
                        abi_length: info.contract_class.abi_length(),
                        sierra_version: info.contract_class.sierra_version().map_err(|e| {
                            tracing::error!("Failed to get sierra version: {e}");
                            StarknetRpcApiError::InternalServerError
                        })?,
                    })
                }
                mp_class::ClassInfo::Legacy(info) => {
                    let contract_class =
                        ApiContractClass::V0(info.contract_class.to_starknet_api_no_abi().map_err(|e| {
                            tracing::error!("Failed to convert contract class to starknet-api contract class: {e}");
                            StarknetRpcApiError::InternalServerError
                        })?);
                    Some(ApiClassInfo {
                        contract_class,
                        sierra_program_length: 0,
                        abi_length: 0,
                        sierra_version: ApiSierraVersion::zero(),
                    })
                }
            }
        }
        _ => None,
    };

    btx::Transaction::from_api(transaction.clone(), *tx_hash, class_info, paid_fee_on_l1, None, false).map_err(|_| {
        tracing::error!("Failed to convert transaction to blockifier transaction");
        StarknetRpcApiError::InternalServerError
    })
}
