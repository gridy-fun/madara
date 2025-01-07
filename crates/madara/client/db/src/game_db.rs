use crate::DatabaseExt;
use crate::{Column, MadaraBackend};
use rocksdb::WriteBatch;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
pub struct AddressNode {
    address: String,
    next: Option<String>,     // Next address in sequence
    previous: Option<String>, // Previous address in sequence
}

#[derive(Serialize, Deserialize, Debug, Default)]
pub struct ListMetadata {
    game_address: String,
    head: Option<String>,               // First address
    tail: Option<String>,               // Last address
    pub next_iter_addr: Option<String>, // Next address for iterator
    pub length: u64,                    // Number of addresses in list
    pub tiles_mined: u64,               // Number of tiles mined
}

const MINES_PER_TRANSACTION: i64 = 5;
const METADATA_KEY: &[u8] = b"list_metadata";

impl MadaraBackend {
    pub fn game_get_metadata(&self) -> Result<ListMetadata, rocksdb::Error> {
        let col = self.db.get_column(Column::Game);
        let meta = self
            .db
            .get_cf(&col, METADATA_KEY)?
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap_or(ListMetadata::default());
        Ok(meta)
    }

    pub fn game_update_metadata(&self, updates: impl FnOnce(&mut ListMetadata)) -> Result<(), rocksdb::Error> {
        let col = self.db.get_column(Column::Game);
        let mut batch = WriteBatch::default();

        // Get current metadata or create default if none exists
        let mut metadata = self
            .db
            .get_cf(&col, METADATA_KEY)?
            .map(|bytes| serde_json::from_slice(&bytes).unwrap())
            .unwrap_or(ListMetadata::default());

        // Apply the updates to the metadata
        updates(&mut metadata);

        // Serialize and write the updated metadata
        let serialized = serde_json::to_vec(&metadata).unwrap();
        batch.put_cf(&col, METADATA_KEY, serialized);

        // Write the batch to the database
        self.db.write(batch)?;

        Ok(())
    }

    /// Add new bot address to the linked list
    /// 1. Create new node with current address, with previous pointing to old tail
    /// 2. Add new node to the database
    /// 3. If it's first address, set head & tail to this address
    /// 4. Update old tail's next field to point to new address
    /// 5. Update tail to the new address
    /// 6. Update metadata, including tail and length
    /// 7. Write batch to the database
    pub fn game_add_bot_address(&self, address: &str) -> Result<(), rocksdb::Error> {
        let col = self.db.get_column(Column::Game);
        let mut meta = self.game_get_metadata()?;

        let new_node = AddressNode {
            address: address.to_string(),
            next: None,
            previous: meta.tail.clone(), // Point to old tail
        };

        let mut batch = WriteBatch::default();

        // If list is empty
        if meta.head.is_none() {
            meta.head = Some(address.to_string());
            meta.tail = Some(address.to_string());
        } else {
            // Update old tail to point to new address
            if let Some(old_tail) = &meta.tail {
                let mut old_tail_node: AddressNode =
                    serde_json::from_slice(&self.db.get_cf(&col, old_tail.as_bytes())?.unwrap()).unwrap();
                old_tail_node.next = Some(address.to_string());
                batch.put_cf(&col, old_tail.as_bytes(), serde_json::to_vec(&old_tail_node).unwrap());
            }
            meta.tail = Some(address.to_string());
        }

        meta.length += 1;

        batch.put_cf(&col, address.as_bytes(), serde_json::to_vec(&new_node).unwrap());
        batch.put_cf(&col, METADATA_KEY, serde_json::to_vec(&meta).unwrap());

        // Added New Node
        // Updated old tail to point to new node
        // Incremented metadata length by 1

        self.db.write(batch)
    }

    /// Delete a game address from the list
    /// 1. Modify next pointer of previous node to point to node after this one
    /// 2. Modify previous pointer of next node to point to node before this one
    /// 3. Delete provided node. Reduce length by 1 in metadata
    /// 4. Write batch to the database
    pub fn game_delete_bot_address(&self, address: &str) -> Result<(), rocksdb::Error> {
        let col = self.db.get_column(Column::Game);
        let mut meta = self.game_get_metadata()?;

        // Get the node to delete
        if let Some(node_bytes) = self.db.get_cf(&col, address.as_bytes())? {
            let node: AddressNode = serde_json::from_slice(&node_bytes).unwrap();
            let mut batch = WriteBatch::default();

            // Update previous node's next pointer
            if let Some(prev_addr) = &node.previous {
                let mut prev_node: AddressNode =
                    serde_json::from_slice(&self.db.get_cf(&col, prev_addr.as_bytes())?.unwrap()).unwrap();
                prev_node.next = node.next.clone();
                batch.put_cf(&col, prev_addr.as_bytes(), serde_json::to_vec(&prev_node).unwrap());
            } else {
                // Deleting head
                meta.head = node.next.clone();
            }

            // Update next node's previous pointer
            if let Some(next_addr) = &node.next {
                let mut next_node: AddressNode =
                    serde_json::from_slice(&self.db.get_cf(&col, next_addr.as_bytes())?.unwrap()).unwrap();
                next_node.previous = node.previous;
                batch.put_cf(&col, next_addr.as_bytes(), serde_json::to_vec(&next_node).unwrap());
            } else {
                // Deleting tail
                meta.tail = node.previous;
            }

            meta.length -= 1;

            // If the deleted node was the one being iterated over, update the next iteration pointer
            if let Some(next_iter_addr) = meta.next_iter_addr.as_ref() {
                if next_iter_addr == &address {
                    meta.next_iter_addr = node.next;
                }
            }

            batch.delete_cf(&col, address.as_bytes());
            batch.put_cf(&col, METADATA_KEY, serde_json::to_vec(&meta).unwrap());

            // Delete the node
            // Updated previous node to point to next node
            // Decremented metadata length by 1

            return self.db.write(batch);
        }

        Ok(()) // Address not found
    }

    pub fn game_get_all_bot_addresses(&self) -> Result<Vec<String>, rocksdb::Error> {
        let col = self.db.get_column(Column::Game);
        let meta = self.game_get_metadata()?;
        let mut addresses = Vec::new();

        let mut current = meta.head;
        while let Some(addr) = current {
            addresses.push(addr.clone());
            let node: AddressNode = serde_json::from_slice(&self.db.get_cf(&col, addr.as_bytes())?.unwrap()).unwrap();
            current = node.next;
        }

        assert!(
            addresses.len() == meta.length as usize,
            "fetched addresses list != stored addresses length in metadata"
        );

        Ok(addresses)
    }

    pub fn game_get_bots_list(&self) -> Result<Vec<String>, rocksdb::Error> {
        let col = self.db.get_column(Column::Game);
        let meta = self.game_get_metadata()?;

        let next_start_address = meta.next_iter_addr;

        // Handle empty list case
        if meta.length == 0 {
            return Ok(vec![]);
        }
        let mut return_bots = Vec::with_capacity(MINES_PER_TRANSACTION as usize);

        // Start from the given address or head if None
        let mut current_address = match next_start_address {
            Some(addr) => Some(addr),
            None => meta.head.clone(),
        };

        // Keep adding bots until we reach MINES_PER_TRANSACTION
        while return_bots.len() < MINES_PER_TRANSACTION as usize {
            if let Some(addr) = &current_address {
                // Add current address
                return_bots.push(addr.clone());

                // Get next address, if we reach end, start from head
                let node: AddressNode =
                    serde_json::from_slice(&self.db.get_cf(&col, addr.as_bytes())?.unwrap()).unwrap();

                current_address = match node.next {
                    Some(next) => Some(next),
                    None => meta.head.clone(), // Wrap around to start
                };
            } else {
                break; // Should never happen if meta.length > 0
            }
        }

        let mut meta = self.game_get_metadata()?;
        // Update metadata with next start address
        meta.next_iter_addr = current_address;
        self.db.put_cf(&col, METADATA_KEY, serde_json::to_vec(&meta).unwrap())?;
        // The next start address will be current_address

        Ok(return_bots)
    }
}
