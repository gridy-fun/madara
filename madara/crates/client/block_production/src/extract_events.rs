use blockifier::{
    execution::call_info::{CallInfo, OrderedEvent},
    transaction::objects::TransactionExecutionInfo,
};
/// Function to extract all events from a transaction execution info
pub fn extract_all_events(tx_info: &TransactionExecutionInfo) -> Vec<&OrderedEvent> {
    let mut all_events = Vec::new();

    // Process execute_call_info if it exists
    if let Some(call_info) = &tx_info.execute_call_info {
        collect_events_from_call_info(call_info, &mut all_events);
    }

    // Process validate_call_info if it exists
    if let Some(call_info) = &tx_info.validate_call_info {
        collect_events_from_call_info(call_info, &mut all_events);
    }

    // Process fee_transfer_call_info if it exists
    if let Some(call_info) = &tx_info.fee_transfer_call_info {
        collect_events_from_call_info(call_info, &mut all_events);
    }

    all_events
}

/// Recursive function to collect events from a CallInfo and its inner calls
fn collect_events_from_call_info<'a>(call_info: &'a CallInfo, events: &mut Vec<&'a OrderedEvent>) {
    // Add events from this call
    events.extend(call_info.execution.events.iter());

    // Recursively process inner calls
    for inner_call in &call_info.inner_calls {
        collect_events_from_call_info(inner_call, events);
    }
}

// /// Function to find an event by its key
// pub fn find_event_by_key<'a>(tx_info: &'a TransactionExecutionInfo, target_key: &str) -> Option<&'a OrderedEvent> {
//     let all_events = extract_all_events(tx_info);

//     // Parse the target key as a Felt
//     let target_felt = Felt::from_hex_unchecked(target_key);

//     // Find the first event that contains the target key
//     all_events.into_iter().find(|event| event.event.keys.iter().any(|key| key.0 == target_felt))
// }

// /// Function to create a map of all event keys to their events
// pub fn create_event_key_map<'a>(tx_info: &'a TransactionExecutionInfo) -> HashMap<&'a EventKey, &'a OrderedEvent> {
//     let all_events = extract_all_events(tx_info);
//     let mut key_to_event = HashMap::new();

//     for event in all_events {
//         for key in &event.event.keys {
//             key_to_event.insert(key, event);
//         }
//     }

//     key_to_event
// }

// Usage example
// pub fn usage_example(tx_info: &TransactionExecutionInfo) {
//     // Get all events in a flat vector
//     let all_events = extract_all_events(tx_info);
//     println!("Total events found: {}", all_events.len());

//     // Create a map for quick lookup
//     let event_map = create_event_key_map(tx_info);

//     // Find a specific event by key
//     let target_key_hex = "0x2cd0383e81a65036ae8acc94ac89e891d1385ce01ae6cc127c27615f5420fa3";
//     if let Some(event) = find_event_by_key(tx_info, target_key_hex) {
//         println!("Found event with order: {}", event.order);
//         println!("Event data: {:?}", event.event.data);
//     } else {
//         println!("Event with key {} not found", target_key_hex);
//     }

//     // You can also directly access the event if you have the EventKey object
//     if let Ok(target_felt) = Felt::from_hex_string(target_key_hex) {
//         let target_key = EventKey(target_felt);
//         if let Some(event) = event_map.get(&target_key) {
//             println!("Found event via map lookup with order: {}", event.order);
//         }
//     }
// }
