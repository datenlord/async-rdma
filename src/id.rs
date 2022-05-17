use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

/// Creat a random u64 id.
///
/// Both `WorkRequetId` and `AgentRequestId` depend on this, so make this fn independent.
/// To avoid id duplication, this fn concatenates `SystemTime` and random number into a U64.
/// The syscall may have some overhead, which can be improved later by balancing the pros and cons.
pub(crate) fn random_u64() -> u64 {
    let start = SystemTime::now();
    // No time can be earlier than Unix Epoch
    #[allow(clippy::unwrap_used)]
    let since_the_epoch = start.duration_since(UNIX_EPOCH).unwrap();
    let time = since_the_epoch.subsec_micros();
    let rand = rand::thread_rng().gen::<u32>();
    let left: u64 = time.into();
    let right: u64 = rand.into();
    left.wrapping_shl(32) | right
}
