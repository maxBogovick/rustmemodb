/// Computes a stable shard ID for an entity using FNV-1a hash.
///
/// Ensures valid consistent hashing across the cluster given a fixed shard count.
pub fn stable_shard_for(entity_type: &str, entity_id: &str, shard_count: u32) -> u32 {
    if shard_count == 0 {
        return 0;
    }
    let mut hash = 14695981039346656037u64;
    for byte in entity_type.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    hash ^= 0xff;
    hash = hash.wrapping_mul(1099511628211);
    for byte in entity_id.as_bytes() {
        hash ^= *byte as u64;
        hash = hash.wrapping_mul(1099511628211);
    }
    (hash % shard_count as u64) as u32
}
