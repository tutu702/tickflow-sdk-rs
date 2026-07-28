use std::{collections::HashMap, hash::Hash};

pub(crate) fn merge_maps<K, V>(maps: impl IntoIterator<Item = HashMap<K, V>>) -> HashMap<K, V>
where
    K: Eq + Hash,
{
    let mut out = HashMap::new();
    for m in maps {
        out.extend(m);
    }
    out
}
