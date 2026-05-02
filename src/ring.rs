use std::collections::{BTreeMap, HashSet};
use std::hash::{DefaultHasher, Hash, Hasher};

#[derive(Clone, Debug)]
pub struct HashRing {
    ring: BTreeMap<u64, String>,
    pub vnodes: usize,
}

impl HashRing {
    pub fn new(vnodes: usize) -> Self {
        Self {
            ring: BTreeMap::new(),
            vnodes,
        }
    }

    pub fn add_volume(&mut self, volume_id: &str) {
        self.remove_volume(volume_id);
        for vnode in 0..self.vnodes {
            let point = hash64(&format!("{volume_id}#{vnode}"));
            self.ring.insert(point, volume_id.to_string());
        }
    }

    pub fn remove_volume(&mut self, volume_id: &str) {
        self.ring.retain(|_, id| id != volume_id);
    }

    pub fn replicas_for(&self, key: &str, n: usize) -> Vec<String> {
        if n == 0 || self.ring.is_empty() {
            return Vec::new();
        }

        let start = hash64(key);
        let mut seen = HashSet::new();
        let mut replicas = Vec::new();

        for (_, volume_id) in self.ring.range(start..).chain(self.ring.range(..start)) {
            if seen.insert(volume_id.clone()) {
                replicas.push(volume_id.clone());
                if replicas.len() == n {
                    break;
                }
            }
        }

        replicas
    }
}

impl Default for HashRing {
    fn default() -> Self {
        Self::new(64)
    }
}

fn hash64(value: &str) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}
