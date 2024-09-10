use std::sync::Arc;

use super::{Arena, ArenaMemory, ArenaPos, STArenaMemory};

#[derive(Clone)]
pub struct FrozenArenaMemory {
    shared_memory: Arc<STArenaMemory>,
}

#[derive(Clone)]
pub struct FrozenArena {
    memory: FrozenArenaMemory,
}

impl ArenaMemory for FrozenArenaMemory {
    fn raw_slice(&self, pos: ArenaPos, len: usize) -> &[u8] {
        self.shared_memory.raw_slice(pos, len)
    }
}

impl FrozenArena {
    #[allow(dead_code)]
    pub fn new(shared_memory: STArenaMemory) -> Self {
        Self { memory: FrozenArenaMemory { shared_memory: Arc::new(shared_memory) } }
    }
}

impl Arena for FrozenArena {
    type Memory = FrozenArenaMemory;

    fn memory(&self) -> &Self::Memory {
        &self.memory
    }
}
