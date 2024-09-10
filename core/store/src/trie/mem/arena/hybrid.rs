use std::sync::Arc;

use super::alloc::Allocator;
use super::{Arena, ArenaMemory, ArenaMemoryMut, ArenaMut, ArenaPos, ArenaSliceMut, STArenaMemory};

pub struct HybridArenaMemory {
    owned_memory: STArenaMemory,
    shared_memory: Arc<STArenaMemory>,
}

impl HybridArenaMemory {
    pub fn new(shared_memory: Arc<STArenaMemory>) -> Self {
        Self { owned_memory: STArenaMemory::new(), shared_memory }
    }

    #[inline]
    fn chunks_offset(&self) -> u32 {
        self.shared_memory.chunks.len() as u32
    }
}

impl ArenaMemory for HybridArenaMemory {
    fn raw_slice(&self, mut pos: ArenaPos, len: usize) -> &[u8] {
        if pos.chunk >= self.chunks_offset() {
            pos.chunk -= self.chunks_offset();
            self.owned_memory.raw_slice(pos, len)
        } else {
            self.shared_memory.raw_slice(pos, len)
        }
    }
}

impl ArenaMemoryMut for HybridArenaMemory {
    fn raw_slice_mut(&mut self, mut pos: ArenaPos, len: usize) -> &mut [u8] {
        assert!(pos.chunk >= self.chunks_offset(), "Cannot mutate shared memory");
        pos.chunk -= self.chunks_offset();
        self.owned_memory.raw_slice_mut(pos, len)
    }
}

pub struct HybridArena {
    memory: HybridArenaMemory,
    allocator: Allocator,
}

impl HybridArena {
    #[allow(dead_code)]
    pub fn new(name: String, shared_memory: Arc<STArenaMemory>) -> Self {
        Self { memory: HybridArenaMemory::new(shared_memory), allocator: Allocator::new(name) }
    }
}

impl Arena for HybridArena {
    type Memory = HybridArenaMemory;

    fn memory(&self) -> &Self::Memory {
        &self.memory
    }
}

impl ArenaMut for HybridArena {
    type MemoryMut = HybridArenaMemory;

    fn memory_mut(&mut self) -> &mut Self::Memory {
        &mut self.memory
    }

    fn alloc(&mut self, size: usize) -> ArenaSliceMut<Self::Memory> {
        let ArenaSliceMut { mut pos, len, .. } =
            self.allocator.allocate(&mut self.memory.owned_memory, size);
        pos.chunk = pos.chunk + self.memory.chunks_offset();
        ArenaSliceMut::new(&mut self.memory, pos, len)
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use crate::trie::mem::arena::{
        Arena, ArenaMemory, ArenaMemoryMut, ArenaMut, ArenaPos, STArenaMemory,
    };

    use super::HybridArena;

    #[test]
    fn test_hybrid_arena() {
        let size = 50;
        let pos1 = ArenaPos { chunk: 1, pos: 25 };

        // Create and populate shared memory with 2 chunks
        let mut shared_memory = STArenaMemory::new();
        shared_memory.chunks.push(vec![0; 1000]);
        shared_memory.chunks.push(vec![0; 1000]);
        for i in 0..size {
            shared_memory.chunks[pos1.chunk()][pos1.pos() + i] = (100 + i) as u8;
        }

        // Create and populate hybrid arena
        let mut hybrid_arena = HybridArena::new("hybrid".to_string(), Arc::new(shared_memory));
        let mut slice = hybrid_arena.alloc(size);
        for i in 0..size {
            slice.raw_slice_mut()[i] = i as u8;
        }

        // Verify pos2 allocated memory has chunk >= 2
        let pos2 = slice.raw_pos();
        assert_eq!(pos2, ArenaPos { chunk: 2, pos: 0 });

        // Verify shared and owned memory written values
        for i in 0..size {
            let shared_mem_val =
                hybrid_arena.memory().slice(pos1, size).subslice(i, 1).raw_slice()[0];
            let owned_mem_val =
                hybrid_arena.memory().slice(pos2, size).subslice(i, 1).raw_slice()[0];

            assert_eq!(shared_mem_val, (100 + i) as u8);
            assert_eq!(owned_mem_val, i as u8);
        }
    }

    #[test]
    #[should_panic(expected = "Cannot mutate shared memory")]
    fn test_hybrid_arena_panic_on_mut_access_shared_memory() {
        let mut shared_memory = STArenaMemory::new();
        shared_memory.chunks.push(vec![0; 1000]);
        shared_memory.chunks.push(vec![0; 1000]);

        let mut hybrid_arena = HybridArena::new("hybrid".to_string(), Arc::new(shared_memory));
        let _slice = hybrid_arena.memory_mut().raw_slice_mut(ArenaPos { chunk: 1, pos: 25 }, 50);
    }
}
