//! Ports of the `java.util` pieces whose exact behaviour the consistent hash output depends on.

use std::cmp::Ordering;

/// `java.util.PriorityQueue` (JDK 17) with natural ordering supplied by `cmp`: a binary heap whose
/// tie-breaking between equal elements is the heap's, so the poll order for equal keys only matches
/// Java if `siftUp`/`siftDown` are the same (`PriorityQueue.java`: `offer`, `poll`, `siftUp`,
/// `siftDown`). The JDK moves a "hole" and writes the key once at the end; swapping is the same
/// permutation.
pub struct PriorityQueue<T> {
    queue: Vec<T>,
    cmp: fn(&T, &T) -> Ordering,
}

impl<T> PriorityQueue<T> {
    /// An empty queue ordered by `cmp`.
    pub fn new(cmp: fn(&T, &T) -> Ordering) -> Self {
        Self { queue: Vec::new(), cmp }
    }

    /// Whether the queue is empty.
    pub fn is_empty(&self) -> bool {
        self.queue.is_empty()
    }

    /// The number of elements.
    pub fn len(&self) -> usize {
        self.queue.len()
    }

    /// Removes every element.
    pub fn clear(&mut self) {
        self.queue.clear();
    }

    /// `offer`/`add`.
    pub fn add(&mut self, e: T) {
        let mut k = self.queue.len();
        self.queue.push(e);
        while k > 0 {
            let parent = (k - 1) >> 1;
            if (self.cmp)(&self.queue[k], &self.queue[parent]) != Ordering::Less {
                break;
            }
            self.queue.swap(k, parent);
            k = parent;
        }
    }

    /// `poll`/`remove()`.
    pub fn poll(&mut self) -> Option<T> {
        if self.queue.is_empty() {
            return None;
        }
        let n = self.queue.len() - 1;
        self.queue.swap(0, n);
        let result = self.queue.pop();
        if n > 0 {
            self.sift_down(0, n);
        }
        result
    }

    fn sift_down(&mut self, mut k: usize, n: usize) {
        let half = n >> 1;
        while k < half {
            let mut child = 2 * k + 1;
            let right = child + 1;
            if right < n && (self.cmp)(&self.queue[child], &self.queue[right]) == Ordering::Greater {
                child = right;
            }
            if (self.cmp)(&self.queue[k], &self.queue[child]) != Ordering::Greater {
                break;
            }
            self.queue.swap(k, child);
            k = child;
        }
    }
}

/// `Arrays.binarySearch(long[], long)`: index of the match, or `-(insertion point) - 1`.
pub fn binary_search_long(a: &[i64], key: i64) -> isize {
    let mut low: isize = 0;
    let mut high: isize = a.len() as isize - 1;
    while low <= high {
        let mid = ((low + high) as usize >> 1) as isize;
        let mid_val = a[mid as usize];
        if mid_val < key {
            low = mid + 1;
        } else if mid_val > key {
            high = mid - 1;
        } else {
            return mid;
        }
    }
    -(low + 1)
}

/// How a `java.util.HashMap` was constructed, which fixes its table size.
#[derive(Debug, Clone, Copy)]
pub enum HashMapInit {
    /// `new HashMap<>()`: 16 buckets.
    Default,
    /// `new HashMap<>(initialCapacity)`.
    Capacity(i32),
    /// `new HashMap<>(otherMapOfThisSize)` (`HashMap.putMapEntries`, `:518-539`).
    CopyOf(usize),
}

/// `HashMap.tableSizeFor`.
fn table_size_for(cap: i32) -> usize {
    if cap <= 1 { 1 } else { (cap as u32).next_power_of_two() as usize }
}

/// Iteration order of a `java.util.HashMap` after putting entries with the given `hashCode()`s in
/// order (distinct keys). Returns indices into `hashes`.
///
/// A bucket chain keeps insertion order and a resize split (`HashMap.resize`, lo/hi lists) keeps
/// relative order, so the final order is the stable sort by bucket index at the final table size.
/// Treeified bins (8+ colliding keys in a 64+ table) iterate the same way for insertion-ordered
/// keys, so they are not modelled.
pub fn hash_map_iteration_order(hashes: &[i32], init: HashMapInit) -> Vec<usize> {
    let mut cap = match init {
        HashMapInit::Default => 16,
        HashMapInit::Capacity(n) => table_size_for(n),
        HashMapInit::CopyOf(s) => table_size_for((s as f32 / 0.75 + 1.0) as i32),
    };
    let mut thr = (cap as f32 * 0.75) as usize;
    let mut size = 0usize;
    for _ in hashes {
        size += 1;
        if size > thr {
            cap *= 2;
            thr = (cap as f32 * 0.75) as usize;
        }
    }
    let mut idx: Vec<usize> = (0..hashes.len()).collect();
    idx.sort_by_key(|&i| {
        let h = hashes[i];
        let spread = h ^ ((h as u32) >> 16) as i32;
        (spread as u32 as usize) & (cap - 1)
    });
    idx
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn pq_polls_in_order_with_stable_heap_ties() {
        let mut q = PriorityQueue::new(|a: &(i64, u32), b: &(i64, u32)| a.0.cmp(&b.0));
        for (i, d) in [5i64, 1, 5, 1, 3, 3, 5, 0].iter().enumerate() {
            q.add((*d, i as u32));
        }
        let mut out = vec![];
        while let Some(x) = q.poll() {
            out.push(x);
        }
        assert_eq!(out.iter().map(|x| x.0).collect::<Vec<_>>(), vec![0, 1, 1, 3, 3, 5, 5, 5]);
    }

    #[test]
    fn binary_search_matches_java() {
        let a = [1i64, 4, 4, 9];
        assert_eq!(binary_search_long(&a, 9), 3);
        assert_eq!(binary_search_long(&a, 0), -1);
        assert_eq!(binary_search_long(&a, 5), -4);
        assert_eq!(binary_search_long(&a, 10), -5);
    }
}
