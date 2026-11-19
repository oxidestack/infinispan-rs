//! Port of `SyncConsistentHashFactory` (core 14.0.35.Final,
//! `distribution/ch/impl/SyncConsistentHashFactory.java`) and its `Builder`.
//!
//! The segment placement must match Java bit for bit, so the details that look accidental are kept:
//! `float` arithmetic in `computeExpectedSegments`/`nodeSegmentDistance`, `PriorityQueue` heap
//! order for equal distances ([`PriorityQueue`]), and `nodeCanOwnSegment` reading `ownerIndices`.
// Index loops mirror upstream's `for (int i = 0; ...)` so the line-by-line comparison stays easy.
#![allow(clippy::needless_range_loop)]


use std::cmp::Ordering;
use std::collections::HashSet;

use super::address::{Address, CapacityFactors};
use super::default_consistent_hash::DefaultConsistentHash;
use super::java_util::{PriorityQueue, binary_search_long};
use super::murmur_hash3::murmur_hash3_x64_64_longs;

#[derive(Clone, Copy)]
struct SegmentInfo {
    segment: usize,
    node_index: usize,
    distance: i64,
}

fn segment_info_cmp(a: &SegmentInfo, b: &SegmentInfo) -> Ordering {
    // Long.compare(distance, o.distance)
    a.distance.cmp(&b.distance)
}

/// `SyncConsistentHashFactory.checkCapacityFactors` (`:68-80`).
pub fn check_capacity_factors(members: &[Address], capacity_factors: Option<&CapacityFactors>) -> Result<(), String> {
    if let Some(cf) = capacity_factors {
        let mut total = 0f32;
        for node in members {
            match cf.get(node) {
                Some(f) if f >= 0.0 => total += f,
                other => {
                    return Err(format!(
                        "Invalid capacity factor for node {node:?}: {}",
                        other.map(|f| f.to_string()).unwrap_or_else(|| "null".into())
                    ));
                }
            }
        }
        if total == 0.0 {
            return Err("There must be at least one node with a non-zero capacity factor".into());
        }
    }
    Ok(())
}

/// `SyncConsistentHashFactory.create` (`:56-66`).
pub fn create(
    num_owners: i32,
    num_segments: i32,
    members: &[Address],
    capacity_factors: Option<&CapacityFactors>,
) -> Result<DefaultConsistentHash, String> {
    check_capacity_factors(members, capacity_factors)?;
    let mut builder = Builder::new(num_owners, num_segments, members, capacity_factors);
    builder.populate_owners();
    Ok(DefaultConsistentHash::new(num_owners, num_segments, members, capacity_factors, builder.segment_owners))
}

/// `SyncConsistentHashFactory.updateMembers` (`:84-122`). Returns a clone of `base` when nothing
/// changes (Java returns the same instance).
pub fn update_members(
    base: &DefaultConsistentHash,
    new_members: &[Address],
    actual_capacity_factors: Option<&CapacityFactors>,
) -> Result<DefaultConsistentHash, String> {
    check_capacity_factors(new_members, actual_capacity_factors)?;

    let base_cf = base.capacity_factors();
    let same_cf = match actual_capacity_factors {
        None => base_cf.is_none(),
        Some(a) => base_cf.as_ref() == Some(a),
    };
    if new_members == base.members() && same_cf {
        return Ok(base.clone());
    }

    let num_segments = base.num_segments();
    let num_owners = base.num_owners();
    let leavers: HashSet<&Address> = base.members().iter().filter(|m| !new_members.contains(m)).collect();

    let mut rebalanced: Option<DefaultConsistentHash> = None;
    let mut new_segment_owners: Vec<Vec<Address>> = Vec::with_capacity(num_segments);
    for s in 0..num_segments {
        let owners: Vec<Address> =
            base.locate_owners_for_segment(s).iter().filter(|o| !leavers.contains(o)).cloned().collect();
        if !owners.is_empty() {
            new_segment_owners.push(owners);
        } else {
            if rebalanced.is_none() {
                rebalanced = Some(create(num_owners, num_segments as i32, new_members, actual_capacity_factors)?);
            }
            new_segment_owners.push(rebalanced.as_ref().unwrap().locate_owners_for_segment(s).to_vec());
        }
    }
    Ok(DefaultConsistentHash::new(
        num_owners,
        num_segments as i32,
        new_members,
        actual_capacity_factors,
        new_segment_owners,
    ))
}

/// `SyncConsistentHashFactory.rebalance` (`:124-134`).
pub fn rebalance(base: &DefaultConsistentHash) -> Result<DefaultConsistentHash, String> {
    let cf = base.capacity_factors();
    let rebalanced = create(base.num_owners(), base.num_segments() as i32, base.members(), cf.as_ref())?;
    if &rebalanced == base { Ok(base.clone()) } else { Ok(rebalanced) }
}

struct Builder {
    num_segments: usize,
    segment_owners: Vec<Vec<Address>>,
    owner_indices: Vec<Vec<i32>>,
    sorted_members: Vec<Address>,
    num_nodes: usize,
    sorted_capacity_factors: Vec<f32>,
    distance_factors: Vec<f32>,
    total_capacity: f32,
    actual_num_owners: usize,
    num_node_hashes: usize,
    segment_hashes: Vec<i64>,
    node_hashes: Vec<Vec<i64>>,
    // OwnershipStatistics
    owned: Vec<i32>,
    primary_owned: Vec<i32>,
    sum_owned: i32,
    sum_primary: i32,
}

impl Builder {
    fn new(num_owners: i32, num_segments: i32, members: &[Address], capacity_factors: Option<&CapacityFactors>) -> Self {
        let sorted_members = sort_members_by_capacity(members, capacity_factors);
        let sorted_capacity_factors: Vec<f32> = sorted_members
            .iter()
            .map(|m| capacity_factors.map(|c| c.get(m).unwrap()).unwrap_or(1.0))
            .collect();
        // computeTotalCapacity (`:292-300`): the array is never null here.
        let mut total_capacity = 0f32;
        for f in &sorted_capacity_factors {
            total_capacity += f;
        }
        let num_nodes = sorted_members.len();
        let actual_num_owners = (num_owners as usize).min(num_nodes);
        let min_capacity = sorted_capacity_factors[num_nodes - 1];
        let distance_factors: Vec<f32> = sorted_capacity_factors.iter().map(|c| min_capacity / c).collect();
        let num_segments_us = num_segments as usize;
        let segment_owners = (0..num_segments_us).map(|_| Vec::with_capacity(actual_num_owners)).collect();
        let owner_indices = (0..num_segments_us).map(|_| vec![0i32; actual_num_owners]).collect();

        let segment_size = i64::MAX / num_segments as i64;
        let mut segment_hashes = Vec::with_capacity(num_segments_us);
        let mut cur = segment_size >> 1;
        for _ in 0..num_segments_us {
            segment_hashes.push(cur);
            cur = cur.wrapping_add(segment_size);
        }
        // 32 - Integer.numberOfLeadingZeros(numSegments)
        let num_node_hashes = (32 - (num_segments as u32).leading_zeros()) as usize;
        let node_hashes = sorted_members
            .iter()
            .map(|m| {
                let mut h: Vec<i64> = (0..num_node_hashes).map(|v| node_hash(m, v as i32)).collect();
                h.sort_unstable();
                h
            })
            .collect();

        Builder {
            num_segments: num_segments_us,
            segment_owners,
            owner_indices,
            sorted_members,
            num_nodes,
            sorted_capacity_factors,
            distance_factors,
            total_capacity,
            actual_num_owners,
            num_node_hashes,
            segment_hashes,
            node_hashes,
            owned: vec![0; num_nodes],
            primary_owned: vec![0; num_nodes],
            sum_owned: 0,
            sum_primary: 0,
        }
    }

    fn compute_expected_segments(&self, expected_owners: i32, total_capacity: f32, iteration: i32) -> Vec<i32> {
        let mut expected = vec![0i32; self.num_nodes];
        let mut remaining_capacity = total_capacity;
        let mut remaining_copies = expected_owners * self.num_segments as i32;
        let average_segments = remaining_copies as f32 / self.num_nodes as f32;
        for n in 0..self.num_nodes {
            let capacity_factor = self.sorted_capacity_factors[n];
            if capacity_factor == 0.0 {
                expected[n] = 0;
            }
            let ideal = remaining_copies as f32 * capacity_factor / remaining_capacity;
            if ideal > self.num_segments as f32 {
                remaining_capacity -= capacity_factor;
                remaining_copies -= self.num_segments as i32;
                expected[n] = self.num_segments as i32;
            } else {
                expected[n] = fudge_expected_segments(ideal, average_segments, iteration);
            }
        }
        expected
    }

    fn node_segment_distance(&self, node_index: usize, segment_hash: i64) -> i64 {
        let current = &self.node_hashes[node_index];
        let mut hash_index = binary_search_long(current, segment_hash);
        if hash_index > 0 {
            // Found an exact match (index 0 is treated as no match, as upstream does)
            0
        } else {
            hash_index = -(hash_index + 1);
            let n = self.num_node_hashes as isize;
            let hash_before = if hash_index > 0 { current[(hash_index - 1) as usize] } else { current[(n - 1) as usize] };
            let hash_after = if hash_index < n { current[hash_index as usize] } else { current[0] };
            let distance = distance(hash_before, segment_hash).min(distance(hash_after, segment_hash));
            (distance as f32 * self.distance_factors[node_index]) as i64
        }
    }

    fn populate_owners(&mut self) {
        let queues_len = 1.max(self.actual_num_owners);
        let mut segment_queues: Vec<PriorityQueue<SegmentInfo>> =
            (0..queues_len).map(|_| PriorityQueue::new(segment_info_cmp)).collect();
        let mut temporary_queue: PriorityQueue<SegmentInfo> = PriorityQueue::new(segment_info_cmp);

        let total_capacity = self.total_capacity;
        self.assign_segments(1, total_capacity, 1, &mut segment_queues, &mut temporary_queue);
        debug_assert_eq!(self.sum_primary as usize, self.num_segments);
        let actual = self.actual_num_owners as i32;
        self.assign_segments(actual, total_capacity, actual - 1, &mut segment_queues, &mut temporary_queue);
        debug_assert_eq!(self.sum_owned as usize, self.actual_num_owners * self.num_segments);
    }

    fn assign_segments(
        &mut self,
        current_num_owners: i32,
        total_capacity: f32,
        queues_count: i32,
        segment_queues: &mut [PriorityQueue<SegmentInfo>],
        temporary_queue: &mut PriorityQueue<SegmentInfo>,
    ) {
        let total_copies = current_num_owners * self.num_segments as i32;
        let mut load_iteration = 0;
        while self.sum_owned < total_copies {
            let mut node_segments_to_add = self.compute_expected_segments(current_num_owners, total_capacity, load_iteration);
            let mut iteration_copies = 0;
            for n in 0..self.num_nodes {
                iteration_copies += node_segments_to_add[n];
                node_segments_to_add[n] -= self.owned[n];
            }
            iteration_copies = iteration_copies.max(total_copies);
            for _distance_iteration in 0..self.num_nodes {
                if self.sum_owned >= iteration_copies {
                    break;
                }
                self.populate_queues(current_num_owners, &node_segments_to_add, queues_count, segment_queues, temporary_queue);
                if !self.assign_queued_owners(
                    current_num_owners,
                    &mut node_segments_to_add,
                    queues_count,
                    iteration_copies,
                    segment_queues,
                ) {
                    break;
                }
            }
            load_iteration += 1;
        }
    }

    fn assign_queued_owners(
        &mut self,
        current_num_owners: i32,
        node_segments_to_add: &mut [i32],
        queues_count: i32,
        iteration_copies: i32,
        segment_queues: &mut [PriorityQueue<SegmentInfo>],
    ) -> bool {
        let mut assigned = false;
        for i in 0..queues_count as usize {
            while let Some(si) = segment_queues[i].poll() {
                let owner_position = self.segment_owners[si.segment].len();
                if node_segments_to_add[si.node_index] <= 0 {
                    continue;
                }
                if i == 0
                    || (self.segment_is_available(si.segment, current_num_owners)
                        && self.node_can_own_segment(si.segment, owner_position, si.node_index))
                {
                    self.assign_owner(si.segment, owner_position, si.node_index, node_segments_to_add);
                    assigned = true;
                }
                if self.sum_owned >= iteration_copies {
                    return assigned;
                }
            }
            segment_queues[i].clear();
        }
        assigned
    }

    fn populate_queues(
        &mut self,
        current_num_owners: i32,
        node_segments_to_add: &[i32],
        queue_count: i32,
        segment_queues: &mut [PriorityQueue<SegmentInfo>],
        temporary_queue: &mut PriorityQueue<SegmentInfo>,
    ) {
        let mut best: Option<SegmentInfo> = None;
        for s in 0..self.num_segments {
            if !self.segment_is_available(s, current_num_owners) {
                continue;
            }
            for n in 0..self.num_nodes {
                if node_segments_to_add[n] > 0 && self.node_can_own_segment(s, self.segment_owners[s].len(), n) {
                    let scaled = self.node_segment_distance(n, self.segment_hashes[s]);
                    if queue_count > 1 {
                        temporary_queue.add(SegmentInfo { segment: s, node_index: n, distance: scaled });
                    } else {
                        match &mut best {
                            None => best = Some(SegmentInfo { segment: s, node_index: n, distance: scaled }),
                            Some(b) if scaled < b.distance => {
                                b.node_index = n;
                                b.distance = scaled;
                            }
                            _ => {}
                        }
                    }
                }
            }
            if queue_count > 1 {
                let mut i = 0;
                while i < queue_count as usize && !temporary_queue.is_empty() {
                    segment_queues[i].add(temporary_queue.poll().unwrap());
                    i += 1;
                }
                temporary_queue.clear();
            } else {
                if let Some(b) = best.take() {
                    segment_queues[0].add(b);
                }
            }
        }
    }

    fn segment_is_available(&self, segment: usize, current_num_owners: i32) -> bool {
        (self.segment_owners[segment].len() as i32) < current_num_owners
    }

    fn assign_owner(&mut self, segment: usize, owner_position: usize, node_index: usize, wanted: &mut [i32]) {
        debug_assert!(wanted[node_index] > 0);
        wanted[node_index] -= 1;
        debug_assert_eq!(self.segment_owners[segment].len(), owner_position);
        self.segment_owners[segment].push(self.sorted_members[node_index].clone());
        self.owner_indices[segment][owner_position] = node_index as i32;
        // stats.incOwned(nodeIndex, ownerPosition == 0)
        self.owned[node_index] += 1;
        self.sum_owned += 1;
        if owner_position == 0 {
            self.primary_owned[node_index] += 1;
            self.sum_primary += 1;
        }
    }

    fn node_can_own_segment(&self, segment: usize, owner_position: usize, node_index: usize) -> bool {
        !self.owner_indices[segment][..owner_position].contains(&(node_index as i32))
    }
}

/// `fudgeExpectedSegments` (`:233-242`).
fn fudge_expected_segments(ideal_owned_segments: f32, average_segments: f32, iteration: i32) -> i32 {
    let step = f32::max(f32::min(average_segments * 0.05, ideal_owned_segments * 0.15), 1.0);
    i32::max((ideal_owned_segments + (iteration as f32 - 2.5) * step) as i32, 0)
}

/// `Builder.distance` (`:331-342`): distance between points in `0..2^63-1`, at most `2^62-1`.
fn distance(a: i64, b: i64) -> i64 {
    let mut d = if a < b { b.wrapping_sub(a) } else { a.wrapping_sub(b) };
    if d & (1i64 << 62) != 0 {
        d = d.wrapping_neg().wrapping_sub(i64::MIN);
    }
    d
}

/// `Builder.nodeHash` (`:311-329`).
fn node_hash(address: &Address, virtual_node: i32) -> i64 {
    let key: [i64; 2] = match address.uuid_bits() {
        Some((msb, lsb)) => [lsb, msb],
        None => [address.hash_code() as i64, 0],
    };
    murmur_hash3_x64_64_longs(&key, virtual_node) & i64::MAX
}

/// `Builder.sortMembersByCapacity` (`:213-232`): drop zero-capacity members, sort descending by
/// capacity with a stable sort (`List.sort` is TimSort).
fn sort_members_by_capacity(members: &[Address], capacity_factors: Option<&CapacityFactors>) -> Vec<Address> {
    let Some(cf) = capacity_factors else { return members.to_vec() };
    let mut sorted: Vec<Address> = members.iter().filter(|m| cf.get(m).unwrap() != 0.0).cloned().collect();
    // Float.compare(cf(a2), cf(a1)); `total_cmp` agrees with Float.compare on every non-NaN value.
    sorted.sort_by(|a1, a2| cf.get(a2).unwrap().total_cmp(&cf.get(a1).unwrap()));
    sorted
}
