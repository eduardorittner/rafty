use crate::node_id::ValidNodeId;
use std::iter::FromIterator;
use std::ops::{Index, IndexMut};

/// A map-like data structure optimized for small clusters with sequential node IDs.
///
/// Since cluster sizes are typically small (< 100 nodes) and node IDs are sequential,
/// this uses a `Vec<T>` as the backing store instead of a `HashMap`.
///
/// # Indexing
/// - Node IDs are 1-indexed (0 is invalid)
/// - `NodeMap[1]` accesses the first element (index 0 in the underlying Vec)
/// - `NodeMap[self_id]` contains a sentinel value and should not be used for actual data
///
/// # Type Parameters
/// - `T`: The type of values stored in the map
#[derive(Debug, Clone, PartialEq)]
pub struct NodeMap<T> {
    /// The underlying storage. Index `i` stores the value for node `i + 1`.
    /// The sentinel value at index `self_id - 1` is unused.
    data: Vec<T>,
    /// The node ID of the owner of this NodeMap (1-indexed)
    self_id: ValidNodeId,
}

impl<T> NodeMap<T> {
    /// Creates a new NodeMap with the given capacity and sentinel value.
    pub fn new(cluster_size: u64, self_id: ValidNodeId, sentinel: T) -> Self
    where
        T: Clone,
    {
        assert!(
            self_id.get() <= cluster_size,
            "Self ID {} must be <= cluster size {}",
            self_id.get(),
            cluster_size
        );

        let mut data = Vec::with_capacity(cluster_size as usize);
        for _ in 0..cluster_size {
            data.push(sentinel.clone());
        }

        Self { data, self_id }
    }

    /// Creates a new NodeMap from an iterator of values.
    ///
    /// # Panics
    /// Panics if the iterator doesn't produce exactly `cluster_size` elements,
    /// or if `self_id` is greater than `cluster_size`.
    pub fn from_iter<I>(cluster_size: u64, self_id: ValidNodeId, iter: I) -> Self
    where
        I: IntoIterator<Item = T>,
    {
        assert!(
            self_id.get() <= cluster_size,
            "Self ID {} must be <= cluster size {}",
            self_id.get(),
            cluster_size
        );

        let data: Vec<T> = iter.into_iter().collect();
        assert!(
            data.len() == cluster_size as usize,
            "Expected {} elements, got {}",
            cluster_size,
            data.len()
        );

        Self { data, self_id }
    }

    /// Returns the number of nodes in the cluster (including the sentinel position).
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Returns true if the NodeMap is empty (cluster size is 0).
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Returns a reference to the value for the given node ID.
    pub fn get(&self, node_id: ValidNodeId) -> &T {
        let index = node_id.to_index();
        &self.data[index]
    }

    /// Returns a mutable reference to the value for the given node ID.
    pub fn get_mut(&mut self, node_id: ValidNodeId) -> &mut T {
        let index = node_id.to_index();
        &mut self.data[index]
    }

    /// Sets the value for the given node ID and returns the old value.
    pub fn insert(&mut self, node_id: ValidNodeId, value: T) -> T {
        let index = node_id.to_index();
        std::mem::replace(&mut self.data[index], value)
    }

    /// Returns true if the node ID is valid (within cluster size).
    pub fn contains_key(&self, node_id: ValidNodeId) -> bool {
        node_id.to_index() < self.data.len()
    }

    /// Returns an iterator over all entries (node_id, value) pairs.
    /// This includes the sentinel value at the owner's position.
    pub fn iter(&self) -> impl Iterator<Item = (ValidNodeId, &T)> {
        self.data
            .iter()
            .enumerate()
            .map(|(i, v)| (ValidNodeId::from_index(i), v))
    }

    /// Returns an iterator over all entries (node_id, value) pairs, excluding the sentinel.
    pub fn iter_others(&self) -> impl Iterator<Item = (ValidNodeId, &T)> {
        let self_index = self.self_id.to_index();
        self.data
            .iter()
            .enumerate()
            .filter(move |(i, _)| *i != self_index)
            .map(|(i, v)| (ValidNodeId::from_index(i), v))
    }

    /// Returns a mutable iterator over all entries (node_id, value) pairs.
    /// This includes the sentinel value at the owner's position.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = (ValidNodeId, &mut T)> {
        self.data
            .iter_mut()
            .enumerate()
            .map(|(i, v)| (ValidNodeId::from_index(i), v))
    }

    /// Returns a mutable iterator over all entries (node_id, value) pairs, excluding the sentinel.
    pub fn iter_others_mut(&mut self) -> impl Iterator<Item = (ValidNodeId, &mut T)> {
        let self_index = self.self_id.to_index();
        self.data
            .iter_mut()
            .enumerate()
            .filter(move |(i, _)| *i != self_index)
            .map(|(i, v)| (ValidNodeId::from_index(i), v))
    }

    /// Returns the node ID of the owner of this NodeMap.
    pub fn self_id(&self) -> ValidNodeId {
        self.self_id
    }
}

impl<T: Clone> NodeMap<T> {
    /// Creates a new NodeMap with the same cluster size and self_id, but with a new sentinel value.
    pub fn with_new_sentinel(&self, new_sentinel: T) -> Self {
        let mut new_data = Vec::with_capacity(self.data.len());
        let self_index = self.self_id.to_index();
        for i in 0..self.data.len() {
            if i == self_index {
                new_data.push(new_sentinel.clone());
            } else {
                new_data.push(self.data[i].clone());
            }
        }
        Self {
            data: new_data,
            self_id: self.self_id,
        }
    }
}

impl<T> Index<ValidNodeId> for NodeMap<T> {
    type Output = T;

    fn index(&self, node_id: ValidNodeId) -> &Self::Output {
        self.get(node_id)
    }
}

impl<T> IndexMut<ValidNodeId> for NodeMap<T> {
    fn index_mut(&mut self, node_id: ValidNodeId) -> &mut Self::Output {
        self.get_mut(node_id)
    }
}

impl<T> FromIterator<T> for NodeMap<T> {
    fn from_iter<I: IntoIterator<Item = T>>(iter: I) -> Self {
        let data: Vec<T> = iter.into_iter().collect();
        // Default to self_id = 1 (first valid node ID)
        // This is mainly for compatibility with the FromIterator trait
        Self {
            data,
            self_id: ValidNodeId::from_index(0),
        }
    }
}

/// Iterator over references to NodeMap entries
pub struct NodeMapIter<'a, T> {
    inner: std::iter::Enumerate<std::slice::Iter<'a, T>>,
}

impl<'a, T> Iterator for NodeMapIter<'a, T> {
    type Item = (ValidNodeId, &'a T);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(i, v)| (ValidNodeId::from_index(i), v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

/// Iterator over mutable references to NodeMap entries
pub struct NodeMapIterMut<'a, T> {
    inner: std::iter::Enumerate<std::slice::IterMut<'a, T>>,
}

impl<'a, T> Iterator for NodeMapIterMut<'a, T> {
    type Item = (ValidNodeId, &'a mut T);

    fn next(&mut self) -> Option<Self::Item> {
        self.inner
            .next()
            .map(|(i, v)| (ValidNodeId::from_index(i), v))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl<'a, T> IntoIterator for &'a NodeMap<T> {
    type Item = (ValidNodeId, &'a T);
    type IntoIter = NodeMapIter<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        NodeMapIter {
            inner: self.data.iter().enumerate(),
        }
    }
}

impl<'a, T> IntoIterator for &'a mut NodeMap<T> {
    type Item = (ValidNodeId, &'a mut T);
    type IntoIter = NodeMapIterMut<'a, T>;

    fn into_iter(self) -> Self::IntoIter {
        NodeMapIterMut {
            inner: self.data.iter_mut().enumerate(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_map_basic() {
        let self_id = ValidNodeId::new(3).unwrap();
        let mut map: NodeMap<i32> = NodeMap::new(5, self_id, -1);

        // Test indexing (1-based)
        map[ValidNodeId::new(1).unwrap()] = 10;
        map[ValidNodeId::new(2).unwrap()] = 20;
        // map[3] is sentinel
        map[ValidNodeId::new(4).unwrap()] = 40;
        map[ValidNodeId::new(5).unwrap()] = 50;

        assert_eq!(map[ValidNodeId::new(1).unwrap()], 10);
        assert_eq!(map[ValidNodeId::new(2).unwrap()], 20);
        assert_eq!(map[ValidNodeId::new(4).unwrap()], 40);
        assert_eq!(map[ValidNodeId::new(5).unwrap()], 50);
    }

    #[test]
    fn test_node_map_sentinel() {
        let self_id = ValidNodeId::new(3).unwrap();
        let map: NodeMap<i32> = NodeMap::new(5, self_id, -1);

        // The sentinel value should be at position 3
        assert_eq!(map[ValidNodeId::new(3).unwrap()], -1);
    }

    #[test]
    fn test_iter_others() {
        let self_id = ValidNodeId::new(3).unwrap();
        let map: NodeMap<i32> = NodeMap::new(5, self_id, -1);

        let others: Vec<_> = map.iter_others().collect();
        assert_eq!(others.len(), 4);
        assert!(others.iter().all(|(id, _)| id.get() != 3));
    }

    #[test]
    fn test_contains_key() {
        let self_id = ValidNodeId::new(3).unwrap();
        let map: NodeMap<i32> = NodeMap::new(5, self_id, -1);

        assert!(map.contains_key(ValidNodeId::new(1).unwrap()));
        assert!(map.contains_key(ValidNodeId::new(5).unwrap()));
        assert!(!map.contains_key(ValidNodeId::new(6).unwrap()));
    }
}
