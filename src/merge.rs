use std::error;
use std::fmt;

use async_trait::async_trait;
use serde::{Deserialize, Serialize};

/// Represents an object that can be merged with the implementation in this
/// module.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct Mergeable<I, T>
where
    I: Eq,
{
    /// Index of the parent of this node.
    parent_index: I,
    /// Height of the tree, if this node were to be the root.
    rank: u64,
    item: T,
}

impl<I, T> Mergeable<I, T>
where
    I: Eq,
{
    pub fn new(index: I, item: T) -> Self {
        Mergeable {
            parent_index: index,
            rank: 0,
            item,
        }
    }

    fn is_root(&self, index: &I) -> bool {
        self.parent_index == *index
    }
}

/// Is used to lookup the nodes from a storage implementation.
#[async_trait]
pub trait MergeCtx {
    type Index: Eq;
    type Item;

    /// Tries to load/get a node from storage given the passed index.
    ///
    /// # Arguments
    ///
    /// * `index` index of the node to lookup.
    async fn get_node(&mut self, index: &Self::Index)
        -> Option<Mergeable<Self::Index, Self::Item>>;

    /// Sets a node inside the storage.
    ///
    /// # Arguments
    ///
    /// * `index` index of the node to lookup.
    async fn set_node(&mut self, index: &Self::Index, item: Mergeable<Self::Index, Self::Item>);
}

/// Represents a merge error.
#[derive(Debug)]
pub enum Error<K> {
    /// The operation did not find the key it was expecting to exist.
    MissingEntryError(K),
    /// Although a node specifies a parent key, the node does not exist.
    NoParentError(K),
}

impl<K> fmt::Display for Error<K>
where
    K: fmt::Debug,
{
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::MissingEntryError(ref index) => write!(f, "no entry with index {:?}", index),
            Error::NoParentError(ref index) => {
                write!(f, "missing parent for node with index {:?}", index)
            }
        }
    }
}

impl<K> error::Error for Error<K>
where
    K: fmt::Debug,
{
    fn source(&self) -> Option<&(dyn error::Error + 'static)> {
        None
    }
}

async fn run_find<K, V, C>(ctx: &mut C, index: K) -> Result<Mergeable<K, V>, Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    let mut index = index.clone();
    let mut node = ctx
        .get_node(&index)
        .await
        .ok_or_else(|| Error::MissingEntryError(index.clone()))?;

    while !node.is_root(&index) {
        let parent_index = node.parent_index.clone();
        let parent = ctx
            .get_node(&parent_index)
            .await
            .ok_or_else(|| Error::NoParentError(index.clone()))?;

        node.parent_index = parent.parent_index.clone();
        ctx.set_node(&index, node).await;

        index = parent_index;
        node = parent;
    }

    Ok(node)
}

async fn run_set<K, V, C>(ctx: &mut C, index: K, item: V) -> Result<(), Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    let mut node = run_find(ctx, index.clone()).await?;
    node.item = item.clone();
    let index = node.parent_index.clone();
    ctx.set_node(&index, node).await;
    Ok(())
}

async fn run_merge<K, V, F, C>(
    ctx: &mut C,
    left_index: K,
    right_index: K,
    merge_op: F,
) -> Result<Mergeable<K, V>, Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    F: Fn(&V, &mut V),
    C: MergeCtx<Index = K, Item = V>,
{
    let mut left = run_find(ctx, left_index.clone()).await?;
    let left_index = left.parent_index.clone();
    let mut right = run_find(ctx, right_index.clone()).await?;
    let right_index = right.parent_index.clone();

    if left_index == right_index {
        return Ok(left);
    }

    if left.rank < right.rank {
        merge_op(&left.item, &mut right.item);
        ctx.set_node(&right_index, right.clone()).await;
        left.parent_index = right_index;
        ctx.set_node(&left_index, left).await;

        Ok(right)
    } else {
        merge_op(&right.item, &mut left.item);
        if left.rank == right.rank {
            left.rank += 1;
        }
        ctx.set_node(&left_index, left.clone()).await;
        right.parent_index = left_index;
        ctx.set_node(&right_index, right).await;

        Ok(left)
    }
}

/// Finds an entry in a union-find forest.
pub async fn find<K, V, C>(ctx: &mut C, index: K) -> Result<V, Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    // TODO(mkiefel): key should be passed as reference.
    run_find(ctx, index).await.map(|node| node.item)
}

/// Merges two trees in a union-find forest.
pub async fn merge<K, V, F, C>(
    ctx: &mut C,
    left_index: K,
    right_index: K,
    merge_op: F,
) -> Result<V, Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    F: Fn(&V, &mut V),
    C: MergeCtx<Index = K, Item = V>,
{
    run_merge(ctx, left_index, right_index, merge_op)
        .await
        .map(|node| node.item)
}

/// Sets the value of a node inside a union-find forest.
pub async fn set<K, V, C>(ctx: &mut C, index: K, item: V) -> Result<(), Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    run_set(ctx, index, item).await
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug)]
    struct MemoryStore {
        elements: Vec<Mergeable<usize, String>>,
    }

    #[derive(Debug)]
    struct MemoryStoreCtx {
        write_elements: Vec<Mergeable<usize, String>>,
    }

    impl MergeCtx for MemoryStoreCtx {
        type Index = usize;
        type Item = String;

        async fn get_node(
            &mut self,
            index: &Self::Index,
        ) -> Option<Mergeable<Self::Index, Self::Item>> {
            self.write_elements.get(*index).map(|s| s.to_owned())
        }

        async fn set_node(
            &mut self,
            index: &Self::Index,
            item: Mergeable<Self::Index, Self::Item>,
        ) {
            self.write_elements
                .get_mut(*index)
                .map(|element| *element = item);
        }
    }

    impl MemoryStore {
        fn run<T, R>(&mut self, t: T) -> Result<R, Error<usize>>
        where
            T: Fn(&mut MemoryStoreCtx) -> R,
        {
            let mut ops = MemoryStoreCtx {
                write_elements: self.elements.clone(),
            };
            let r = t(&mut ops)?;
            self.elements = ops.write_elements;
            Ok(r)
        }
    }

    fn simple_store() -> MemoryStore {
        MemoryStore {
            elements: vec![
                Mergeable::new(0, "first".to_owned()),
                Mergeable::new(1, "second".to_owned()),
                Mergeable::new(2, "third".to_owned()),
            ],
        }
    }

    #[test]
    fn test_find() {
        let mut store = simple_store();
        let find_first = store.run(|ctx| find(ctx, 0));
        assert!(find_first.is_ok());
        assert_eq!(find_first.unwrap(), "first");
        let find_second = store.run(&find(1));
        assert!(find_second.is_ok());
        assert_eq!(find_second.unwrap(), "second");
    }

    #[test]
    fn test_merge() {
        let mut store = simple_store();
        let merge = store.run(&merge(0, 1, |left: &String, right: &mut String| {
            *right = left.to_owned() + " " + right
        }));
        assert!(merge.is_ok());
        let merged_item = merge.unwrap();
        assert!(merged_item == "first second" || merged_item == "second first");

        let find_first = store.run(&find(0));
        assert!(find_first.is_ok());
        assert_eq!(find_first.unwrap(), merged_item);

        let find_second = store.run(&find(1));
        assert!(find_second.is_ok());
        assert_eq!(find_second.unwrap(), merged_item);

        let find_third = store.run(&find(2));
        assert!(find_third.is_ok());
        assert_eq!(find_third.unwrap(), "third");
    }

    #[test]
    fn test_missing() {
        let mut store = simple_store();
        let missing = store.run(&find(14));
        match missing {
            Err(Error::MissingEntryError(14)) => assert!(true),
            _ => assert!(false, "Entry should not exist"),
        }
    }

    #[test]
    fn test_missing_parent() {
        let mut store = MemoryStore {
            elements: vec![Mergeable::new(1, "first".to_owned())],
        };
        let missing_parent = store.run(&find(0));
        match missing_parent {
            Err(Error::NoParentError(0)) => assert!(true),
            _ => assert!(false, "Parent should not exist"),
        }
    }

    #[test]
    fn test_path_halving() {
        let mut store = MemoryStore {
            elements: vec![
                Mergeable::new(0, "first".to_owned()),
                Mergeable::new(0, "second".to_owned()),
                Mergeable::new(1, "third".to_owned()),
                Mergeable::new(2, "forth".to_owned()),
            ],
        };
        let find_on_leaf = store.run(&find(3));
        assert!(find_on_leaf.is_ok());
        assert_eq!(find_on_leaf.unwrap(), "first");
        let find_on_leaf = store.run(&find(3));
        assert!(find_on_leaf.is_ok());
        assert_eq!(find_on_leaf.unwrap(), "first");
    }
}
