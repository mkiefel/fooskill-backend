use std::error;
use std::fmt;

use transaction::Transaction;

/// Represents a versioned version of an object that can be merged with the
/// implementation in this module.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Mergeable<I, T>
where
    I: Eq,
{
    V0(MergeableV0<I, T>),
}

/// Contains all version 0 information for a node that can be merged in the
/// forest.
#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MergeableV0<I, T>
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
        Mergeable::V0(MergeableV0 {
            parent_index: index,
            rank: 0,
            item,
        })
    }

    /// Unwraps to the latest node.
    fn latest(self) -> MergeableV0<I, T> {
        match self {
            Mergeable::V0(inner) => inner,
        }
    }

    /// Wraps a node into a versioned node.
    fn wrap(inner: MergeableV0<I, T>) -> Self {
        Mergeable::V0(inner)
    }
}

impl<I, T> MergeableV0<I, T>
where
    I: Eq,
{
    fn is_root(&self, index: &I) -> bool {
        self.parent_index == *index
    }
}

/// Is used to lookup the nodes from a storage implementation.
pub trait MergeCtx {
    type Index: Eq;
    type Item;

    /// Tries to load/get a node from storage given the passed index.
    ///
    /// # Arguments
    ///
    /// * `index` index of the node to lookup.
    fn get_node(&mut self, index: &Self::Index) -> Option<Mergeable<Self::Index, Self::Item>>;

    /// Sets a node inside the storage.
    ///
    /// # Arguments
    ///
    /// * `index` index of the node to lookup.
    fn set_node(&mut self, index: &Self::Index, item: Mergeable<Self::Index, Self::Item>);
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

struct Set<K, V, C>
where
    K: Eq,
{
    index: K,
    item: V,
    ctx_: std::marker::PhantomData<C>,
}

impl<K, V, C> Set<K, V, C>
where
    K: Eq,
{
    pub fn new(index: K, item: V) -> Self {
        Set {
            index,
            item,
            ctx_: std::marker::PhantomData,
        }
    }
}

impl<K, V, C> Transaction for Set<K, V, C>
where
    K: Eq + Clone,
    V: Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    type Ctx = C;
    type Item = ();
    type Err = Error<K>;

    fn run(&self, ctx: &mut Self::Ctx) -> Result<Self::Item, Self::Err> {
        let mut node = Find::new(self.index.clone()).run(ctx)?.latest();
        node.item = self.item.clone();
        let index = node.parent_index.clone();
        ctx.set_node(&index, Mergeable::wrap(node));
        Ok(())
    }
}

pub struct Find<K, V, C> {
    index: K,
    value_: std::marker::PhantomData<V>,
    ctx_: std::marker::PhantomData<C>,
}

impl<K, V, C> Find<K, V, C> {
    pub fn new(index: K) -> Self {
        Find {
            index,
            value_: std::marker::PhantomData,
            ctx_: std::marker::PhantomData,
        }
    }
}

impl<K, V, C> Transaction for Find<K, V, C>
where
    K: Eq + Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    type Ctx = C;
    type Item = Mergeable<K, V>;
    type Err = Error<K>;

    fn run(&self, ctx: &mut Self::Ctx) -> Result<Self::Item, Self::Err> {
        let mut index = self.index.clone();
        let mut node = ctx
            .get_node(&index)
            .ok_or_else(|| Error::MissingEntryError(self.index.clone()))?
            .latest();

        while !node.is_root(&index) {
            let parent_index = node.parent_index.clone();
            let parent = ctx
                .get_node(&parent_index)
                .ok_or_else(|| Error::NoParentError(index.clone()))?
                .latest();

            node.parent_index = parent.parent_index.clone();
            ctx.set_node(&index, Mergeable::wrap(node));

            index = parent_index;
            node = parent;
        }

        Ok(Mergeable::wrap(node))
    }
}

struct Merge<K, V, F, C>
where
    F: Fn(&V, &mut V),
{
    left_index: K,
    right_index: K,
    merge_op: F,
    value_: std::marker::PhantomData<V>,
    context_: std::marker::PhantomData<C>,
}

impl<'a, K, V, F, C> Merge<K, V, F, C>
where
    F: Fn(&V, &mut V),
{
    pub fn new(left_index: K, right_index: K, merge_op: F) -> Self
    where
        F: Fn(&V, &mut V),
    {
        Merge {
            left_index,
            right_index,
            merge_op,
            value_: std::marker::PhantomData,
            context_: std::marker::PhantomData,
        }
    }
}

impl<K, V, F, C> Transaction for Merge<K, V, F, C>
where
    V: Clone,
    K: Eq + Clone,
    C: MergeCtx<Index = K, Item = V>,
    F: Fn(&V, &mut V),
{
    type Ctx = C;
    type Item = Mergeable<K, V>;
    type Err = Error<K>;

    fn run(&self, ctx: &mut Self::Ctx) -> Result<Self::Item, Self::Err> {
        let mut left = Find::new(self.left_index.clone()).run(ctx)?.latest();
        let left_index = left.parent_index.clone();
        let mut right = Find::new(self.right_index.clone()).run(ctx)?.latest();
        let right_index = right.parent_index.clone();

        if left_index == right_index {
            return Ok(Mergeable::wrap(left));
        }

        if left.rank < right.rank {
            (self.merge_op)(&left.item, &mut right.item);
            ctx.set_node(&right_index, Mergeable::wrap(right.clone()));
            left.parent_index = right_index;
            ctx.set_node(&left_index, Mergeable::wrap(left));

            return Ok(Mergeable::wrap(right));
        } else {
            (self.merge_op)(&right.item, &mut left.item);
            if left.rank == right.rank {
                left.rank += 1;
            }
            ctx.set_node(&left_index, Mergeable::wrap(left.clone()));
            right.parent_index = left_index;
            ctx.set_node(&right_index, Mergeable::wrap(right));

            return Ok(Mergeable::wrap(left));
        }
    }
}

/// Finds an entry in a union-find forest.
pub fn find<K, V, C>(index: K) -> impl Transaction<Ctx = C, Item = V, Err = Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    let find_op = Find::new(index);
    find_op.map(|node| node.latest().item)
}

/// Merges two trees in a union-find forest.
pub fn merge<K, V, F, C>(
    left_index: K,
    right_index: K,
    merge_op: F,
) -> impl Transaction<Ctx = C, Item = V, Err = Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    F: Fn(&V, &mut V),
    C: MergeCtx<Index = K, Item = V>,
{
    let merge_op = Merge::new(left_index, right_index, merge_op);
    merge_op.map(|node| node.latest().item)
}

/// Sets the value of a node inside a union-find forest.
pub fn set<K, V, C>(index: K, item: V) -> impl Transaction<Ctx = C, Item = (), Err = Error<K>>
where
    K: Eq + Clone,
    V: Clone,
    C: MergeCtx<Index = K, Item = V>,
{
    Set::new(index, item)
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

        fn get_node(&mut self, index: &Self::Index) -> Option<Mergeable<Self::Index, Self::Item>> {
            self.write_elements.get(*index).map(|s| s.to_owned())
        }

        fn set_node(&mut self, index: &Self::Index, item: Mergeable<Self::Index, Self::Item>) {
            self.write_elements
                .get_mut(*index)
                .map(|element| *element = item);
        }
    }

    impl MemoryStore {
        fn run<T, R>(&mut self, t: &T) -> Result<R, Error<usize>>
        where
            T: Transaction<Ctx = MemoryStoreCtx, Item = R, Err = Error<usize>>,
        {
            let mut ops = MemoryStoreCtx {
                write_elements: self.elements.clone(),
            };
            let r = t.run(&mut ops)?;
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
        let find_first = store.run(&find(0));
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
