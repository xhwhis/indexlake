use std::{collections::HashSet, hash::Hash};

pub fn has_duplicated_items(container: impl Iterator<Item = impl Eq + Hash>) -> bool {
    let mut set = HashSet::new();
    for item in container {
        if set.contains(&item) {
            return true;
        }
        set.insert(item);
    }
    false
}

#[cfg(test)]
mod tests {
    use crate::utils::has_duplicated_items;

    #[test]
    fn test_has_duplicated_items() {
        let items = vec![1, 2, 3, 4, 5];
        assert!(!has_duplicated_items(items.iter()));

        let items_with_duplicates = vec![1, 2, 3, 4, 5, 3];
        assert!(has_duplicated_items(items_with_duplicates.iter()));
    }
}
