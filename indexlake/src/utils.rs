pub fn has_duplicated_items(mut container: impl Iterator<Item = impl PartialEq>) -> bool {
    // TODO better solution
    let vec = container.collect::<Vec<_>>();
    for i in 0..vec.len() {
        for j in (i + 1)..vec.len() {
            if vec[i] == vec[j] {
                return true;
            }
        }
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
