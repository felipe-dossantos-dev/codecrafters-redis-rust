use std::{
    cmp::Ordering,
    collections::{btree_set::Iter, BTreeMap, BTreeSet},
    iter::{Skip, Take},
    vec::IntoIter,
};

#[derive(Debug, Clone)]
pub struct SortedValue {
    pub member: String,
    pub score: f64,
}

impl PartialEq for SortedValue {
    fn eq(&self, other: &Self) -> bool {
        self.member == other.member && self.score.total_cmp(&other.score) == Ordering::Equal
    }
}

impl Eq for SortedValue {}

impl PartialOrd for SortedValue {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SortedValue {
    fn cmp(&self, other: &Self) -> Ordering {
        self.score
            .total_cmp(&other.score)
            .then_with(|| self.member.cmp(&other.member))
    }
}

#[derive(Debug)]
pub struct RedisSortedSet {
    set: BTreeSet<SortedValue>,
    map: BTreeMap<String, SortedValue>,
}

impl RedisSortedSet {
    pub fn new() -> Self {
        Self {
            set: BTreeSet::new(),
            map: BTreeMap::new(),
        }
    }

    /// Faz o insert caso não exista ou faz o replace caso exista, utiliza o member como chave, retorna a quantidade de itens inseridos
    pub fn replace(&mut self, value: SortedValue) -> i64 {
        let mut count = 1;
        if let Some(old_value) = self.map.insert(value.member.clone(), value.clone()) {
            count = 0;
            self.set.remove(&old_value);
        }
        self.set.insert(value.clone());
        return count;
    }

    /// Remove pelo membro
    pub fn remove_by_member(&mut self, member: &String) -> i64 {
        let mut count = 0;
        if let Some(old_value) = self.map.remove(member) {
            count = 1;
            self.set.remove(&old_value);
        }
        return count;
    }

    /// Retorna o lugar no ranking do membro
    pub fn get_rank_by_member(&self, member: &String) -> Option<i64> {
        if let Some(value) = self.map.get(member) {
            // TODO - não escala muito bem mas não vou mexer para não complicar no momento
            return self
                .set
                .iter()
                .position(|item| item == value)
                .map(|f| f as i64);
        }
        None
    }

    pub fn len(&self) -> i64 {
        return self.map.len() as i64;
    }

    pub fn range(&self, start: usize, end: usize) -> Take<Skip<Iter<'_, SortedValue>>> {
        return self.set.iter().skip(start).take(end - start + 1);
    }

    pub fn get_score_by_member(&self, member: &String) -> Option<f64> {
        if let Some(value) = self.map.get(member) {
            return Some(value.score);
        }
        None
    }
}
