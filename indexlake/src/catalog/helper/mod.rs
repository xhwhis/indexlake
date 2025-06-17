mod create;
mod delete;
mod insert;
mod query;
mod update;

use crate::{ILResult, catalog::Transaction};

pub(crate) struct TransactionHelper {
    transaction: Box<dyn Transaction>,
}

impl TransactionHelper {
    pub(crate) fn new(transaction: Box<dyn Transaction>) -> Self {
        Self { transaction }
    }

    pub(crate) async fn commit(&mut self) -> ILResult<()> {
        self.transaction.commit().await
    }

    pub(crate) async fn rollback(&mut self) -> ILResult<()> {
        self.transaction.rollback().await
    }
}
