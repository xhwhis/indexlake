use crate::catalog::Transaction;

pub(crate) struct TransactionHelper {
    transaction: Box<dyn Transaction>,
}

impl TransactionHelper {
    pub(crate) fn new(transaction: Box<dyn Transaction>) -> Self {
        Self { transaction }
    }
}
