use uuid::Uuid;

use crate::{
    ILError, ILResult,
    catalog::{
        DataFileRecord, FieldRecord, IndexFileRecord, IndexRecord, TableRecord, TransactionHelper,
        inline_row_table_name,
    },
};

impl TransactionHelper {
    pub(crate) async fn insert_namespace(
        &mut self,
        namespace_id: &Uuid,
        namespace_name: &str,
    ) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_namespace (namespace_id, namespace_name) VALUES ({}, '{namespace_name}')",
                self.database.sql_uuid_value(namespace_id)
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_table(&mut self, table_record: &TableRecord) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_table ({}) VALUES {}",
                TableRecord::catalog_schema()
                    .select_items(self.database)
                    .join(", "),
                table_record.to_sql(self.database)?
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_fields(&mut self, fields: &[FieldRecord]) -> ILResult<()> {
        if fields.is_empty() {
            return Ok(());
        }
        let mut values = Vec::new();
        for record in fields {
            values.push(record.to_sql(self.database)?);
        }
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_field ({}) VALUES {}",
                FieldRecord::catalog_schema()
                    .select_items(self.database)
                    .join(", "),
                values.join(", ")
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_inline_rows(
        &mut self,
        table_id: &Uuid,
        field_names: &[String],
        values: Vec<String>,
    ) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "INSERT INTO {} ({}) VALUES {}",
                inline_row_table_name(table_id),
                field_names
                    .iter()
                    .map(|name| self.database.sql_identifier(name))
                    .collect::<Vec<_>>()
                    .join(", "),
                values.join(", ")
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_dump_task(&mut self, table_id: &Uuid) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_dump_task (table_id) VALUES ({})",
                self.database.sql_uuid_value(table_id)
            ))
            .await
    }

    pub(crate) async fn insert_data_files(
        &mut self,
        data_files: &[DataFileRecord],
    ) -> ILResult<usize> {
        if data_files.is_empty() {
            return Ok(0);
        }
        let values = data_files
            .iter()
            .map(|r| r.to_sql(self.database))
            .collect::<Vec<_>>();
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_data_file ({}) VALUES {}",
                DataFileRecord::catalog_schema()
                    .select_items(self.database)
                    .join(", "),
                values.join(", ")
            ))
            .await
    }

    pub(crate) async fn insert_index(&mut self, index_record: &IndexRecord) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_index ({}) VALUES {}",
                IndexRecord::catalog_schema()
                    .select_items(self.database)
                    .join(", "),
                index_record.to_sql(self.database)
            ))
            .await
    }

    pub(crate) async fn insert_index_files(
        &mut self,
        index_files: &[IndexFileRecord],
    ) -> ILResult<usize> {
        if index_files.is_empty() {
            return Ok(0);
        }
        let values = index_files
            .iter()
            .map(|r| r.to_sql(self.database))
            .collect::<Vec<_>>();
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_index_file ({}) VALUES {}",
                IndexFileRecord::catalog_schema()
                    .select_items(self.database)
                    .join(", "),
                values.join(", ")
            ))
            .await
    }
}
