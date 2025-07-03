use crate::{
    ILError, ILResult,
    catalog::{
        DataFileRecord, INTERNAL_ROW_ID_FIELD_NAME, IndexFileRecord, IndexRecord,
        RowMetadataRecord, TableRecord, TransactionHelper,
    },
};
use arrow::datatypes::Fields;

impl TransactionHelper {
    pub(crate) async fn insert_namespace(
        &mut self,
        namespace_id: i64,
        namespace_name: &str,
    ) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_namespace (namespace_id, namespace_name) VALUES ({namespace_id}, '{namespace_name}')"
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_table(&mut self, table_record: &TableRecord) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_table ({}) VALUES {}",
                TableRecord::select_items().join(", "),
                table_record.to_sql()?
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_fields(
        &mut self,
        table_id: i64,
        field_ids: &[i64],
        fields: &Fields,
    ) -> ILResult<()> {
        let mut values = Vec::new();
        // TODO save dict_is_ordered
        for (field_id, field) in field_ids.iter().zip(fields.iter()) {
            values.push(format!(
                "({field_id}, {table_id}, '{}', '{}', {}, '{}')",
                field.name(),
                field.data_type().to_string(),
                field.is_nullable(),
                serde_json::to_string(&field.metadata()).map_err(|e| ILError::InternalError(
                    format!("Failed to serialize field metadata: {e:?}")
                ))?,
            ));
        }
        self.transaction
            .execute(&format!(
                "
            INSERT INTO indexlake_field 
            (field_id, table_id, field_name, data_type, nullable, metadata) 
            VALUES {}",
                values.join(", ")
            ))
            .await?;
        Ok(())
    }

    pub(crate) async fn insert_inline_rows(
        &mut self,
        table_id: i64,
        field_names: &[String],
        values: Vec<String>,
    ) -> ILResult<()> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_inline_row_{table_id} ({}) VALUES {}",
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

    pub(crate) async fn insert_row_metadatas(
        &mut self,
        table_id: i64,
        metadatas: &[RowMetadataRecord],
    ) -> ILResult<()> {
        let values = metadatas.iter().map(|m| m.to_sql()).collect::<Vec<_>>();
        self.transaction.execute(&format!("INSERT INTO indexlake_row_metadata_{table_id} ({INTERNAL_ROW_ID_FIELD_NAME}, location, deleted) VALUES {}", values.join(", "))).await?;
        Ok(())
    }

    pub(crate) async fn insert_dump_task(&mut self, table_id: i64) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_dump_task (table_id) VALUES ({table_id})"
            ))
            .await
    }

    pub(crate) async fn insert_data_files(
        &mut self,
        data_files: &[DataFileRecord],
    ) -> ILResult<usize> {
        let values = data_files
            .iter()
            .map(|r| r.to_sql(self.database))
            .collect::<Vec<_>>();
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_data_file ({}) VALUES {}",
                DataFileRecord::select_items().join(", "),
                values.join(", ")
            ))
            .await
    }

    pub(crate) async fn insert_index(&mut self, index_record: &IndexRecord) -> ILResult<usize> {
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_index ({}) VALUES {}",
                IndexRecord::select_items().join(", "),
                index_record.to_sql()
            ))
            .await
    }

    pub(crate) async fn insert_index_files(
        &mut self,
        index_files: &[IndexFileRecord],
    ) -> ILResult<usize> {
        let values = index_files.iter().map(|r| r.to_sql()).collect::<Vec<_>>();
        self.transaction
            .execute(&format!(
                "INSERT INTO indexlake_index_file ({}) VALUES {}",
                IndexFileRecord::select_items().join(", "),
                values.join(", ")
            ))
            .await
    }
}
