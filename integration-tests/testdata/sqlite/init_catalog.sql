CREATE TABLE indexlake_namespace (
    namespace_id BIGINT PRIMARY KEY,
    namespace_name VARCHAR NOT NULL
);

CREATE TABLE indexlake_table (
    table_id BIGINT PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    namespace_id BIGINT NOT NULL,
    config VARCHAR NOT NULL
);

CREATE TABLE indexlake_field (
    field_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    field_name VARCHAR NOT NULL,
    data_type VARCHAR NOT NULL,
    nullable BOOLEAN NOT NULL,
    metadata VARCHAR NOT NULL
);

CREATE TABLE indexlake_dump_task (
    table_id BIGINT PRIMARY KEY
);

CREATE TABLE indexlake_data_file (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    relative_path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    record_count BIGINT NOT NULL,
    row_ids BLOB NOT NULL
);