CREATE TABLE indexlake_namespace (
    namespace_id BIGINT PRIMARY KEY,
    namespace_name VARCHAR NOT NULL
);

CREATE TABLE indexlake_table (
    table_id BIGINT PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    namespace_id BIGINT NOT NULL
);

CREATE TABLE indexlake_column (
    column_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    column_name VARCHAR NOT NULL,
    column_type VARCHAR NOT NULL,
    default_value VARCHAR,
    nullable BOOLEAN NOT NULL
);

CREATE TABLE indexlake_data_file (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    file_path VARCHAR NOT NULL,
    file_format VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    record_count BIGINT NOT NULL
);

CREATE TABLE indexlake_delete_file (
    delete_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    file_path VARCHAR NOT NULL,
    file_format VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    delete_count BIGINT NOT NULL
);