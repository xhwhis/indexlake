CREATE TABLE indexlake_namespace (
    namespace_id BIGINT PRIMARY KEY,
    namespace_name VARCHAR NOT NULL
);

CREATE TABLE indexlake_table (
    table_id BIGINT PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    namespace_id BIGINT NOT NULL
);

CREATE TABLE indexlake_field (
    field_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    field_name VARCHAR NOT NULL,
    data_type VARCHAR NOT NULL,
    nullable BOOLEAN NOT NULL,
    default_value VARCHAR
);

CREATE TABLE indexlake_data_file (
    data_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    file_path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    record_count BIGINT NOT NULL
);

CREATE TABLE indexlake_delete_file (
    delete_file_id BIGINT PRIMARY KEY,
    table_id BIGINT NOT NULL,
    data_file_id BIGINT NOT NULL,
    file_path VARCHAR NOT NULL,
    file_size_bytes BIGINT NOT NULL,
    delete_count BIGINT NOT NULL
);