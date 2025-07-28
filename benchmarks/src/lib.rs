pub mod data;

pub fn setup_s3_env() {
    unsafe {
        std::env::set_var("AWS_ACCESS_KEY_ID", "admin");
        std::env::set_var("AWS_SECRET_ACCESS_KEY", "password");
        std::env::set_var("AWS_ENDPOINT", "http://127.0.0.1:9000");
        std::env::set_var("AWS_REGION", "us-east-1");
        std::env::set_var("AWS_BUCKET", "indexlake");
        std::env::set_var("AWS_EC2_METADATA_DISABLED", "true");
        std::env::set_var("AWS_ALLOW_HTTP", "true");
    }
}

pub fn set_lance_language_model_home() {
    unsafe {
        std::env::set_var(
            "LANCE_LANGUAGE_MODEL_HOME",
            format!(
                "{}/testdata/lance/language_models",
                std::env::var("CARGO_MANIFEST_DIR").unwrap()
            ),
        );
    }
}
