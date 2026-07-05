pub type ExampleResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
