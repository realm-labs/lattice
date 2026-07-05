use opentelemetry_sdk::trace::SdkTracerProvider;

#[derive(Debug, Default)]
pub struct TelemetryGuard {
    pub(crate) tracer_provider: Option<SdkTracerProvider>,
}

impl Drop for TelemetryGuard {
    fn drop(&mut self) {
        if let Some(provider) = self.tracer_provider.take() {
            let _ = provider.shutdown();
        }
    }
}
