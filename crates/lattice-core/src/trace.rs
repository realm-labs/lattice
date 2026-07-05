use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TraceContext {
    #[serde(default)]
    pub traceparent: Option<String>,
    #[serde(default)]
    pub tracestate: Option<String>,
}

impl TraceContext {
    pub fn is_empty(&self) -> bool {
        self.traceparent.is_none() && self.tracestate.is_none()
    }

    pub fn span(&self, name: &'static str, kind: TraceSpanKind) -> tracing::Span {
        tracing::info_span!(
            "lattice.trace_context",
            otel.name = name,
            otel.kind = kind.as_str(),
            traceparent = self.traceparent.as_deref().unwrap_or(""),
            tracestate = self.tracestate.as_deref().unwrap_or("")
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TraceSpanKind {
    Internal,
    Client,
    Server,
    Producer,
    Consumer,
}

impl TraceSpanKind {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Internal => "internal",
            Self::Client => "client",
            Self::Server => "server",
            Self::Producer => "producer",
            Self::Consumer => "consumer",
        }
    }
}
