use std::panic::{AssertUnwindSafe, catch_unwind};
use std::time::Instant;

use serde::Serialize;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "kebab-case")]
pub(super) enum ScenarioStatus {
    Passed,
    Failed,
    NotRun,
}

#[derive(Debug, Clone, Serialize)]
pub(super) struct ScenarioOutcome {
    pub name: String,
    pub status: ScenarioStatus,
    pub elapsed_millis: u128,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub(super) struct ScenarioRunner {
    outcomes: Vec<ScenarioOutcome>,
}

impl ScenarioRunner {
    pub fn new(names: &[&str]) -> Self {
        Self {
            outcomes: names
                .iter()
                .map(|name| ScenarioOutcome {
                    name: (*name).to_owned(),
                    status: ScenarioStatus::NotRun,
                    elapsed_millis: 0,
                    error: None,
                })
                .collect(),
        }
    }

    pub fn run(&mut self, name: &str, test: impl FnOnce() -> Result<(), String>) {
        let started = Instant::now();
        let result = catch_unwind(AssertUnwindSafe(test)).unwrap_or_else(|panic| {
            let message = panic
                .downcast_ref::<&str>()
                .map(|message| (*message).to_owned())
                .or_else(|| panic.downcast_ref::<String>().cloned())
                .unwrap_or_else(|| "scenario panicked without a message".to_owned());
            Err(message)
        });
        let Some(outcome) = self
            .outcomes
            .iter_mut()
            .find(|outcome| outcome.name == name)
        else {
            panic!("scenario {name} is missing from the profile declaration");
        };
        outcome.elapsed_millis = started.elapsed().as_millis();
        match result {
            Ok(()) => outcome.status = ScenarioStatus::Passed,
            Err(error) => {
                outcome.status = ScenarioStatus::Failed;
                outcome.error = Some(error);
            }
        }
    }

    pub fn outcomes(&self) -> &[ScenarioOutcome] {
        &self.outcomes
    }

    pub fn finish(&self) -> Result<(), String> {
        let failures = self
            .outcomes
            .iter()
            .filter(|outcome| outcome.status != ScenarioStatus::Passed)
            .map(|outcome| match &outcome.error {
                Some(error) => format!("{}: {error}", outcome.name),
                None => format!("{}: not run", outcome.name),
            })
            .collect::<Vec<_>>();
        if failures.is_empty() {
            Ok(())
        } else {
            Err(format!("scenario failures: {}", failures.join("; ")))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ScenarioRunner, ScenarioStatus};

    #[test]
    fn records_all_results_after_a_failure() {
        let mut runner = ScenarioRunner::new(&["first", "second"]);
        runner.run("first", || Err("broken".to_owned()));
        runner.run("second", || Ok(()));

        assert!(runner.finish().is_err());
        assert_eq!(runner.outcomes()[0].status, ScenarioStatus::Failed);
        assert_eq!(runner.outcomes()[1].status, ScenarioStatus::Passed);
    }
}
