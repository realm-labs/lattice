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
    selected: bool,
}

impl ScenarioRunner {
    /// Restricts a profile to named scenarios so one long fault injection can be re-run on its own.
    /// An empty selection keeps the whole profile, which is what continuous integration runs.
    pub fn select(names: &[&str], only: &[String]) -> Result<Self, String> {
        if let Some(unknown) = only
            .iter()
            .find(|name| !names.iter().any(|declared| declared == name))
        {
            return Err(format!("{unknown} is not a scenario of this profile"));
        }
        Ok(Self {
            outcomes: names
                .iter()
                .filter(|name| only.is_empty() || only.iter().any(|chosen| chosen == *name))
                .map(|name| ScenarioOutcome {
                    name: (*name).to_owned(),
                    status: ScenarioStatus::NotRun,
                    elapsed_millis: 0,
                    error: None,
                })
                .collect(),
            selected: !only.is_empty(),
        })
    }

    pub fn run(&mut self, name: &str, test: impl FnOnce() -> Result<(), String>) {
        if self.selected && !self.outcomes.iter().any(|outcome| outcome.name == name) {
            return;
        }
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
        let mut runner = ScenarioRunner::select(&["first", "second"], &[]).unwrap();
        runner.run("first", || Err("broken".to_owned()));
        runner.run("second", || Ok(()));

        assert!(runner.finish().is_err());
        assert_eq!(runner.outcomes()[0].status, ScenarioStatus::Failed);
        assert_eq!(runner.outcomes()[1].status, ScenarioStatus::Passed);
    }

    #[test]
    fn a_selection_reports_only_the_named_scenarios() {
        let mut runner = ScenarioRunner::select(&["first", "second"], &["second".to_owned()])
            .expect("second is declared");
        runner.run("first", || Err("must not run".to_owned()));
        runner.run("second", || Ok(()));

        assert!(runner.finish().is_ok());
        assert_eq!(runner.outcomes().len(), 1);
        assert_eq!(runner.outcomes()[0].name, "second");
    }

    #[test]
    fn an_undeclared_selection_is_rejected() {
        assert!(ScenarioRunner::select(&["first"], &["third".to_owned()]).is_err());
    }
}
