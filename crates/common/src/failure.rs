use serde::{Deserialize, Serialize};

use crate::proto::TaskType;

/// `extra_data` payload for post-execute task failures.
/// On the wire as `{"proving_failure": <ProvingFailure>}` to coexist with
/// the `ExecutionResult` shape written by execute_only.
#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct ProvingFailure {
    pub task_type: TaskType,
    pub reason: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub program_error: Option<ProgramError>,
}

/// A program the worker refused, which fails the same way on every retry.
#[derive(Serialize, Deserialize, Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProgramError {
    TooLarge,
    Invalid,
}

impl ProvingFailure {
    /// Render as the `extra_data` wire string.
    pub fn to_extra_data(&self) -> String {
        serde_json::json!({ "proving_failure": self }).to_string()
    }

    /// Parse from the `extra_data` wire string. Returns `None` if the string
    /// isn't a proving-failure payload.
    pub fn from_extra_data(s: &str) -> Option<Self> {
        #[derive(Deserialize)]
        struct Envelope {
            proving_failure: ProvingFailure,
        }
        serde_json::from_str::<Envelope>(s)
            .ok()
            .map(|e| e.proving_failure)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn program_error_round_trips_through_extra_data() {
        let failure = ProvingFailure {
            task_type: TaskType::ExecuteOnly,
            reason: "Program exceeds 536870912 bytes".into(),
            program_error: Some(ProgramError::TooLarge),
        };
        let parsed = ProvingFailure::from_extra_data(&failure.to_extra_data()).unwrap();
        assert_eq!(parsed.program_error, Some(ProgramError::TooLarge));
    }

    #[test]
    fn extra_data_without_program_error_still_parses() {
        let extra = r#"{"proving_failure":{"task_type":"ProveShard","reason":"GPU OOM"}}"#;
        let parsed = ProvingFailure::from_extra_data(extra).unwrap();
        assert_eq!(parsed.program_error, None);
    }
}
