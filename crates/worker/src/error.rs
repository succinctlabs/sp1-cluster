use anyhow::anyhow;
use sp1_core_executor::ExecutionError;
use sp1_core_machine::utils::SP1CoreProverError;
use sp1_prover::SP1RecursionProverError;
use tonic::Code;

#[derive(Debug)]
pub enum TaskError {
    Retryable(anyhow::Error),
    Fatal(anyhow::Error),
    Execution(ExecutionError),
}

impl From<anyhow::Error> for TaskError {
    fn from(err: anyhow::Error) -> Self {
        if err.is::<reqwest::Error>() {
            Self::Retryable(err)
        } else if err.is::<tonic::Status>() {
            err.downcast::<tonic::Status>().unwrap().into()
        } else {
            Self::Fatal(err)
        }
    }
}

impl From<tonic::Status> for TaskError {
    fn from(err: tonic::Status) -> Self {
        match err.code() {
            Code::Internal
            | Code::Unavailable
            | Code::Unknown
            | Code::Cancelled
            | Code::DeadlineExceeded
            | Code::ResourceExhausted
            | Code::Aborted
            | Code::DataLoss => Self::Retryable(err.into()),
            _ => Self::Fatal(err.into()),
        }
    }
}

impl From<SP1CoreProverError> for TaskError {
    fn from(err: SP1CoreProverError) -> Self {
        Self::Fatal(err.into())
    }
}

impl From<SP1RecursionProverError> for TaskError {
    fn from(err: SP1RecursionProverError) -> Self {
        Self::Fatal(err.into())
    }
}

impl From<ExecutionError> for TaskError {
    fn from(err: ExecutionError) -> Self {
        Self::Execution(err)
    }
}

impl From<eyre::Report> for TaskError {
    fn from(err: eyre::Report) -> Self {
        Self::Fatal(anyhow!(Box::new(err)))
    }
}

impl std::error::Error for TaskError {
    fn description(&self) -> &str {
        match self {
            TaskError::Retryable(_) => "Retryable",
            TaskError::Fatal(_) => "Fatal",
            TaskError::Execution(_) => "Execution",
        }
    }
}

impl std::fmt::Display for TaskError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            TaskError::Retryable(err) => write!(f, "Retryable: {}", err),
            TaskError::Fatal(err) => write!(f, "Fatal: {}", err),
            TaskError::Execution(err) => write!(f, "Execution: {}", err),
        }
    }
}
