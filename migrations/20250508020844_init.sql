-- Proof requests table
CREATE TABLE IF NOT EXISTS proof_requests (
    id TEXT PRIMARY KEY,
    proof_status INT NOT NULL,
    requester BYTEA NOT NULL,
    execution_status INT,
    execution_failure_cause INT,
    execution_cycles BIGINT,
    execution_gas BIGINT,
    execution_public_values_hash BYTEA,
    stdin_artifact_id TEXT NOT NULL,
    program_artifact_id TEXT NOT NULL,
    proof_artifact_id TEXT,
    options_artifact_id TEXT,
    cycle_limit BIGINT,
    gas_limit BIGINT,
    deadline BIGINT NOT NULL,
    handled BOOLEAN NOT NULL,
    metadata TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Indexes for efficient querying
CREATE INDEX idx_proof_requests_list ON proof_requests(handled, deadline, proof_status);
