-- Supports proof_request_list's ORDER BY (created_at, id) pagination.
CREATE INDEX idx_proof_requests_created_at_id ON proof_requests (created_at, id);
