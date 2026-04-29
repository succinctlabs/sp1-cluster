-- Emit a pg_notify('proof_event', json_payload) on every proof_requests
-- INSERT or UPDATE. Consumers (bin/api ClusterEventsService, which fans the
-- payload out over a server-streaming gRPC to coordinator/gateway) replace
-- the previous periodic ProofRequestList polling with a push channel.
--
-- The payload is a small JSON blob with just the routing-relevant fields;
-- consumers that need the full row still call ProofRequestGet.

CREATE OR REPLACE FUNCTION proof_request_notify() RETURNS trigger
    LANGUAGE plpgsql AS $$
BEGIN
    PERFORM pg_notify(
        'proof_event',
        json_build_object(
            'proof_id', NEW.id,
            'proof_status', NEW.proof_status,
            'handled', NEW.handled,
            'scheduled_by', NEW.scheduled_by
        )::text
    );
    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS proof_request_notify_trigger ON proof_requests;
CREATE TRIGGER proof_request_notify_trigger
AFTER INSERT OR UPDATE ON proof_requests
FOR EACH ROW EXECUTE FUNCTION proof_request_notify();
