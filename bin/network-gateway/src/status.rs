use sp1_cluster_common::proto as cluster_pb;
use sp1_sdk::network::proto::base::types as sdk_pb;

/// Map cluster `ProofRequestStatus` → SDK `FulfillmentStatus`.
pub fn fulfillment_from_cluster(
    status: cluster_pb::ProofRequestStatus,
) -> sdk_pb::FulfillmentStatus {
    use cluster_pb::ProofRequestStatus as C;
    use sdk_pb::FulfillmentStatus as S;
    match status {
        C::Unspecified => S::UnspecifiedFulfillmentStatus,
        C::Pending => S::Requested,
        C::Completed => S::Fulfilled,
        C::Failed | C::Cancelled => S::Unfulfillable,
    }
}

/// Map cluster `ExecutionStatus` → SDK `ExecutionStatus`.
///
/// Any cluster failure or cancellation maps to `Unexecutable`, which the SDK's
/// `process_proof_status` treats as a terminal error and bails out of `wait_proof`.
pub fn execution_from_cluster(status: cluster_pb::ExecutionStatus) -> sdk_pb::ExecutionStatus {
    use cluster_pb::ExecutionStatus as C;
    use sdk_pb::ExecutionStatus as S;
    match status {
        C::Unspecified => S::UnspecifiedExecutionStatus,
        C::Unexecuted => S::Unexecuted,
        C::Executed => S::Executed,
        C::Failed | C::Cancelled => S::Unexecutable,
    }
}

/// Map an SDK `FulfillmentStatus` filter to the cluster states it covers.
///
/// `None` means no filter. `Some(vec![])` means the filter matches nothing:
/// `Assigned`, `Reverted` and `Expired` are on-chain settlement states with no
/// cluster analogue — the cluster only tracks Pending/Completed/Failed/Cancelled
/// — and [`fulfillment_from_cluster`] can never produce them, so a caller
/// filtering by one must get an empty result. The two cannot share an encoding:
/// the cluster API treats an empty status list as "no filter", which would turn
/// "matches nothing" into "matches everything".
pub fn cluster_fulfillment_filter(
    status: sdk_pb::FulfillmentStatus,
) -> Option<Vec<cluster_pb::ProofRequestStatus>> {
    use cluster_pb::ProofRequestStatus as C;
    use sdk_pb::FulfillmentStatus as S;
    match status {
        S::UnspecifiedFulfillmentStatus => None,
        S::Requested => Some(vec![C::Pending]),
        S::Assigned | S::Reverted | S::Expired => Some(vec![]),
        S::Fulfilled => Some(vec![C::Completed]),
        S::Unfulfillable => Some(vec![C::Failed, C::Cancelled]),
    }
}

/// Map an SDK `ExecutionStatus` filter to the cluster states it covers.
pub fn cluster_execution_filter(
    status: sdk_pb::ExecutionStatus,
) -> Vec<cluster_pb::ExecutionStatus> {
    use cluster_pb::ExecutionStatus as C;
    use sdk_pb::ExecutionStatus as S;
    match status {
        S::UnspecifiedExecutionStatus => vec![],
        S::Unexecuted => vec![C::Unexecuted],
        S::Executed => vec![C::Executed],
        S::Unexecutable => vec![C::Failed, C::Cancelled],
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fulfillment_mapping_exhaustive() {
        use cluster_pb::ProofRequestStatus as C;
        use sdk_pb::FulfillmentStatus as S;
        assert_eq!(
            fulfillment_from_cluster(C::Unspecified),
            S::UnspecifiedFulfillmentStatus
        );
        assert_eq!(fulfillment_from_cluster(C::Pending), S::Requested);
        assert_eq!(fulfillment_from_cluster(C::Completed), S::Fulfilled);
        assert_eq!(fulfillment_from_cluster(C::Failed), S::Unfulfillable);
        assert_eq!(fulfillment_from_cluster(C::Cancelled), S::Unfulfillable);
    }

    #[test]
    fn execution_mapping_exhaustive() {
        use cluster_pb::ExecutionStatus as C;
        use sdk_pb::ExecutionStatus as S;
        assert_eq!(
            execution_from_cluster(C::Unspecified),
            S::UnspecifiedExecutionStatus
        );
        assert_eq!(execution_from_cluster(C::Unexecuted), S::Unexecuted);
        assert_eq!(execution_from_cluster(C::Executed), S::Executed);
        assert_eq!(execution_from_cluster(C::Failed), S::Unexecutable);
        assert_eq!(execution_from_cluster(C::Cancelled), S::Unexecutable);
    }

    #[test]
    fn cluster_fulfillment_filter_maps_correctly() {
        use cluster_pb::ProofRequestStatus as C;
        use sdk_pb::FulfillmentStatus as S;
        assert_eq!(
            cluster_fulfillment_filter(S::UnspecifiedFulfillmentStatus),
            None
        );
        assert_eq!(
            cluster_fulfillment_filter(S::Requested),
            Some(vec![C::Pending])
        );
        // Settlement states the cluster cannot produce: a filter that matches
        // nothing, NOT "no filter".
        assert_eq!(cluster_fulfillment_filter(S::Assigned), Some(vec![]));
        assert_eq!(cluster_fulfillment_filter(S::Reverted), Some(vec![]));
        assert_eq!(cluster_fulfillment_filter(S::Expired), Some(vec![]));
        assert_eq!(
            cluster_fulfillment_filter(S::Fulfilled),
            Some(vec![C::Completed])
        );
        assert_eq!(
            cluster_fulfillment_filter(S::Unfulfillable),
            Some(vec![C::Failed, C::Cancelled])
        );
    }

    #[test]
    fn cluster_execution_filter_maps_correctly() {
        use cluster_pb::ExecutionStatus as C;
        use sdk_pb::ExecutionStatus as S;
        assert!(cluster_execution_filter(S::UnspecifiedExecutionStatus).is_empty());
        assert_eq!(cluster_execution_filter(S::Unexecuted), vec![C::Unexecuted]);
        assert_eq!(cluster_execution_filter(S::Executed), vec![C::Executed]);
        assert_eq!(
            cluster_execution_filter(S::Unexecutable),
            vec![C::Failed, C::Cancelled]
        );
    }
}
