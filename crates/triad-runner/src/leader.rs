use std::sync::Arc;

use async_trait::async_trait;
use triad_core::{
    error::ElectionError,
    traits::{LeaderElector, LeaderHandle},
};

/// No-op leader — always reports itself as leader.
///
/// Used for Mode 1 (embedded) and Mode 2 (standalone single-instance).
/// Calling `campaign()` immediately grants the handle; `is_leader()` always
/// returns `true`.
pub struct NoopLeader;

#[async_trait]
impl LeaderElector for NoopLeader {
    async fn campaign(&self) -> Result<LeaderHandle, ElectionError> {
        let sem = Arc::new(tokio::sync::Semaphore::new(1));
        let permit = Arc::clone(&sem)
            .acquire_owned()
            .await
            .expect("semaphore acquire failed");
        Ok(LeaderHandle::new(permit))
    }

    fn is_leader(&self) -> bool {
        true
    }
}

/// K8s Lease-based leader election for Mode 3 (multi-replica Kubernetes deployment).
///
/// Acquires a `coordination.k8s.io/v1 Lease`; renews every
/// `lease_duration_s / 3` seconds (≈ 5s with the default 15s lease).
/// Dropping the returned `LeaderHandle` stops renewal and releases the lease.
///
/// This type is only compiled when the `kubernetes` feature is enabled.
#[cfg(feature = "kubernetes")]
pub mod k8s {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    use async_trait::async_trait;
    use k8s_openapi::api::coordination::v1::{Lease, LeaseSpec};
    use k8s_openapi::apimachinery::pkg::apis::meta::v1::ObjectMeta;
    use kube::{
        Client,
        api::{Api, Patch, PatchParams},
    };
    use tracing::{error, info};
    use triad_core::{
        error::ElectionError,
        traits::{LeaderElector, LeaderHandle},
    };

    const LEASE_DURATION_S: i32 = 15;
    const RENEW_INTERVAL_S: u64 = 5;

    pub struct K8sLeaseLeader {
        client: Client,
        namespace: String,
        lease_name: String,
        pod_name: String,
        is_leader: Arc<AtomicBool>,
    }

    impl K8sLeaseLeader {
        pub fn new(
            client: Client,
            namespace: String,
            lease_name: String,
            pod_name: String,
        ) -> Self {
            Self {
                client,
                namespace,
                lease_name,
                pod_name,
                is_leader: Arc::new(AtomicBool::new(false)),
            }
        }
    }

    #[async_trait]
    impl LeaderElector for K8sLeaseLeader {
        async fn campaign(&self) -> Result<LeaderHandle, ElectionError> {
            let api: Api<Lease> = Api::namespaced(self.client.clone(), &self.namespace);
            let lease_name = self.lease_name.clone();
            let pod_name = self.pod_name.clone();
            let is_leader_flag = Arc::clone(&self.is_leader);

            // Attempt to create or acquire the lease.
            let lease = Lease {
                metadata: ObjectMeta {
                    name: Some(lease_name.clone()),
                    namespace: Some(self.namespace.clone()),
                    ..Default::default()
                },
                spec: Some(LeaseSpec {
                    holder_identity: Some(pod_name.clone()),
                    lease_duration_seconds: Some(LEASE_DURATION_S),
                    acquire_time: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime(
                        chrono::Utc::now(),
                    )),
                    renew_time: Some(k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime(
                        chrono::Utc::now(),
                    )),
                    ..Default::default()
                }),
            };

            api.patch(
                &lease_name,
                &PatchParams::apply("triad-runner").force(),
                &Patch::Apply(&lease),
            )
            .await
            .map_err(|e| ElectionError::K8sApi(e.to_string()))?;

            is_leader_flag.store(true, Ordering::Release);
            info!(lease = %lease_name, pod = %pod_name, "acquired K8s lease");

            // Spawn a renewal task. The OwnedSemaphorePermit drop stops it.
            let sem = Arc::new(tokio::sync::Semaphore::new(1));
            let permit = Arc::clone(&sem)
                .acquire_owned()
                .await
                .expect("semaphore acquire failed");

            let api2 = api.clone();
            let lease_name2 = lease_name.clone();
            let pod_name2 = pod_name.clone();
            let flag2 = Arc::clone(&is_leader_flag);
            let weak_sem = Arc::downgrade(&sem);

            tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_secs(RENEW_INTERVAL_S));
                loop {
                    interval.tick().await;
                    // Stop renewal if the permit holder has dropped the semaphore.
                    if weak_sem.upgrade().is_none() {
                        break;
                    }
                    let renewed = Lease {
                        metadata: ObjectMeta {
                            name: Some(lease_name2.clone()),
                            ..Default::default()
                        },
                        spec: Some(LeaseSpec {
                            holder_identity: Some(pod_name2.clone()),
                            lease_duration_seconds: Some(LEASE_DURATION_S),
                            renew_time: Some(
                                k8s_openapi::apimachinery::pkg::apis::meta::v1::MicroTime(
                                    chrono::Utc::now(),
                                ),
                            ),
                            ..Default::default()
                        }),
                    };
                    if let Err(e) = api2
                        .patch(
                            &lease_name2,
                            &PatchParams::apply("triad-runner"),
                            &Patch::Apply(&renewed),
                        )
                        .await
                    {
                        error!(lease = %lease_name2, error = %e, "lease renewal failed");
                        flag2.store(false, Ordering::Release);
                        break;
                    }
                }
            });

            Ok(LeaderHandle::new(permit))
        }

        fn is_leader(&self) -> bool {
            self.is_leader.load(Ordering::Acquire)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_noop_leader_is_always_leader() {
        let leader = NoopLeader;
        assert!(leader.is_leader());
    }

    #[tokio::test]
    async fn test_noop_leader_campaign_succeeds() {
        let leader = NoopLeader;
        let handle = leader.campaign().await;
        assert!(handle.is_ok());
    }

    #[tokio::test]
    async fn test_noop_leader_multiple_campaigns() {
        let leader = NoopLeader;
        // Each campaign grants a fresh semaphore — no blocking.
        let h1 = leader.campaign().await.unwrap();
        let h2 = leader.campaign().await.unwrap();
        drop(h1);
        drop(h2);
    }

    #[test]
    fn test_noop_leader_is_leader_is_true() {
        assert!(NoopLeader.is_leader());
    }
}
