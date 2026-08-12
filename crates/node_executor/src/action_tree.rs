//! Keep nested Node actions on the same side of a multi-pool source or
//! environment change.
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        Mutex,
        OnceLock,
    },
};

use common::{
    execution_context::ExecutionId,
    types::{
        EnvVarName,
        EnvVarValue,
        Timestamp,
    },
};
use tokio::sync::{
    watch,
    OwnedRwLockReadGuard,
    OwnedRwLockWriteGuard,
    RwLock,
};

type Variables = BTreeMap<EnvVarName, EnvVarValue>;
pub(crate) struct ActiveEnvironment {
    variables: Variables,
    version: Timestamp,
    // Every waiter for this environment wakes when an exclusive owner removes it.
    retired: watch::Sender<()>,
}

type TreePermit = Arc<OwnedRwLockReadGuard<OnceLock<ActiveEnvironment>>>;
pub(crate) type TreeCutoverGuard = OwnedRwLockWriteGuard<OnceLock<ActiveEnvironment>>;

pub enum NodeEnvironmentAdmission {
    Ready {
        variables: Variables,
        version: Timestamp,
    },
    Changed(watch::Receiver<()>),
}

#[derive(Default)]
pub struct NodeActionTreeAdmission {
    fence: Arc<RwLock<OnceLock<ActiveEnvironment>>>,
    executions: Mutex<BTreeMap<ExecutionId, TreePermit>>,
}

pub struct NodeActionTreeLease<'a> {
    admission: &'a NodeActionTreeAdmission,
    execution: ExecutionId,
    permit: TreePermit,
    inherited: bool,
}

impl NodeActionTreeAdmission {
    pub fn try_enter(
        &self,
        execution: ExecutionId,
        parent: Option<ExecutionId>,
    ) -> Option<NodeActionTreeLease<'_>> {
        let mut executions = self
            .executions
            .lock()
            .expect("Node action admission poisoned");
        let inherited = parent.and_then(|id| executions.get(&id).cloned());
        let is_descendant = inherited.is_some();
        // A writer waiting for the parent must not queue its child behind itself.
        let permit =
            inherited.or_else(|| self.fence.clone().try_read_owned().ok().map(Arc::new))?;
        assert!(executions.insert(execution, permit.clone()).is_none());
        Some(NodeActionTreeLease {
            admission: self,
            execution,
            permit,
            inherited: is_descendant,
        })
    }

    pub async fn enter(&self, execution: ExecutionId) -> NodeActionTreeLease<'_> {
        let permit = Arc::new(self.fence.clone().read_owned().await);
        assert!(self
            .executions
            .lock()
            .expect("Node action admission poisoned")
            .insert(execution, permit.clone())
            .is_none());
        NodeActionTreeLease {
            admission: self,
            execution,
            permit,
            inherited: false,
        }
    }

    pub(crate) async fn drain(&self) -> TreeCutoverGuard {
        let mut guard = self.fence.clone().write_owned().await;
        guard.take();
        guard
    }

    pub async fn refresh_environment(&self, mut retired: watch::Receiver<()>) {
        // Concurrent observers share the first drain. A delayed waiter must not
        // clear values already selected from a newer snapshot.
        tokio::select! {
            biased;
            _ = retired.changed() => {},
            mut guard = self.fence.write() => {
                if retired.has_changed().is_ok() {
                    guard.take();
                }
            },
        }
    }
}

impl NodeActionTreeLease<'_> {
    pub fn select_environment(
        &self,
        observed: Variables,
        version: Timestamp,
    ) -> NodeEnvironmentAdmission {
        let current = self.permit.get_or_init(|| ActiveEnvironment {
            variables: observed.clone(),
            version,
            retired: watch::Sender::new(()),
        });
        if current.variables == observed {
            // A later transaction with the same source and values must not move the
            // router watermark past a matching request still acquiring a cold pool.
            NodeEnvironmentAdmission::Ready {
                variables: observed,
                version: current.version,
            }
        } else if self.inherited {
            // A caller holding an old generation must be able to run its child
            // before a replacement can drain that generation.
            NodeEnvironmentAdmission::Ready {
                variables: current.variables.clone(),
                version: current.version,
            }
        } else {
            NodeEnvironmentAdmission::Changed(current.retired.subscribe())
        }
    }

    pub fn inherited(&self) -> bool {
        self.inherited
    }
}

impl Drop for NodeActionTreeLease<'_> {
    fn drop(&mut self) {
        assert!(self
            .admission
            .executions
            .lock()
            .expect("Node action admission poisoned")
            .remove(&self.execution)
            .is_some());
    }
}

#[cfg(test)]
mod tests {
    use futures::poll;

    use super::*;

    #[tokio::test]
    async fn environment_edit_drains_old_snapshots_and_preserves_nested_calls() {
        let environment =
            |value: &str| BTreeMap::from([("SETTING".parse().unwrap(), value.parse().unwrap())]);
        let ready = |lease: &NodeActionTreeLease<'_>, observed, ts: u64| {
            let NodeEnvironmentAdmission::Ready { variables, version } =
                lease.select_environment(observed, Timestamp::try_from(ts).unwrap())
            else {
                panic!("matching root or descendant unexpectedly waited");
            };
            (variables, version)
        };
        let old = environment("old");
        let edit = environment("edit");
        let gate = NodeActionTreeAdmission::default();
        let parent_id = ExecutionId::new();
        let parent = gate.try_enter(parent_id, None).unwrap();
        let old_env = (old.clone(), Timestamp::try_from(1_u64).unwrap());
        assert_eq!(ready(&parent, old.clone(), 1), old_env);
        let matching = gate.try_enter(ExecutionId::new(), None).unwrap();
        assert_eq!(ready(&matching, old.clone(), 2), old_env);
        drop(matching);
        // A root that already opened an old snapshot may still be resolving its
        // runtime.
        let provisional = gate.try_enter(ExecutionId::new(), None).unwrap();
        let changed = || {
            let root = gate.try_enter(ExecutionId::new(), None).unwrap();
            let NodeEnvironmentAdmission::Changed(retired) =
                root.select_environment(edit.clone(), Timestamp::try_from(3_u64).unwrap())
            else {
                panic!("root mixed old and new environments");
            };
            retired
        };
        let mut refresh = Box::pin(gate.refresh_environment(changed()));
        let delayed_observer = changed();
        assert!(poll!(&mut refresh).is_pending());
        assert!(gate.try_enter(ExecutionId::new(), None).is_none());

        // An intermediate isolate does not select a Node environment. Its own child
        // still inherits the old values, even after the original parent exits.
        let isolate_id = ExecutionId::new();
        let isolate = gate.try_enter(isolate_id, Some(parent_id)).unwrap();
        drop(parent);
        let child = gate
            .try_enter(ExecutionId::new(), Some(isolate_id))
            .unwrap();
        drop(isolate);
        assert_eq!(ready(&child, edit, 4), old_env);
        drop(child);
        assert!(poll!(&mut refresh).is_pending());
        drop(provisional);
        refresh.await;

        // Select from the retry's fresh snapshot, including a second edit. An older
        // observer arriving late must not drain this new root.
        let latest = environment("latest");
        let next = gate.try_enter(ExecutionId::new(), None).unwrap();
        assert_eq!(
            ready(&next, latest.clone(), 5),
            (latest, Timestamp::try_from(5_u64).unwrap())
        );
        assert!(poll!(Box::pin(gate.refresh_environment(delayed_observer))).is_ready());
    }

    #[tokio::test]
    async fn queued_cutover_admits_descendants_before_new_roots() {
        let gate = NodeActionTreeAdmission::default();
        let parent_id = ExecutionId::new();
        let parent = gate.try_enter(parent_id, None).unwrap();
        let mut writer = Box::pin(gate.drain());
        assert!(poll!(&mut writer).is_pending());
        let new_id = ExecutionId::new();
        assert!(gate.try_enter(new_id, None).is_none());
        let mut new_root = Box::pin(gate.enter(new_id));
        assert!(poll!(&mut new_root).is_pending());
        let child_id = ExecutionId::new();
        let child = gate.try_enter(child_id, Some(parent_id)).unwrap();
        drop(parent);
        let grandchild = gate.try_enter(ExecutionId::new(), Some(child_id)).unwrap();
        drop(child);
        assert!(poll!(&mut writer).is_pending());
        drop(grandchild);
        let publication = writer.await;
        assert!(poll!(&mut new_root).is_pending());
        drop(publication);
        assert!(!new_root.await.inherited());
    }
}
