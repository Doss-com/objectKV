//! Shared object-history composition harness for the Tetris and Chess evals.
//!
//! This is not a product storage API. It composes the real object client and
//! pure publication authority so both applications exercise identical prepare,
//! immutable PUT, verified read, publish, pin, root removal, and GC semantics.

use bytes::Bytes;
use okv_app_history::HistoryObjectRef;
use okv_object::{memory_backend, Backend, ObjectClient, ObjectIdentity};
use okv_publication::{
    AuthorityContext, AuthorityPosition, ObjectIdentity as PublicationObjectIdentity, ObjectKind,
    ObjectReference, PublicationAction, PublicationAuthorityFaults, PublicationAuthorityState,
    PublicationCommandStatus, PublicationIntent, PublicationOutcome, RevisionToken,
};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct ObjectBlob {
    pub reference: HistoryObjectRef,
    pub kind: ObjectKind,
    pub bytes: Vec<u8>,
}

impl ObjectBlob {
    #[must_use]
    pub fn content_addressed(
        namespace: &str,
        label: &str,
        kind: ObjectKind,
        bytes: Vec<u8>,
    ) -> Self {
        let sha256 = digest(&bytes);
        let key = format!("objects/{namespace}/{label}/sha256/{sha256}");
        Self {
            reference: HistoryObjectRef {
                key,
                length: u64::try_from(bytes.len()).unwrap_or(u64::MAX),
                sha256,
            },
            kind,
            bytes,
        }
    }

    #[must_use]
    pub fn publication_reference(&self) -> ObjectReference {
        ObjectReference {
            kind: self.kind,
            key: self.reference.key.clone(),
            length: self.reference.length,
            sha256: self.reference.sha256.clone(),
        }
    }
}

#[derive(Clone, Debug)]
pub struct PreparedRoot {
    pub publication_id: String,
    pub destination_root: String,
    pub expected_prior_root: Option<ObjectReference>,
    pub manifest: ObjectReference,
}

pub struct MemoryObjectHistory {
    backend: Arc<dyn Backend>,
    client: ObjectClient,
    authority: PublicationAuthorityState,
    identities: BTreeMap<String, ObjectIdentity>,
    authority_index: u64,
    put_count: u64,
    get_count: u64,
    delete_count: u64,
    put_bytes: u64,
    get_bytes: u64,
}

impl Default for MemoryObjectHistory {
    fn default() -> Self {
        Self::new()
    }
}

impl MemoryObjectHistory {
    #[must_use]
    pub fn new() -> Self {
        let backend = memory_backend();
        Self {
            client: ObjectClient::new(backend.clone()),
            backend,
            authority: PublicationAuthorityState::default(),
            identities: BTreeMap::new(),
            authority_index: 0,
            put_count: 0,
            get_count: 0,
            delete_count: 0,
            put_bytes: 0,
            get_bytes: 0,
        }
    }

    /// Prepare one publication and upload only its declared new-object set.
    ///
    /// # Errors
    ///
    /// Returns an error when authority prepare or any immutable PUT fails.
    pub async fn prepare_and_upload(
        &mut self,
        publication_id: &str,
        destination_root: &str,
        expected_prior_root: Option<ObjectReference>,
        manifest: &ObjectBlob,
        new_objects: &[ObjectBlob],
    ) -> Result<PreparedRoot, String> {
        if manifest.kind != ObjectKind::Manifest {
            return Err("prepared root requires a manifest blob".to_owned());
        }
        let manifest_reference = manifest.publication_reference();
        let object_keys = new_objects
            .iter()
            .map(|object| object.reference.key.clone())
            .chain(std::iter::once(manifest.reference.key.clone()))
            .collect::<BTreeSet<_>>();
        let intent = PublicationIntent {
            object_keys,
            manifest: manifest_reference.clone(),
            destination_root: destination_root.to_owned(),
            expected_prior_root: expected_prior_root.clone(),
        };
        let context = self.next_context();
        let prepared = self.authority.apply(
            &PublicationAction::Prepare {
                publication_id: publication_id.to_owned(),
                intent,
            },
            context,
            PublicationAuthorityFaults::default(),
        );
        if prepared.status != PublicationCommandStatus::Accepted {
            return Err(format!("publication prepare failed: {:?}", prepared.status));
        }
        for object in new_objects.iter().chain(std::iter::once(manifest)) {
            let (_, identity) = self
                .client
                .put_if_absent(&object.reference.key, Bytes::from(object.bytes.clone()))
                .await
                .map_err(|error| error.to_string())?;
            self.put_count = self.put_count.saturating_add(1);
            self.put_bytes = self.put_bytes.saturating_add(object.reference.length);
            self.identities
                .insert(object.reference.key.clone(), identity);
        }
        Ok(PreparedRoot {
            publication_id: publication_id.to_owned(),
            destination_root: destination_root.to_owned(),
            expected_prior_root,
            manifest: manifest_reference,
        })
    }

    /// Publish one previously prepared and caller-verified root.
    ///
    /// # Errors
    ///
    /// Returns an error when the exact authority transition is rejected.
    pub fn publish(&mut self, prepared: &PreparedRoot) -> Result<(), String> {
        let context = self.next_context();
        let transition = self.authority.apply(
            &PublicationAction::Publish {
                publication_id: prepared.publication_id.clone(),
                destination_root: prepared.destination_root.clone(),
                expected_prior_root: prepared.expected_prior_root.clone(),
                manifest: prepared.manifest.clone(),
            },
            context,
            PublicationAuthorityFaults::default(),
        );
        if transition.status != PublicationCommandStatus::Accepted {
            return Err(format!(
                "publication publish failed: {:?}",
                transition.status
            ));
        }
        Ok(())
    }

    /// Cold-read one full object with exact length and digest verification.
    ///
    /// # Errors
    ///
    /// Returns an error when the object is absent, corrupt, or unreadable.
    pub async fn read(&mut self, reference: &HistoryObjectRef) -> Result<Vec<u8>, String> {
        let cold_client = ObjectClient::new(self.backend.clone());
        let (bytes, _) = cold_client
            .read_full_verified(&reference.key, None, reference.length, &reference.sha256)
            .await
            .map_err(|error| error.to_string())?;
        self.get_count = self.get_count.saturating_add(1);
        self.get_bytes = self.get_bytes.saturating_add(reference.length);
        Ok(bytes.to_vec())
    }

    /// Atomically acquire a pin from one exact live root.
    ///
    /// # Errors
    ///
    /// Returns an error when the source root or destination pin changed.
    pub fn pin_from_root(
        &mut self,
        source_root: &str,
        expected_manifest: &ObjectReference,
        pin_id: &str,
    ) -> Result<(), String> {
        let context = self.next_context();
        let transition = self.authority.apply(
            &PublicationAction::PinFromRoot {
                source_root: source_root.to_owned(),
                expected_manifest: expected_manifest.clone(),
                pin_id: pin_id.to_owned(),
                expected_pin: None,
            },
            context,
            PublicationAuthorityFaults::default(),
        );
        if transition.status != PublicationCommandStatus::Accepted {
            return Err(format!("pin from root failed: {:?}", transition.status));
        }
        Ok(())
    }

    /// Remove one exact construction pin.
    ///
    /// # Errors
    ///
    /// Returns an error when the pin no longer names the expected manifest.
    pub fn unpin(&mut self, pin_id: &str, expected: &ObjectReference) -> Result<(), String> {
        let context = self.next_context();
        let transition = self.authority.apply(
            &PublicationAction::Unpin {
                pin_id: pin_id.to_owned(),
                expected: expected.clone(),
            },
            context,
            PublicationAuthorityFaults::default(),
        );
        if transition.status != PublicationCommandStatus::Accepted {
            return Err(format!("unpin failed: {:?}", transition.status));
        }
        Ok(())
    }

    /// Remove one exact root without weakening other roots or pins.
    ///
    /// # Errors
    ///
    /// Returns an error when the root no longer names the expected manifest.
    pub fn remove_root(&mut self, root_id: &str, expected: &ObjectReference) -> Result<(), String> {
        let context = self.next_context();
        let transition = self.authority.apply(
            &PublicationAction::RemoveRoot {
                root_id: root_id.to_owned(),
                expected_manifest: expected.clone(),
            },
            context,
            PublicationAuthorityFaults::default(),
        );
        if transition.status != PublicationCommandStatus::Accepted {
            return Err(format!("root removal failed: {:?}", transition.status));
        }
        Ok(())
    }

    /// Reserve, delete, and retire every inventory candidate outside one
    /// caller-computed complete reachable set.
    ///
    /// # Errors
    ///
    /// Returns an error when inventory, identity, reservation, deletion, or
    /// retirement fails.
    pub async fn sweep_unreachable(
        &mut self,
        reachable: &BTreeSet<String>,
        plan_id: &str,
    ) -> Result<Vec<String>, String> {
        let candidates = self
            .client
            .list_candidates("objects/")
            .await
            .map_err(|error| error.to_string())?;
        let mut deleted = Vec::new();
        for key in candidates {
            if reachable.contains(&key) {
                continue;
            }
            let identity = self
                .identities
                .get(&key)
                .cloned()
                .ok_or_else(|| format!("missing exact identity for {key}"))?;
            let mark_epoch = self.authority.root_intent_epoch;
            let publication_identity = PublicationObjectIdentity {
                revision: RevisionToken {
                    e_tag: identity.revision.e_tag.clone(),
                    version: identity.revision.version.clone(),
                },
                length: identity.length,
                sha256: identity.sha256.clone(),
            };
            let context = self.next_context();
            let reserved = self.authority.apply(
                &PublicationAction::ReserveDelete {
                    plan_id: plan_id.to_owned(),
                    mark_epoch,
                    key: key.clone(),
                    identity: publication_identity,
                },
                context,
                PublicationAuthorityFaults::default(),
            );
            let Some(PublicationOutcome::DeleteReserved { permit }) = reserved.outcome else {
                return Err(format!("delete reservation failed: {:?}", reserved.status));
            };
            let storage_permit = okv_object::DeletePermit::from_publication(&permit);
            self.client
                .delete_reserved(&storage_permit)
                .await
                .map_err(|error| error.to_string())?;
            self.delete_count = self.delete_count.saturating_add(1);
            let context = self.next_context();
            let retired = self.authority.apply(
                &PublicationAction::RetireDelete { permit },
                context,
                PublicationAuthorityFaults::default(),
            );
            if retired.status != PublicationCommandStatus::Accepted {
                return Err(format!("delete retirement failed: {:?}", retired.status));
            }
            deleted.push(key);
        }
        Ok(deleted)
    }

    #[must_use]
    pub fn root(&self, root_id: &str) -> Option<&ObjectReference> {
        self.authority.roots.get(root_id)
    }

    #[must_use]
    pub const fn put_count(&self) -> u64 {
        self.put_count
    }

    #[must_use]
    pub const fn get_count(&self) -> u64 {
        self.get_count
    }

    #[must_use]
    pub const fn delete_count(&self) -> u64 {
        self.delete_count
    }

    #[must_use]
    pub const fn put_bytes(&self) -> u64 {
        self.put_bytes
    }

    #[must_use]
    pub const fn get_bytes(&self) -> u64 {
        self.get_bytes
    }

    fn next_context(&mut self) -> AuthorityContext {
        self.authority_index = self.authority_index.saturating_add(1);
        AuthorityContext {
            generation: 1,
            position: AuthorityPosition {
                term: 1,
                index: self.authority_index,
            },
        }
    }
}

fn digest(bytes: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(bytes);
    format!("{:x}", digest.finalize())
}
