//! Immutable native-resident deployment authority shared by deployment and
//! supervision.
use anyhow::Context;
use errors::ErrorMetadata;
use serde::{
    Deserialize,
    Serialize,
};

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeResidentDescriptor {
    pub artifact_sha256: String,
    pub configuration_sha256: String,
    pub lifecycle_protocol: u32,
    pub application_contract: String,
}

impl NativeResidentDescriptor {
    pub fn validate(&self) -> anyhow::Result<()> {
        for digest in [&self.artifact_sha256, &self.configuration_sha256] {
            anyhow::ensure!(
                digest.len() == 64
                    && digest
                        .bytes()
                        .all(|b| b.is_ascii_digit() || matches!(b, b'a'..=b'f')),
                "Native resident digest must be lowercase SHA-256"
            );
        }
        anyhow::ensure!(
            self.lifecycle_protocol == 1,
            "Unsupported native resident lifecycle protocol"
        );
        anyhow::ensure!(
            !self.application_contract.is_empty()
                && self.application_contract.len() <= 128
                && self
                    .application_contract
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.')),
            "Invalid native resident application contract"
        );
        Ok(())
    }
}

#[derive(Clone, Debug, Eq, PartialEq, Deserialize, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NativeResidentActivation {
    #[serde(deserialize_with = "Option::deserialize")]
    pub expected_prior: Option<NativeResidentDescriptor>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub target: Option<NativeResidentDescriptor>,
    #[serde(deserialize_with = "Option::deserialize")]
    pub application_contract: Option<String>,
}

impl NativeResidentActivation {
    pub fn validate(&self) -> anyhow::Result<()> {
        for descriptor in [&self.expected_prior, &self.target].into_iter().flatten() {
            descriptor.validate()?;
        }
        if let Some(target) = &self.target {
            anyhow::ensure!(
                self.application_contract
                    .as_ref()
                    .context("Native resident deployment contract is required")?
                    == &target.application_contract,
                "Native resident deployment contract mismatch"
            );
        }
        // An incompatible publication cannot expose new APIs to an old draining
        // process.
        if let Some(prior) = &self.expected_prior {
            anyhow::ensure!(
                self.application_contract.as_ref() == Some(&prior.application_contract),
                "Retire the native resident before changing its application contract"
            );
        }
        Ok(())
    }

    pub fn validate_prior(&self, prior: Option<&NativeResidentDescriptor>) -> anyhow::Result<()> {
        anyhow::ensure!(
            prior == self.expected_prior.as_ref(),
            ErrorMetadata::bad_request(
                "NativeResidentExpectedPriorMismatch",
                "Committed native resident changed before activation",
            )
        );
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    fn descriptor() -> NativeResidentDescriptor {
        NativeResidentDescriptor {
            artifact_sha256: "a".repeat(64),
            configuration_sha256: "b".repeat(64),
            lifecycle_protocol: 1,
            application_contract: "example-v1".into(),
        }
    }
    #[test]
    fn activation_checks_compatibility_and_prior() {
        let old = descriptor();
        let mut activation = NativeResidentActivation {
            expected_prior: Some(old.clone()),
            target: Some(old.clone()),
            application_contract: Some("example-v1".into()),
        };
        activation.validate().unwrap();
        activation.validate_prior(Some(&old)).unwrap();
        assert!(activation.validate_prior(None).is_err());
        activation.application_contract = Some("example-v2".into());
        assert!(activation.validate().is_err());
    }
    #[test]
    fn ordinary_selection_does_not_require_native_contract() {
        NativeResidentActivation {
            expected_prior: None,
            target: None,
            application_contract: None,
        }
        .validate()
        .unwrap();
        let mut value = descriptor();
        value.artifact_sha256 = "../resident".into();
        assert!(value.validate().is_err());
    }

    #[test]
    fn activation_requires_explicit_nullable_fields() {
        let complete = serde_json::json!({
            "expectedPrior": null,
            "target": null,
            "applicationContract": null,
        });
        serde_json::from_value::<NativeResidentActivation>(complete.clone())
            .unwrap()
            .validate()
            .unwrap();
        for field in ["expectedPrior", "target", "applicationContract"] {
            let mut incomplete = complete.clone();
            incomplete.as_object_mut().unwrap().remove(field);
            assert!(serde_json::from_value::<NativeResidentActivation>(incomplete).is_err());
        }
    }
}
