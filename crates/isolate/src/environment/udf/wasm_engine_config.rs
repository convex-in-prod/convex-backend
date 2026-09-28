use std::num::NonZeroU64;

use serde::{
    Deserialize,
    Deserializer,
    Serialize,
};

pub(crate) const WASMTIME_REVISION: &str = "2539448f49dec017d502164f46736b64c561e070";
pub(crate) const LEGACY_WASMTIME_REVISION: &str = "7ad2e732ab9ca8665d3cdd91f9c395315eeafc81";

#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum WasmMeteringMode {
    EpochChecks,
    FuelEpochPolling,
}

impl WasmMeteringMode {
    pub(crate) fn fuel_epoch_poll_interval(self) -> u64 {
        match self {
            Self::EpochChecks => 0,
            Self::FuelEpochPolling => 100_000,
        }
    }

    pub(crate) fn configuration_sha256(self) -> &'static str {
        match self {
            Self::EpochChecks => "bebac200e042d7c2678b574ba75645f1bf06bb9295982d46184384b0376874b6",
            Self::FuelEpochPolling => {
                "ac4cebe59819a99482a362e76dfc0428cc15bc2570e88033ae8ca0d2ee7dcf45"
            },
        }
    }

    // These hashes describe the baseline x86-64 native target. Core Wasm
    // contracts are independent of this native artifact compatibility boundary.
    pub(crate) fn compatibility_sha256(self) -> &'static str {
        match self {
            Self::EpochChecks => "89880ad9de64f2bc9e218bf19bac3fb9963b23c56f02b302f1bf1f69d48606f8",
            Self::FuelEpochPolling => {
                "0ac017b218ed85eabc3dd2d4b27914be6002786bd7729656177d0445a27246e0"
            },
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct WasmEngineConfig {
    pub(crate) consume_fuel: bool,
    pub(crate) epoch_interruption: bool,
    // Absence is the historical zero mode. Explicit null and zero are invalid,
    // and serialization must preserve the old configuration's exact identity.
    #[serde(
        default,
        skip_serializing_if = "Option::is_none",
        deserialize_with = "poll_interval"
    )]
    pub(crate) fuel_epoch_poll_interval: Option<NonZeroU64>,
    pub(crate) profiling_strategy: String,
    pub(crate) wasm_exceptions: bool,
}

fn poll_interval<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<NonZeroU64>, D::Error> {
    NonZeroU64::deserialize(deserializer).map(Some)
}

impl WasmEngineConfig {
    pub(crate) fn metering_mode(&self) -> anyhow::Result<WasmMeteringMode> {
        anyhow::ensure!(
            self.consume_fuel
                && self.epoch_interruption
                && self.profiling_strategy == "perf-map"
                && self.wasm_exceptions,
            "unsupported native Wasm engine configuration"
        );
        match self.fuel_epoch_poll_interval {
            None => Ok(WasmMeteringMode::EpochChecks),
            Some(interval)
                if interval.get()
                    == WasmMeteringMode::FuelEpochPolling.fuel_epoch_poll_interval() =>
            {
                Ok(WasmMeteringMode::FuelEpochPolling)
            },
            Some(_) => anyhow::bail!("unsupported fuel-scheduled epoch polling interval"),
        }
    }
}

#[cfg(test)]
mod tests {
    use sha2::{
        Digest,
        Sha256,
    };

    use super::*;

    #[test]
    fn native_modes_preserve_legacy_identity_and_reject_unknown_policies() -> anyhow::Result<()> {
        let legacy = serde_json::json!({"consumeFuel": true, "epochInterruption": true, "profilingStrategy": "perf-map", "wasmExceptions": true});
        for mode in [
            WasmMeteringMode::EpochChecks,
            WasmMeteringMode::FuelEpochPolling,
        ] {
            let mut value = legacy.clone();
            if mode == WasmMeteringMode::FuelEpochPolling {
                value["fuelEpochPollInterval"] = serde_json::json!(100_000);
            }
            let config: WasmEngineConfig = serde_json::from_value(value.clone())?;
            assert_eq!(config.metering_mode()?, mode);
            assert_eq!(serde_json::to_value(&config)?, value);
            assert_eq!(
                format!("{:x}", Sha256::digest(serde_json::to_vec(&config)?)),
                mode.configuration_sha256()
            );
        }
        for interval in [
            serde_json::Value::Null,
            serde_json::json!(0),
            serde_json::json!(-1),
            serde_json::json!(0.5),
            serde_json::json!("100000"),
        ] {
            let mut value = legacy.clone();
            value["fuelEpochPollInterval"] = interval;
            assert!(serde_json::from_value::<WasmEngineConfig>(value).is_err());
        }
        let mut unsupported = legacy;
        unsupported["fuelEpochPollInterval"] = serde_json::json!(200_000);
        assert!(serde_json::from_value::<WasmEngineConfig>(unsupported)?
            .metering_mode()
            .is_err());
        Ok(())
    }
}
