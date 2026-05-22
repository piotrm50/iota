// Test-only fault injector for e2e validation of misbehavior reporting.
// Gated entirely by env vars; absent vars = no-op everywhere.
//
//   IOTA_FAULT_INJECTION_MODE   csv of: header,missed,equivocation,invalid_report
//   IOTA_FAULT_INJECTION_RATE   f64 in [0,1] for probabilistic modes (default 0.05)
//   IOTA_FAULT_INJECTION_TARGET hostname; if set, only that authority injects
//   IOTA_FAULT_INJECTION_DET_MOD u64; deterministic modulus for missed/equivocation (default 17)

use std::sync::OnceLock;

#[derive(Copy, Clone, Debug, Default)]
pub(crate) struct Modes {
    pub header: bool,
    pub missed: bool,
    /// Receive-side bump: when a block arrives from the targeted peer, roll
    /// dice and increment the equivocations counter for that peer. Lets us
    /// surface the metric without legitimately producing equivocations on the
    /// wire.
    pub equivocation: bool,
    /// Receive-side bump: same idea for the unprovable-fault counter.
    pub unprovable: bool,
    pub invalid_report: bool,
}

#[derive(Debug)]
pub(crate) struct Config {
    pub modes: Modes,
    pub rate: f64,
    pub target: Option<String>,
    pub det_mod: u64,
}

impl Config {
    fn from_env() -> Self {
        let modes_str = std::env::var("IOTA_FAULT_INJECTION_MODE").unwrap_or_default();
        let mut modes = Modes::default();
        for tok in modes_str.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            match tok {
                "header" => modes.header = true,
                "missed" => modes.missed = true,
                "equivocation" => modes.equivocation = true,
                "unprovable" => modes.unprovable = true,
                "invalid_report" => modes.invalid_report = true,
                other => tracing::warn!("fault_injection: unknown mode {other:?}"),
            }
        }
        let rate = std::env::var("IOTA_FAULT_INJECTION_RATE")
            .ok()
            .and_then(|s| s.parse().ok())
            .unwrap_or(0.05);
        let target = std::env::var("IOTA_FAULT_INJECTION_TARGET")
            .ok()
            .filter(|s| !s.is_empty());
        let det_mod = std::env::var("IOTA_FAULT_INJECTION_DET_MOD")
            .ok()
            .and_then(|s| s.parse().ok())
            .filter(|&m: &u64| m > 0)
            .unwrap_or(17);
        Self { modes, rate, target, det_mod }
    }

    pub fn any_enabled(&self) -> bool {
        let m = self.modes;
        m.header || m.missed || m.equivocation || m.unprovable || m.invalid_report
    }
}

pub(crate) fn config() -> &'static Config {
    static CFG: OnceLock<Config> = OnceLock::new();
    CFG.get_or_init(|| {
        let cfg = Config::from_env();
        if cfg.any_enabled() {
            tracing::warn!(
                target: "fault_injection",
                ?cfg,
                "FAULT INJECTION ENABLED — this build is for testing only",
            );
        }
        cfg
    })
}

/// True if this authority should inject (no target filter, or target matches).
pub(crate) fn authority_in_scope(own_hostname: &str) -> bool {
    match &config().target {
        None => true,
        Some(t) => t == own_hostname,
    }
}

/// Probabilistic roll using the configured rate.
pub(crate) fn roll() -> bool {
    use rand::Rng;
    rand::thread_rng().gen::<f64>() < config().rate
}

/// Deterministic gate: true on rounds where `round % det_mod == 0`.
/// Same input → same output across all observers.
pub(crate) fn deterministic_match(round: u32) -> bool {
    let m = config().det_mod;
    round as u64 % m == 0
}

/// Called on every successful block-bundle receive. If the receive-side modes
/// (`unprovable`, `equivocation`) are enabled and the peer is in scope, rolls
/// dice and bumps the matching counter against the peer.
pub(crate) fn maybe_inject_on_receive(
    store: &crate::misbehavior_store::MisbehaviorStore,
    peer: starfish_config::AuthorityIndex,
    peer_hostname: &str,
) {
    let cfg = config();
    if !cfg.modes.unprovable && !cfg.modes.equivocation {
        return;
    }
    // No target check on receive-side: the local container's env enables the
    // mode, and the fault is attributed to whatever peer sent the block.
    // Tests byzantine reporting: a malicious validator can claim ANY peer
    // misbehaved; median scoring across honest reporters filters the lies.
    let peer_idx = peer.value();
    if cfg.modes.unprovable && roll() {
        tracing::warn!(
            target: "fault_injection",
            peer = peer_hostname,
            "injecting unprovable fault on receive",
        );
        store.inject_unprovable_for_test(peer_idx);
    }
    if cfg.modes.equivocation && roll() {
        tracing::warn!(
            target: "fault_injection",
            peer = peer_hostname,
            "injecting equivocation on receive",
        );
        store.inject_equivocation_for_test(peer_idx);
    }
}
