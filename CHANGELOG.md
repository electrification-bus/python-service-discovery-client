# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.1] - 2026-07-16

### Added

- `service-discovery stats`: characterize the live bus in one shot -- totals,
  per-interface (instances + addresses), and the service-type breakdown.
- `service-discovery snapshot`: capture the retained bus plus metadata
  (`captured_at`, `base`, host) to a JSON file for soak testing over time.
- `service-discovery diff OLD NEW`: fuzzy-compare two snapshots. Ignores the
  volatile timestamps and leads with a plain-language "is this network kinda the
  same?" verdict (keyed on how much of the prior instance set survived), then
  side-by-side characterization (counts, scope/family histograms, size, age) and
  the structural delta (instances added / removed / changed). `--json` for
  machine output.

## [0.1.0] - 2026-07-16

### Added

- `Record` and `Address` model for the v1 mDNS/DNS-SD service-discovery contract.
  Addresses are carried raw on the wire; scope / APIPA / link-local classification
  and reachability-preference ordering are derived client-side from the address
  value so the taxonomy can evolve without a contract change.
- Bundled draft 2020-12 JSON Schema (`record.schema.json`), exposed via
  `load_schema()`, plus an optional `validate_record()` helper gated on the
  `validation` extra (`jsonschema`).
- Percent-encoded topic derivation, freshness helpers (`age_seconds`,
  `is_stale`), and active/removed (tombstone) record states.
- `ServiceResolver`: subscribes to a v1 discovery bus via `ebus-mqtt-client`,
  keeps a fresh in-memory view honoring tombstones, and resolves a target
  service to a reachable endpoint by ordering candidates routable-first and
  TCP-probing each (bound to the record's interface). An unreachable IPv4 (e.g.
  an APIPA lease) simply falls through to a working IPv6 -- no family special-casing.
- `service-discovery` CLI (`dump` / `watch` / `resolve` / `validate`) for
  inspecting and resolving a bus and validating records against the schema.
  A global `--json` flag switches every command to machine-readable output for
  `jq` (arrays for `dump`/`validate`, newline-delimited events for `watch`, a
  single object or `null` for `resolve`); `dump`/`watch` records are enriched
  with a derived per-address `scope` and record-level `age_seconds`/`is_stale`.
