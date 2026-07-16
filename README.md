# ebus-service-discovery-client

[![PyPI version](https://img.shields.io/pypi/v/ebus-service-discovery-client.svg)](https://pypi.org/project/ebus-service-discovery-client/)
[![Ruff](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/astral-sh/ruff/main/assets/badge/v2.json)](https://github.com/astral-sh/ruff)

Client and shared model for an mDNS/DNS-SD **service-discovery bus over MQTT**. A discovery service browses the local network and publishes each advertisement as a retained MQTT record; consumers subscribe, keep a fresh view (honoring freshness and tombstones), and resolve a target service to a reachable address per interface.

This library is the **consumer side plus the shared wire contract**: the record model, its JSON Schema, a live-view resolver, and a debug CLI. A publisher and any number of clients share the contract described below.

- [Why](#why)
- [Install](#install)
- [The contract](#the-contract) — topic, record schema, tombstones, freshness
- [Library usage](#library-usage) — model, resolver, validation
- [CLI usage](#cli-usage) — `dump` / `watch` / `resolve` / `validate`
- [Releasing](#releasing) · [Contributing](#contributing) · [License](#license)

> **Status: alpha.** The API and the v2 contract may still shift before `1.0`.

## Why

Network advertisements are messy in ways every consumer otherwise re-solves alone:

- An advertised IPv4 can be a self-assigned APIPA (`169.254.x`) address that is unroutable, while the peer is reachable only over IPv6.
- The same instance can be heard on several interfaces with different addresses; reachability is per-interface (a probe must bind to the right one).
- Records go stale unless something expires them.

So the contract is deliberately **honest and raw** — it carries the current addresses per interface plus an explicit freshness/tombstone state — and the **classification and reachability policy live here, in one place**, so every consumer gets them for free instead of reinventing (often buggy) copies.

## Install

```bash
pip install ebus-service-discovery-client
# optional JSON-Schema validation (CLI `validate`, strict callers):
pip install "ebus-service-discovery-client[validation]"
```

Requires Python 3.10+. Depends on [`ebus-mqtt-client`](https://github.com/electrification-bus/ebus-mqtt-client) for MQTT transport.

## The contract

The wire contract is what a publisher and every consumer agree on. It is versioned (`v2`) and specified normatively by [`record.schema.json`](src/ebus_service_discovery_client/record.schema.json) (JSON Schema draft 2020-12). This section is the human-readable version.

### Topic

Each record is published as a **retained** message on:

```
{base}/v2/{service_type}/{interface}/{percent_encoded_instance}
```

| Segment | Meaning |
|---|---|
| `{base}` | Deployment topic root. Default `local/mdns/discovery` (so the full base is `local/mdns/discovery/v2`). |
| `{service_type}` | DNS-SD service type, e.g. `_http._tcp`. |
| `{interface}` | The network interface the advertisement was observed on, e.g. `eth0`. The record's addresses are the candidates reachable **via this interface**. |
| `{percent_encoded_instance}` | The DNS-SD **service instance** label, percent-encoded so an arbitrary UTF-8 label (spaces, `/`, unicode) is a single safe topic segment. |

Keying by **instance** (not hostname) is deliberate: DNS-SD identity lives in the instance name, and two instances can share a host. Splitting by **interface** is deliberate too: the same instance heard on `eth0` and `wlan0` is two records with different reachability.

### Record payload

| Field | Type | Required | Meaning |
|---|---|---|---|
| `schema_version` | int (`2`) | yes | Contract major version. |
| `service_type` | string | yes | DNS-SD service type. |
| `instance_name` | string | yes | The unencoded DNS-SD instance label (the topic carries a percent-encoded copy). |
| `hostname` | string | yes | SRV target hostname, e.g. `host-1234.local`. |
| `interface` | string | yes | Observing interface. |
| `port` | int | yes | SRV port as advertised. A client may deliberately use a different port. |
| `addresses` | array of `{address, family}` | yes | The **current** advertised addresses on this interface. Never carried forward; may be empty transiently or contain only IPv6. |
| `txt` | object of string→string | yes | DNS-SD TXT key/values. |
| `state` | `"active"` \| `"removed"` | yes | `removed` is a tombstone (see below). |
| `first_seen` | RFC 3339 UTC | yes | When first observed. |
| `last_seen` | RFC 3339 UTC | yes | When most recently confirmed. Authoritative freshness. |
| `ttl_seconds` | int | no | Advertised TTL / expected refresh window. |
| `removed_at` | RFC 3339 UTC | iff removed | Tombstone timestamp. |

Addresses are carried **raw** — only `{address, family}`. Scope, APIPA, and link-local classification are *derived client-side* from the address value (see [the model](#the-model-record--address)) so the taxonomy can evolve without a contract change.

### Tombstones and freshness

A record is **removed** in one of two ways, and a consumer honors both:

1. **Tombstone message** — a retained record with `state: "removed"` (the full last-known fields plus `removed_at`). This is the primary mechanism; it fires on a DNS-SD goodbye, a TTL expiry, or a browse `ItemRemove`.
2. **Empty retained payload** — clearing the retained topic (a zero-length message) also means "gone."

**Freshness** is `age = now - last_seen`. A consumer can also age a record out itself once `age > ttl_seconds` (a backstop for a missed tombstone) via `Record.is_stale(...)`.

### Example

Active record:

```json
{
  "schema_version": 2,
  "service_type": "_http._tcp",
  "instance_name": "Example Device 42",
  "hostname": "host-1234.local",
  "interface": "eth0",
  "port": 80,
  "addresses": [
    { "address": "192.168.1.10", "family": "ipv4" },
    { "address": "2606:4700:4700::1111", "family": "ipv6" },
    { "address": "fe80::1", "family": "ipv6" }
  ],
  "txt": { "model": "example-1", "id": "abc123" },
  "state": "active",
  "first_seen": "2026-01-01T00:00:00Z",
  "last_seen": "2026-01-01T00:05:00Z",
  "ttl_seconds": 120
}
```

Tombstone (same shape, `state: "removed"` + `removed_at`).

## Library usage

### The model (`Record` / `Address`)

`Record` and `Address` are plain dataclasses that round-trip the wire form. Address classification is derived from the address value:

```python
from ebus_service_discovery_client import Address, Record

rec = Record.from_json(mqtt_payload)      # bytes or str
rec.topic()                                # -> the retained topic for this record
rec.age_seconds()                          # -> freshness, or None if last_seen absent
rec.is_stale()                             # -> True if age > ttl_seconds
rec.is_removed                             # -> True for a tombstone

# routable addresses first, link-local/APIPA last, loopback/unspecified excluded:
for a in rec.candidate_addresses():
    print(a.address, a.family.value, a.scope.value)

a = Address.parse("169.254.1.1")
a.scope          # AddressScope.LINK_LOCAL
a.is_apipa       # True  -> DHCPv4 failed; do not prefer this
a.preference     # sort key: lower is tried first
```

### The resolver (`ServiceResolver`)

`ServiceResolver` keeps a live, tombstone-aware view of the bus and resolves a target service to a **reachable** endpoint. You own the MQTT connection lifecycle; the resolver only subscribes.

```python
from ebus_mqtt_client import MqttClient
from ebus_service_discovery_client import ServiceResolver

mqtt = MqttClient("my-consumer", "127.0.0.1", 1883)
resolver = ServiceResolver(mqtt)
resolver.watch("_http._tcp")
mqtt.start()
# ... let retained records arrive ...

# Resolve a specific instance (match on a TXT field), on port 443 regardless of
# the advertised port:
res = resolver.resolve(
    "_http._tcp",
    match=lambda r: r.txt.get("id") == "abc123",
    port=443,
)
if res:
    url = f"https://{res.host}:{res.port}"   # host is bracketed/zone-qualified as needed
    print(f"reachable via {res.interface}: {url}")
else:
    print("no reachable endpoint")

mqtt.stop()
```

**How `resolve()` chooses:** it gathers every candidate `(record, address)` for the matching instances, orders them **routable-first** (by address scope), then by interface priority, then by preferred family, and **TCP-probes each in order** — binding the probe to the record's interface (`SO_BINDTODEVICE`) — returning the first that connects. Because it probes, an unreachable IPv4 (an APIPA lease, a dead interface) is simply skipped in favor of a working IPv6; there is no family-specific special-casing.

Constructor options:

| Option | Default | Effect |
|---|---|---|
| `base` | `local/mdns/discovery/v2` | Topic base to subscribe under. |
| `probe_timeout` | `5.0` | Per-candidate TCP connect timeout (seconds). |
| `prefer_family` | `None` | `AddressFamily.IPV4`/`IPV6` as a tie-breaker (never overrides scope). |
| `interface_priority` | `[]` | Ordered interface names to prefer, e.g. `["eth1", "eth0", "wlan0"]`. |

Other methods: `records(service_type=None, include_stale=True)` snapshots the current view; `prune_stale()` drops aged-out records; `ingest(record)` applies a record directly (for non-MQTT feeds or tests).

### Schema validation

```python
from ebus_service_discovery_client import load_schema, validate_record

validate_record(record_dict)   # raises jsonschema.ValidationError if invalid
schema = load_schema()         # the bundled draft 2020-12 schema as a dict
```

`validate_record` requires the `validation` extra (`jsonschema`); the model itself round-trips without it.

## CLI usage

Installing the package provides the `service-discovery` command. Global options select the broker and topic base:

```
service-discovery [--host H] [--port P] [--base B] <command> ...
#   --host  MQTT broker host   (default 127.0.0.1)
#   --port  MQTT broker port   (default 1883)
#   --base  topic base         (default local/mdns/discovery/v2)
```

### `dump` — snapshot the retained bus

```bash
service-discovery dump                 # everything
service-discovery dump _http._tcp      # one service type
service-discovery dump --interface eth0 --window 3
```

```
_http._tcp
  eth0
    Example Device 42  (host-1234.local:80)  age 5m  [active]
      192.168.1.10  (ipv4/private)
      2606:4700:4700::1111  (ipv6/global)
      fe80::1  (ipv6/link-local)
```

### `watch` — live add / update / remove

```bash
service-discovery watch _http._tcp     # Ctrl-C to stop
```

```
20:48:17 ACTIVE   _http._tcp/eth0/Example Device 42  [192.168.1.10,fe80::1]
20:49:02 REMOVED  local/mdns/discovery/v2/_http._tcp/eth0/Example%20Device%2042
```

### `resolve` — reachable endpoint for a service

```bash
service-discovery resolve _http._tcp --match id=abc123 --probe-port 443
```

```
192.168.1.10:443  via eth0  (ipv4/private)  instance=Example Device 42
```

Exit code is `0` when resolved, `1` when nothing is reachable.

### `validate` — check records against the schema

```bash
service-discovery validate --file record.json     # a record or a list of records
service-discovery validate                        # validate live records off the bus
```

```
[0] valid
[1] INVALID: 'removed_at' is a required property
2 record(s), 1 invalid
```

Exit code is non-zero if any record is invalid.

## Releasing

The version lives in exactly one place: `__version__` in `src/ebus_service_discovery_client/__init__.py`. `pyproject.toml` reads it dynamically, the `setup.py` legacy shim reads it by regex, and the publish workflow refuses to release a tag that disagrees with it. To cut a release:

1. Bump `__version__` in `src/ebus_service_discovery_client/__init__.py` (the only place).
2. Move the CHANGELOG's `[Unreleased]` entries under a new version heading.
3. Commit, then tag it `v`-prefixed to match: `git tag vX.Y.Z && git push --tags`.

Pushing a `v*` tag runs the publish workflow, which verifies the tag equals `v$__version__`, builds the sdist and wheel, and publishes to PyPI via Trusted Publishing (OIDC, no stored token).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for Discussions, Issues, and pull requests. The library is intentionally vendor- and product-agnostic: it models generic DNS-SD discovery, not any particular device. Changes to the wire contract ([`record.schema.json`](src/ebus_service_discovery_client/record.schema.json) or the topic layout) affect every publisher and consumer — prefer additive changes and align in a Discussion first.

## License

[MIT License](LICENSE) — Copyright (c) 2026 Clark Communications Corporation
