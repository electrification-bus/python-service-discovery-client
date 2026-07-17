"""Command-line tools for inspecting and resolving a v1 service-discovery bus.

Subcommands: ``dump`` (snapshot the retained bus), ``watch`` (live add/update/
remove), ``resolve`` (find a reachable endpoint for a service), ``validate``
(check records against the bundled JSON Schema), ``snapshot`` (capture the bus
plus metadata to a JSON file) and ``diff`` (fuzzy-compare two snapshots -- "is
this network kinda the same?"). ``--json`` switches every command to
machine-readable output for ``jq`` post-processing. The MQTT client is imported
lazily inside the commands that need a broker, so the pure formatters (and the
whole snapshot/diff path) remain importable and testable without one.
"""

from __future__ import annotations

import argparse
import json
import sys
import time
from collections import Counter
from datetime import datetime, timezone

from ebus_service_discovery_client.record import DEFAULT_TOPIC_BASE, Record
from ebus_service_discovery_client.resolver import Resolution, ServiceResolver
from ebus_service_discovery_client.schema import validate_record

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 1883
DEFAULT_WINDOW = 2.0
_CLIENT_ID = "service-discovery-cli"


# --- pure formatters (no I/O) ---------------------------------------------


def _format_age(seconds: float | None) -> str:
    if seconds is None:
        return "?"
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m"
    if s < 86400:
        return f"{s // 3600}h"
    return f"{s // 86400}d"


def render_tree(records: list[Record], now: datetime | None = None) -> str:
    """A readable service_type -> interface -> instance -> addresses tree."""
    now = now or datetime.now(timezone.utc)
    by_type: dict[str, dict[str, list[Record]]] = {}
    for r in records:
        by_type.setdefault(r.service_type, {}).setdefault(r.interface, []).append(r)
    lines: list[str] = []
    for stype in sorted(by_type):
        lines.append(stype)
        for iface in sorted(by_type[stype]):
            lines.append(f"  {iface}")
            for r in sorted(by_type[stype][iface], key=lambda x: x.instance_name):
                stale = " STALE" if r.is_stale(now) else ""
                lines.append(
                    f"    {r.instance_name}  ({r.hostname}:{r.port})  "
                    f"age {_format_age(r.age_seconds(now))}  [{r.state.value}{stale}]"
                )
                for a in r.addresses:
                    lines.append(f"      {a.address}  ({a.family.value}/{a.scope.value})")
    return "\n".join(lines) if lines else "(no records)"


def render_resolution(res: Resolution | None) -> str:
    if res is None:
        return "unresolved (no reachable address)"
    return (
        f"{res.host}:{res.port}  via {res.interface}  "
        f"({res.address.family.value}/{res.address.scope.value})  "
        f"instance={res.record.instance_name}"
    )


def record_to_debug_json(record: Record, now: datetime | None = None) -> dict:
    """The wire record plus derived fields (per-address ``scope``, ``age_seconds``,
    ``is_stale``) for ``--json`` output. Not the wire contract -- a debug view."""
    now = now or datetime.now(timezone.utc)
    d = record.to_dict()
    d["age_seconds"] = record.age_seconds(now)
    d["is_stale"] = record.is_stale(now)
    for wire, addr in zip(d["addresses"], record.addresses, strict=True):
        wire["scope"] = addr.scope.value
    return d


def resolution_to_json(res: Resolution | None) -> dict | None:
    if res is None:
        return None
    return {
        "host": res.host,
        "port": res.port,
        "interface": res.interface,
        "address": res.address.address,
        "family": res.address.family.value,
        "scope": res.address.scope.value,
        "service_type": res.record.service_type,
        "instance_name": res.record.instance_name,
    }


def _match_from_arg(spec: str | None):
    """Turn a `key=value` TXT filter into a Record predicate."""
    if not spec:
        return None
    key, _, value = spec.partition("=")

    def _match(record: Record) -> bool:
        return record.txt.get(key) == value

    return _match


# --- snapshot / diff (pure; for soak-testing over time) -------------------

SNAPSHOT_VERSION = 1
_DIFF_LIST_CAP = 20


def snapshot_payload(records: list[dict], *, base: str, host: str, port: int, now=None) -> dict:
    """Wrap enriched record dicts with capture metadata so two snapshots taken
    at different times can be characterized and compared."""
    now = now or datetime.now(timezone.utc)
    return {
        "snapshot_version": SNAPSHOT_VERSION,
        "captured_at": now.isoformat().replace("+00:00", "Z"),
        "base": base,
        "host": host,
        "port": port,
        "record_count": len(records),
        "records": records,
    }


def _load_snapshot(path: str) -> tuple[dict, list[dict]]:
    """Read a snapshot file. Accepts either a ``snapshot`` wrapper object or a
    bare ``--json dump`` array; returns ``(metadata, records)``."""
    with open(path, encoding="utf-8") as fh:
        data = json.load(fh)
    if isinstance(data, list):
        return {}, data
    return {k: v for k, v in data.items() if k != "records"}, data.get("records", [])


def _parse_iso(s: str) -> datetime:
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def _record_key(rec: dict) -> tuple[str, str, str]:
    return (rec.get("service_type"), rec.get("interface"), rec.get("instance_name"))


def _address_set(rec: dict) -> frozenset:
    return frozenset((a.get("address"), a.get("family")) for a in rec.get("addresses", []))


def characterize(records: list[dict]) -> dict:
    """Reduce a record set to shape/size/age stats -- the 'roughly, what does
    this network look like' summary that a fuzzy diff compares."""
    scopes: Counter = Counter()
    families: Counter = Counter()
    states: Counter = Counter()
    type_instances: Counter = Counter()
    iface_instances: Counter = Counter()
    iface_addresses: Counter = Counter()
    total_addresses = 0
    stale = 0
    first_seens: list[str] = []
    last_seens: list[str] = []
    for r in records:
        iface = r.get("interface")
        type_instances[r.get("service_type")] += 1
        iface_instances[iface] += 1
        states[r.get("state", "active")] += 1
        if r.get("is_stale"):
            stale += 1
        for a in r.get("addresses", []):
            total_addresses += 1
            iface_addresses[iface] += 1
            scopes[a.get("scope", "unknown")] += 1
            families[a.get("family", "unknown")] += 1
        if r.get("first_seen"):
            first_seens.append(r["first_seen"])
        if r.get("last_seen"):
            last_seens.append(r["last_seen"])
    return {
        "service_types": len(type_instances),
        "instances": len(records),
        "addresses": total_addresses,
        "interfaces": sorted(i for i in iface_instances if i),
        "by_interface": {
            i: {"instances": iface_instances[i], "addresses": iface_addresses[i]}
            for i in sorted(iface_instances)
        },
        "by_service_type": dict(type_instances.most_common()),
        "scopes": dict(scopes),
        "families": dict(families),
        "states": dict(states),
        "stale": stale,
        "size_bytes": len(json.dumps(records)),
        # ISO-8601 UTC strings sort lexicographically, so min/max = oldest/newest
        "oldest_first_seen": min(first_seens) if first_seens else None,
        "newest_last_seen": max(last_seens) if last_seens else None,
    }


def _record_deltas(old: dict, new: dict) -> list[str]:
    """What changed between two records for the same key, ignoring the volatile
    timestamps (first_seen/last_seen/age/is_stale never match across time)."""
    deltas: list[str] = []
    if old.get("state") != new.get("state"):
        deltas.append(f"state {old.get('state')}->{new.get('state')}")
    old_addrs, new_addrs = _address_set(old), _address_set(new)
    for addr, _fam in sorted(new_addrs - old_addrs):
        deltas.append(f"+addr {addr}")
    for addr, _fam in sorted(old_addrs - new_addrs):
        deltas.append(f"-addr {addr}")
    if old.get("hostname") != new.get("hostname"):
        deltas.append(f"hostname {old.get('hostname')}->{new.get('hostname')}")
    if old.get("port") != new.get("port"):
        deltas.append(f"port {old.get('port')}->{new.get('port')}")
    if old.get("txt") != new.get("txt"):
        deltas.append("txt changed")
    return deltas


def diff_snapshots(old_records: list[dict], new_records: list[dict]) -> dict:
    """Structural diff keyed on (service_type, interface, instance_name)."""
    old = {_record_key(r): r for r in old_records}
    new = {_record_key(r): r for r in new_records}
    old_keys, new_keys = set(old), set(new)
    common = old_keys & new_keys
    changed = []
    for key in sorted(common):
        deltas = _record_deltas(old[key], new[key])
        if deltas:
            changed.append({"key": list(key), "changes": deltas})
    union = old_keys | new_keys
    return {
        "added": [list(k) for k in sorted(new_keys - old_keys)],
        "removed": [list(k) for k in sorted(old_keys - new_keys)],
        "changed": changed,
        "unchanged": len(common) - len(changed),
        "jaccard": (len(common) / len(union)) if union else 1.0,
    }


def _verdict(old_c: dict, new_c: dict, diff: dict) -> str:
    """The plain-language 'is this network kinda the same?' one-liner. Keyed on
    retention (how much of the prior instance set survived) rather than raw
    Jaccard, so pure growth reads as 'grew', not 'different'."""
    old_n = old_c["instances"]
    common = diff["unchanged"] + len(diff["changed"])
    retention = common / old_n if old_n else 1.0
    di = new_c["instances"] - old_c["instances"]
    j = diff["jaccard"]
    within_10pct = abs(di) <= max(1, round(0.1 * old_n))
    if retention >= 0.9 and within_10pct:
        return f"kinda the same network ({retention:.0%} of prior instances retained, {j:.0%} overlap, {di:+d})"
    if retention >= 0.6:
        trend = "grew" if di > 0 else "shrank" if di < 0 else "churned"
        return (
            f"same core but {trend} ({retention:.0%} retained, {di:+d} instances, {j:.0%} overlap)"
        )
    return f"substantially different network ({retention:.0%} of prior instances retained, {j:.0%} overlap)"


def _fmt_counter(d: dict) -> str:
    return ", ".join(f"{k}:{v}" for k, v in sorted(d.items())) or "-"


def render_diff(old_meta: dict, old_c: dict, new_meta: dict, new_c: dict, diff: dict) -> str:
    lines = [_verdict(old_c, new_c, diff), ""]

    old_at, new_at = old_meta.get("captured_at"), new_meta.get("captured_at")
    if old_at and new_at:
        try:
            elapsed = (_parse_iso(new_at) - _parse_iso(old_at)).total_seconds()
            lines.append(f"between snapshots: {_format_age(abs(elapsed))}  ({old_at} -> {new_at})")
            lines.append("")
        except ValueError:
            pass

    def row(label, old_v, new_v):
        delta = ""
        if isinstance(old_v, int) and isinstance(new_v, int) and (new_v - old_v):
            delta = f"{new_v - old_v:+d}"
        return f"  {label:<16}{str(old_v):>10}{str(new_v):>10}   {delta}"

    lines.append(f"  {'':<16}{'OLD':>10}{'NEW':>10}")
    lines.append(row("service-types", old_c["service_types"], new_c["service_types"]))
    lines.append(row("instances", old_c["instances"], new_c["instances"]))
    lines.append(row("addresses", old_c["addresses"], new_c["addresses"]))
    lines.append(row("stale", old_c["stale"], new_c["stale"]))
    lines.append(row("size (bytes)", old_c["size_bytes"], new_c["size_bytes"]))
    lines.append(f"  {'scopes':<16}old: {_fmt_counter(old_c['scopes'])}")
    lines.append(f"  {'':<16}new: {_fmt_counter(new_c['scopes'])}")
    lines.append(
        f"  {'families':<16}old: {_fmt_counter(old_c['families'])}   new: {_fmt_counter(new_c['families'])}"
    )
    lines.append("")
    lines.append(
        f"  overlap {diff['jaccard']:.0%}   unchanged {diff['unchanged']}   "
        f"changed {len(diff['changed'])}   added {len(diff['added'])}   removed {len(diff['removed'])}"
    )

    def detail(marker, label, keys):
        if not keys:
            return
        lines.append("")
        lines.append(f"  {marker} {label} ({len(keys)}):")
        for k in keys[:_DIFF_LIST_CAP]:
            lines.append(f"      {k[0]} / {k[1]} / {k[2]}")
        if len(keys) > _DIFF_LIST_CAP:
            lines.append(f"      ... and {len(keys) - _DIFF_LIST_CAP} more")

    detail("+", "added", diff["added"])
    detail("-", "removed", diff["removed"])
    if diff["changed"]:
        lines.append("")
        lines.append(f"  ~ changed ({len(diff['changed'])}):")
        for c in diff["changed"][:_DIFF_LIST_CAP]:
            k = c["key"]
            lines.append(f"      {k[2]} ({k[0]}/{k[1]}): {'; '.join(c['changes'])}")
        if len(diff["changed"]) > _DIFF_LIST_CAP:
            lines.append(f"      ... and {len(diff['changed']) - _DIFF_LIST_CAP} more")

    return "\n".join(lines)


def render_stats(char: dict, meta: dict | None = None) -> str:
    """A one-shot characterization of the live bus: totals, per-interface, and
    the service-type breakdown."""
    lines = ["discovery bus stats"]
    if meta and meta.get("base"):
        lines.append(f"  base           {meta['base']}")
    lines.append(f"  service-types  {char['service_types']}")
    lines.append(f"  instances      {char['instances']}")
    lines.append(f"  addresses      {char['addresses']}")
    lines.append(f"  stale          {char['stale']}")
    lines.append(f"  size           {char['size_bytes']} bytes")
    lines.append(f"  states         {_fmt_counter(char['states'])}")
    lines.append(f"  scopes         {_fmt_counter(char['scopes'])}")
    lines.append(f"  families       {_fmt_counter(char['families'])}")
    if char.get("oldest_first_seen"):
        lines.append(f"  oldest seen    {char['oldest_first_seen']}")
        lines.append(f"  newest seen    {char['newest_last_seen']}")

    lines.append("")
    lines.append("  per interface:")
    for iface, s in char["by_interface"].items():
        lines.append(f"    {iface:<10}{s['instances']:>5} instances  {s['addresses']:>5} addresses")

    lines.append("")
    lines.append("  by service-type:")
    types = list(char["by_service_type"].items())
    for stype, n in types[:_DIFF_LIST_CAP]:
        lines.append(f"    {stype:<30}{n:>5}")
    if len(types) > _DIFF_LIST_CAP:
        lines.append(f"    ... and {len(types) - _DIFF_LIST_CAP} more service-types")
    return "\n".join(lines)


# --- MQTT-backed commands (lazy import) -----------------------------------


def _collect(host, port, patterns, window):
    """Connect, subscribe, collect the latest Record per topic for `window` s."""
    from ebus_mqtt_client import MqttClient

    records: dict[str, Record] = {}

    def handler(topic, payload):
        if not payload or not bytes(payload).strip():
            records.pop(topic, None)
            return
        try:
            rec = Record.from_json(bytes(payload))
        except Exception:
            return
        if rec.is_removed:
            records.pop(topic, None)
        else:
            records[topic] = rec

    mqtt = MqttClient(_CLIENT_ID, host, port)
    for pat in patterns:
        mqtt.subscribe(pat, param=handler)
    mqtt.start()
    time.sleep(window)
    mqtt.stop()
    return list(records.values())


def _service_pattern(base, service_type):
    return f"{base}/{service_type}/+/+" if service_type else f"{base}/#"


def cmd_dump(args) -> int:
    records = _collect(
        args.host, args.port, [_service_pattern(args.base, args.service_type)], args.window
    )
    if args.interface:
        records = [r for r in records if r.interface == args.interface]
    if args.json:
        print(json.dumps([record_to_debug_json(r) for r in records], indent=2))
    else:
        print(render_tree(records))
    return 0


def cmd_watch(args) -> int:
    from ebus_mqtt_client import MqttClient

    def handler(topic, payload):
        now = datetime.now(timezone.utc)
        removed = not payload or not bytes(payload).strip()
        rec = None
        if not removed:
            try:
                rec = Record.from_json(bytes(payload))
            except Exception:
                rec = None
        if args.json:
            event = {"ts": now.isoformat().replace("+00:00", "Z"), "topic": topic}
            if removed:
                event["verb"] = "removed"
            elif rec is None:
                event["verb"] = "unparseable"
            else:
                event["verb"] = "removed" if rec.is_removed else "active"
                event["record"] = record_to_debug_json(rec, now)
            print(json.dumps(event))
            return
        ts = now.strftime("%H:%M:%S")
        if removed:
            print(f"{ts} REMOVED  {topic}")
        elif rec is None:
            print(f"{ts} BADMSG   {topic}")
        else:
            verb = "REMOVED" if rec.is_removed else "ACTIVE"
            addrs = ",".join(a.address for a in rec.addresses)
            print(
                f"{ts} {verb:8} {rec.service_type}/{rec.interface}/{rec.instance_name}  [{addrs}]"
            )

    mqtt = MqttClient(_CLIENT_ID, args.host, args.port)
    mqtt.subscribe(_service_pattern(args.base, args.service_type), param=handler)
    mqtt.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        mqtt.stop()
    return 0


def cmd_resolve(args) -> int:
    from ebus_mqtt_client import MqttClient

    mqtt = MqttClient(_CLIENT_ID, args.host, args.port)
    resolver = ServiceResolver(mqtt, base=args.base)
    resolver.watch(args.service_type)
    mqtt.start()
    time.sleep(args.window)
    res = resolver.resolve(args.service_type, _match_from_arg(args.match), port=args.probe_port)
    mqtt.stop()
    if args.json:
        print(json.dumps(resolution_to_json(res)))
    else:
        print(render_resolution(res))
    return 0 if res is not None else 1


def cmd_validate(args) -> int:
    if args.file:
        with open(args.file, encoding="utf-8") as fh:
            data = json.load(fh)
        records = data if isinstance(data, list) else [data]
    else:
        from ebus_mqtt_client import MqttClient

        collected: dict[str, dict] = {}

        def handler(topic, payload):
            if payload and bytes(payload).strip():
                try:
                    collected[topic] = json.loads(bytes(payload))
                except json.JSONDecodeError:
                    collected[topic] = {"__unparseable__": True}

        mqtt = MqttClient(_CLIENT_ID, args.host, args.port)
        mqtt.subscribe(f"{args.base}/#", param=handler)
        mqtt.start()
        time.sleep(args.window)
        mqtt.stop()
        records = list(collected.values())

    results = []
    for i, rec in enumerate(records):
        try:
            validate_record(rec)
            results.append({"index": i, "valid": True, "error": None})
        except Exception as exc:
            results.append({"index": i, "valid": False, "error": str(exc).splitlines()[0]})
    errors = sum(1 for r in results if not r["valid"])

    if args.json:
        print(json.dumps(results, indent=2))
    else:
        for r in results:
            if r["valid"]:
                print(f"[{r['index']}] valid")
            else:
                print(f"[{r['index']}] INVALID: {r['error']}")
        print(f"{len(records)} record(s), {errors} invalid")
    return 1 if errors else 0


def cmd_snapshot(args) -> int:
    records = _collect(
        args.host, args.port, [_service_pattern(args.base, args.service_type)], args.window
    )
    payload = snapshot_payload(
        [record_to_debug_json(r) for r in records],
        base=args.base,
        host=args.host,
        port=args.port,
    )
    text = json.dumps(payload, indent=2)
    if args.output:
        with open(args.output, "w", encoding="utf-8") as fh:
            fh.write(text + "\n")
        print(
            f"wrote {payload['record_count']} records to {args.output} "
            f"(captured_at {payload['captured_at']})"
        )
    else:
        print(text)
    return 0


def cmd_diff(args) -> int:
    old_meta, old_records = _load_snapshot(args.old)
    new_meta, new_records = _load_snapshot(args.new)
    old_c = characterize(old_records)
    new_c = characterize(new_records)
    diff = diff_snapshots(old_records, new_records)
    if args.json:
        print(
            json.dumps(
                {
                    "old": {**old_meta, "characterization": old_c},
                    "new": {**new_meta, "characterization": new_c},
                    "diff": diff,
                },
                indent=2,
            )
        )
    else:
        print(render_diff(old_meta, old_c, new_meta, new_c, diff))
    return 0


def cmd_stats(args) -> int:
    records = _collect(
        args.host, args.port, [_service_pattern(args.base, args.service_type)], args.window
    )
    char = characterize([record_to_debug_json(r) for r in records])
    if args.json:
        print(json.dumps(char, indent=2))
    else:
        print(render_stats(char, {"base": args.base}))
    return 0


def main(argv: list[str] | None = None) -> int:
    p = argparse.ArgumentParser(
        prog="service-discovery",
        description="Inspect and resolve a v1 mDNS/DNS-SD service-discovery bus over MQTT.",
    )
    p.add_argument(
        "--host", default=DEFAULT_HOST, help=f"MQTT broker host (default {DEFAULT_HOST})"
    )
    p.add_argument(
        "--port", type=int, default=DEFAULT_PORT, help=f"MQTT broker port (default {DEFAULT_PORT})"
    )
    p.add_argument("--base", default=DEFAULT_TOPIC_BASE, help="discovery topic base")
    p.add_argument("--json", action="store_true", help="machine-readable output (for jq)")
    sub = p.add_subparsers(dest="cmd", required=True)

    d = sub.add_parser("dump", help="snapshot the retained bus as a tree")
    d.add_argument("service_type", nargs="?", help="limit to one service type")
    d.add_argument("--interface", help="limit to one interface")
    d.add_argument("--window", type=float, default=DEFAULT_WINDOW)
    d.set_defaults(func=cmd_dump)

    w = sub.add_parser("watch", help="live add/update/remove stream")
    w.add_argument("service_type", nargs="?")
    w.set_defaults(func=cmd_watch)

    r = sub.add_parser("resolve", help="resolve a reachable endpoint for a service")
    r.add_argument("service_type")
    r.add_argument("--match", help="TXT filter key=value, e.g. serialnum=123")
    r.add_argument(
        "--probe-port",
        type=int,
        dest="probe_port",
        help="probe/return this port instead of the advertised one",
    )
    r.add_argument("--window", type=float, default=DEFAULT_WINDOW)
    r.set_defaults(func=cmd_resolve)

    v = sub.add_parser("validate", help="validate record(s) against the JSON Schema")
    v.add_argument("--file", help="a JSON file with a record or a list of records")
    v.add_argument("--window", type=float, default=DEFAULT_WINDOW)
    v.set_defaults(func=cmd_validate)

    s = sub.add_parser("snapshot", help="capture the bus + metadata to a JSON snapshot")
    s.add_argument("service_type", nargs="?", help="limit to one service type")
    s.add_argument("-o", "--output", help="write the snapshot here (default: stdout)")
    s.add_argument("--window", type=float, default=DEFAULT_WINDOW)
    s.set_defaults(func=cmd_snapshot)

    df = sub.add_parser("diff", help="fuzzy-compare two snapshots (is the network kinda the same?)")
    df.add_argument("old", help="the older snapshot file")
    df.add_argument("new", help="the newer snapshot file")
    df.set_defaults(func=cmd_diff)

    st = sub.add_parser("stats", help="characterize the live bus (totals, per-interface, per-type)")
    st.add_argument("service_type", nargs="?", help="limit to one service type")
    st.add_argument("--window", type=float, default=DEFAULT_WINDOW)
    st.set_defaults(func=cmd_stats)

    args = p.parse_args(argv)
    return args.func(args)


if __name__ == "__main__":
    sys.exit(main())
