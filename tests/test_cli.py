import json
from datetime import datetime, timezone

import pytest

from ebus_service_discovery_client import Address, Record
from ebus_service_discovery_client.cli import (
    _format_age,
    _load_snapshot,
    _match_from_arg,
    _verdict,
    characterize,
    diff_snapshots,
    main,
    record_to_debug_json,
    render_resolution,
    render_stats,
    render_tree,
    resolution_to_json,
    snapshot_payload,
)
from ebus_service_discovery_client.resolver import Resolution

NOW = datetime(2026, 1, 1, 0, 10, 0, tzinfo=timezone.utc)


def _record(**kw):
    base = dict(
        service_type="_example._tcp",
        instance_name="Dev 1",
        hostname="h.local",
        interface="eth0",
        port=80,
        addresses=[Address.parse("192.168.1.10"), Address.parse("fe80::1")],
        txt={"serial": "abc"},
        first_seen=datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        last_seen=datetime(2026, 1, 1, 0, 5, 0, tzinfo=timezone.utc),
        ttl_seconds=120,
    )
    base.update(kw)
    return Record(**base)


@pytest.mark.parametrize(
    "seconds,text",
    [(None, "?"), (5, "5s"), (90, "1m"), (7200, "2h"), (200000, "2d")],
)
def test_format_age(seconds, text):
    assert _format_age(seconds) == text


def test_render_tree_structure():
    out = render_tree([_record()], now=NOW)
    assert "_example._tcp" in out
    assert "  eth0" in out
    assert "Dev 1" in out and "h.local:80" in out
    assert "192.168.1.10  (ipv4/private)" in out
    assert "fe80::1  (ipv6/link-local)" in out
    assert "STALE" in out  # last_seen 00:05, ttl 120s, now 00:10 -> stale


def test_render_tree_empty():
    assert render_tree([], now=NOW) == "(no records)"


def test_render_resolution():
    rec = _record()
    res = Resolution(rec, Address.parse("192.168.1.10"), "eth0", 443)
    text = render_resolution(res)
    assert "192.168.1.10:443" in text and "via eth0" in text and "ipv4/private" in text
    assert render_resolution(None) == "unresolved (no reachable address)"


def test_match_from_arg():
    assert _match_from_arg(None) is None
    m = _match_from_arg("serial=abc")
    assert m(_record()) is True
    assert m(_record(txt={"serial": "zzz"})) is False


def test_validate_file_valid(tmp_path, capsys):
    p = tmp_path / "rec.json"
    p.write_text(json.dumps(_record().to_dict()))
    assert main(["validate", "--file", str(p)]) == 0
    assert "valid" in capsys.readouterr().out


def test_validate_file_invalid(tmp_path, capsys):
    p = tmp_path / "bad.json"
    p.write_text(json.dumps({"schema_version": 1, "service_type": "_x._tcp"}))  # missing required
    assert main(["validate", "--file", str(p)]) == 1
    assert "INVALID" in capsys.readouterr().out


def test_validate_file_list(tmp_path, capsys):
    p = tmp_path / "recs.json"
    p.write_text(json.dumps([_record().to_dict(), _record(instance_name="Dev 2").to_dict()]))
    assert main(["validate", "--file", str(p)]) == 0
    assert "2 record(s), 0 invalid" in capsys.readouterr().out


def test_record_to_debug_json():
    d = record_to_debug_json(_record(), now=NOW)
    # wire fields survive
    assert d["service_type"] == "_example._tcp"
    assert d["instance_name"] == "Dev 1"
    # derived fields added
    assert d["age_seconds"] == 300.0  # last_seen 00:05, now 00:10
    assert d["is_stale"] is True  # ttl 120s exceeded
    # per-address scope classification is injected into each wire address
    scopes = {a["address"]: a["scope"] for a in d["addresses"]}
    assert scopes == {"192.168.1.10": "private", "fe80::1": "link-local"}
    # the wire address keys are untouched otherwise
    assert all("family" in a for a in d["addresses"])


def test_resolution_to_json():
    rec = _record()
    res = Resolution(rec, Address.parse("fe80::1"), "eth0", 443)
    d = resolution_to_json(res)
    assert d == {
        "host": "[fe80::1%eth0]",  # link-local host is zone-qualified
        "port": 443,
        "interface": "eth0",
        "address": "fe80::1",
        "family": "ipv6",
        "scope": "link-local",
        "service_type": "_example._tcp",
        "instance_name": "Dev 1",
    }
    assert resolution_to_json(None) is None


def test_validate_json_file_output(tmp_path, capsys):
    p = tmp_path / "recs.json"
    p.write_text(
        json.dumps(
            [
                _record().to_dict(),
                {"schema_version": 1, "service_type": "_x._tcp"},  # invalid
            ]
        )
    )
    # global --json precedes the subcommand
    assert main(["--json", "validate", "--file", str(p)]) == 1
    out = json.loads(capsys.readouterr().out)
    assert out[0] == {"index": 0, "valid": True, "error": None}
    assert out[1]["index"] == 1 and out[1]["valid"] is False
    assert out[1]["error"]  # a non-empty message


# --- snapshot / diff --------------------------------------------------------


def _rec(
    instance="Dev 1",
    service="_x._tcp",
    iface="eth0",
    addrs=(("192.168.1.10", "ipv4", "private"),),
    state="active",
    txt=None,
    first="2026-01-01T00:00:00Z",
    last="2026-01-01T00:05:00Z",
    stale=False,
    port=80,
    hostname="h.local",
):
    """An enriched record dict, shaped like record_to_debug_json output."""
    return {
        "schema_version": 1,
        "service_type": service,
        "instance_name": instance,
        "hostname": hostname,
        "interface": iface,
        "port": port,
        "addresses": [{"address": a, "family": f, "scope": s} for (a, f, s) in addrs],
        "txt": txt or {},
        "state": state,
        "first_seen": first,
        "last_seen": last,
        "ttl_seconds": 120,
        "age_seconds": 300.0,
        "is_stale": stale,
    }


def test_characterize_counts_and_shapes():
    recs = [
        _rec(
            instance="A",
            addrs=(("192.168.1.10", "ipv4", "private"), ("fe80::1", "ipv6", "link-local")),
        ),
        _rec(instance="B", service="_y._tcp", addrs=(("8.8.8.8", "ipv4", "global"),), stale=True),
    ]
    c = characterize(recs)
    assert c["service_types"] == 2
    assert c["instances"] == 2
    assert c["addresses"] == 3
    assert c["scopes"] == {"private": 1, "link-local": 1, "global": 1}
    assert c["families"] == {"ipv4": 2, "ipv6": 1}
    assert c["stale"] == 1
    assert c["size_bytes"] > 0
    assert c["oldest_first_seen"] == "2026-01-01T00:00:00Z"


def test_diff_ignores_volatile_timestamps():
    old = [_rec(instance="A", last="2026-01-01T00:05:00Z")]
    new = [_rec(instance="A", last="2026-01-02T09:00:00Z")]  # only last_seen moved
    d = diff_snapshots(old, new)
    assert d["changed"] == []
    assert d["unchanged"] == 1
    assert d["jaccard"] == 1.0


def test_diff_added_removed_changed():
    old = [_rec(instance="A"), _rec(instance="B"), _rec(instance="C")]
    new = [
        _rec(instance="A"),
        _rec(instance="C", addrs=(("192.168.1.99", "ipv4", "private"),)),  # address changed
        _rec(instance="D"),
    ]
    d = diff_snapshots(old, new)
    assert d["added"] == [["_x._tcp", "eth0", "D"]]
    assert d["removed"] == [["_x._tcp", "eth0", "B"]]
    assert len(d["changed"]) == 1
    changes = d["changed"][0]["changes"]
    assert any("+addr 192.168.1.99" in x for x in changes)
    assert any("-addr 192.168.1.10" in x for x in changes)


def test_verdict_buckets():
    same = [_rec(instance=str(i)) for i in range(10)]
    assert "kinda the same" in _verdict(
        characterize(same), characterize(same), diff_snapshots(same, same)
    )

    grew = same + [_rec(instance=str(i)) for i in range(10, 20)]
    assert "grew" in _verdict(characterize(same), characterize(grew), diff_snapshots(same, grew))

    disjoint = [_rec(instance="x" + str(i)) for i in range(10)]
    assert "substantially different" in _verdict(
        characterize(same), characterize(disjoint), diff_snapshots(same, disjoint)
    )


def test_snapshot_payload_and_load_roundtrip(tmp_path):
    payload = snapshot_payload(
        [_rec(instance="A")], base="local/mdns/discovery/v1", host="127.0.0.1", port=1883, now=NOW
    )
    assert payload["record_count"] == 1
    assert payload["captured_at"] == "2026-01-01T00:10:00Z"
    p = tmp_path / "snap.json"
    p.write_text(json.dumps(payload))
    meta, records = _load_snapshot(str(p))
    assert meta["base"] == "local/mdns/discovery/v1"
    assert meta["captured_at"] == "2026-01-01T00:10:00Z"
    assert len(records) == 1


def test_load_snapshot_accepts_bare_dump_array(tmp_path):
    p = tmp_path / "dump.json"
    p.write_text(json.dumps([_rec(instance="A")]))  # a bare `--json dump` array
    meta, records = _load_snapshot(str(p))
    assert meta == {}
    assert len(records) == 1


def test_cmd_diff_via_main(tmp_path, capsys):
    old = tmp_path / "old.json"
    new = tmp_path / "new.json"
    old.write_text(
        json.dumps(
            snapshot_payload(
                [_rec(instance="A"), _rec(instance="B")], base="b", host="h", port=1, now=NOW
            )
        )
    )
    new.write_text(
        json.dumps(
            snapshot_payload(
                [_rec(instance="A"), _rec(instance="C")], base="b", host="h", port=1, now=NOW
            )
        )
    )
    assert main(["diff", str(old), str(new)]) == 0
    assert "overlap" in capsys.readouterr().out
    assert main(["--json", "diff", str(old), str(new)]) == 0
    data = json.loads(capsys.readouterr().out)
    assert abs(data["diff"]["jaccard"] - 1 / 3) < 1e-9  # common {A}, union {A,B,C}


def test_characterize_per_interface_and_type():
    recs = [
        _rec(instance="A", iface="eth0", service="_http._tcp"),
        _rec(
            instance="B",
            iface="eth0",
            service="_http._tcp",
            addrs=(("192.168.1.11", "ipv4", "private"), ("fe80::2", "ipv6", "link-local")),
        ),
        _rec(instance="C", iface="wlan0", service="_airplay._tcp"),
    ]
    c = characterize(recs)
    assert c["by_interface"] == {
        "eth0": {"instances": 2, "addresses": 3},
        "wlan0": {"instances": 1, "addresses": 1},
    }
    # ordered by count (most common first)
    assert list(c["by_service_type"].items()) == [("_http._tcp", 2), ("_airplay._tcp", 1)]


def test_render_stats_smoke():
    recs = [_rec(instance="A", iface="eth0"), _rec(instance="B", iface="wlan0", service="_y._tcp")]
    out = render_stats(characterize(recs), {"base": "local/mdns/discovery/v1"})
    assert "discovery bus stats" in out
    assert "instances      2" in out
    assert "per interface:" in out
    assert "eth0" in out and "wlan0" in out
    assert "by service-type:" in out


def test_main_requires_subcommand():
    with pytest.raises(SystemExit):
        main([])
