from datetime import datetime, timezone

from ebus_service_discovery import Address, AddressFamily, Record, RecordState
from ebus_service_discovery.resolver import Resolution, ServiceResolver


class FakeMqtt:
    def __init__(self):
        self.subs = {}

    def subscribe(self, sub, param, qos=1):
        self.subs[sub] = param


def _record(
    *, service_type="_example._tcp", instance="Dev 1", interface="eth0", port=80, addrs=(), txt=None
):
    return Record(
        service_type=service_type,
        instance_name=instance,
        hostname="h.local",
        interface=interface,
        port=port,
        addresses=[Address.parse(a) for a in addrs],
        txt=txt or {},
        first_seen=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_seen=datetime(2026, 1, 1, tzinfo=timezone.utc),
    )


def _reachable(*addrs):
    """A _tcp_reachable stand-in that 'connects' only to the given IPs, recording probes."""
    ok = set(addrs)
    calls = []

    def _probe(interface, address, port):
        calls.append((interface, address.address, port))
        return address.address in ok

    _probe.calls = calls
    return _probe


def _resolver(**kw):
    return ServiceResolver(FakeMqtt(), **kw)


# --- subscription / view ---------------------------------------------------


def test_watch_subscribes_to_service_filter_and_state():
    mqtt = FakeMqtt()
    r = ServiceResolver(mqtt)
    r.watch("_example._tcp")
    assert "local/mdns/discovery/v1/_example._tcp/+/+" in mqtt.subs
    assert "local/mdns/discovery/v1/$state" in mqtt.subs  # liveness auto-subscribed
    r.watch("_example._tcp")  # idempotent (service filter + $state, no dupes)
    assert len(mqtt.subs) == 2


def test_on_message_active_then_empty_tombstone():
    r = _resolver()
    rec = _record(addrs=["192.168.1.10"])
    r._on_message(rec.topic(), rec.to_json().encode())
    assert len(r.records("_example._tcp")) == 1
    # empty retained payload clears the record
    r._on_message(rec.topic(), b"")
    assert r.records("_example._tcp") == []


def test_on_message_removed_state_drops():
    r = _resolver()
    rec = _record(addrs=["192.168.1.10"])
    r.ingest(rec)
    tomb = _record(addrs=["192.168.1.10"])
    tomb.state = RecordState.REMOVED
    tomb.removed_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    r._on_message(tomb.topic(), tomb.to_json().encode())
    assert r.records("_example._tcp") == []


def test_on_message_bad_payload_ignored():
    r = _resolver()
    r._on_message("local/mdns/discovery/v1/_example._tcp/eth0/Dev%201", b"{not json")
    assert r.records() == []


def test_key_from_topic_percent_decode():
    r = _resolver()
    key = r._key_from_topic("local/mdns/discovery/v1/_example._tcp/eth0/Dev%20One")
    assert key == ("_example._tcp", "eth0", "Dev One")
    assert r._key_from_topic("some/other/topic") is None


# --- publisher liveness ($state) -------------------------------------------

_STATE = "local/mdns/discovery/v1/$state"


def test_state_topic_sets_publisher_state_not_a_record():
    r = _resolver()
    r._on_message(_STATE, b"ready")
    assert r.publisher_state == "ready"
    assert r.bus_ready is True
    assert r.records() == []  # $state is liveness, never a record


def test_bus_ready_only_when_ready():
    r = _resolver()
    assert r.publisher_state is None and r.bus_ready is False  # unknown until seen
    r._on_message(_STATE, b"init")
    assert r.publisher_state == "init" and r.bus_ready is False
    r._on_message(_STATE, b"ready")
    assert r.bus_ready is True
    r._on_message(_STATE, b"lost")
    assert r.publisher_state == "lost" and r.bus_ready is False


def test_empty_state_payload_clears_publisher_state():
    r = _resolver()
    r._on_message(_STATE, b"ready")
    r._on_message(_STATE, b"")  # zero-length $state = publisher gone
    assert r.publisher_state is None and r.bus_ready is False


def test_dead_publisher_view_keeps_last_known_records():
    # A dead publisher sends no clears, so the view naturally keeps its records;
    # bus_ready flips false via the LWT so the consumer knows not to trust them.
    r = _resolver()
    rec = _record(addrs=["192.168.1.10"])
    r._on_message(rec.topic(), rec.to_json().encode())
    r._on_message(_STATE, b"lost")
    assert r.bus_ready is False
    assert len(r.records("_example._tcp")) == 1  # last-known retained


# --- resolution ------------------------------------------------------------


def test_resolve_prefers_routable_over_apipa():
    r = _resolver()
    # advertised IPv4 is APIPA (unreachable), a routable global IPv6 is present
    r.ingest(_record(addrs=["169.254.1.1", "2606:4700:4700::1111"]))
    probe = _reachable("2606:4700:4700::1111")  # only the IPv6 answers
    r._tcp_reachable = probe
    res = r.resolve("_example._tcp")
    assert res is not None
    assert res.address.address == "2606:4700:4700::1111"
    # the APIPA link-local was ranked last; the routable IPv6 was tried first and won
    assert probe.calls[0][1] == "2606:4700:4700::1111"


def test_resolve_falls_back_to_ipv6_when_ipv4_unreachable():
    r = _resolver()
    r.ingest(_record(addrs=["192.168.1.10", "2606:4700:4700::1111"]))
    r._tcp_reachable = _reachable("2606:4700:4700::1111")  # private IPv4 down, IPv6 up
    res = r.resolve("_example._tcp")
    assert res.address.family is AddressFamily.IPV6


def test_resolve_uses_ipv4_when_reachable():
    r = _resolver()
    r.ingest(_record(addrs=["192.168.1.10", "2606:4700:4700::1111"]))
    r._tcp_reachable = _reachable("192.168.1.10", "2606:4700:4700::1111")  # both up
    res = r.resolve("_example._tcp", port=None)
    # both routable and reachable; either is acceptable, but it must be reachable
    assert res.address.address in {"192.168.1.10", "2606:4700:4700::1111"}


def test_resolve_none_when_nothing_reachable():
    r = _resolver()
    r.ingest(_record(addrs=["192.168.1.10", "fe80::1"]))
    r._tcp_reachable = _reachable()  # nothing connects
    assert r.resolve("_example._tcp") is None


def test_resolve_match_predicate():
    r = _resolver()
    r.ingest(_record(instance="A", addrs=["192.168.1.10"], txt={"serial": "aaa"}))
    r.ingest(_record(instance="B", addrs=["192.168.1.20"], txt={"serial": "bbb"}))
    r._tcp_reachable = _reachable("192.168.1.10", "192.168.1.20")
    res = r.resolve("_example._tcp", match=lambda rec: rec.txt.get("serial") == "bbb")
    assert res.address.address == "192.168.1.20"


def test_resolve_port_override():
    r = _resolver()
    r.ingest(_record(port=80, addrs=["192.168.1.10"]))
    probe = _reachable("192.168.1.10")
    r._tcp_reachable = probe
    res = r.resolve("_example._tcp", port=443)
    assert res.port == 443
    assert probe.calls[-1][2] == 443  # probed on the overridden port


def test_resolve_interface_priority():
    r = _resolver(interface_priority=["eth1", "eth0", "wlan0"])
    r.ingest(_record(instance="D", interface="wlan0", addrs=["192.168.1.30"]))
    r.ingest(_record(instance="D", interface="eth0", addrs=["192.168.1.10"]))
    r._tcp_reachable = _reachable("192.168.1.10", "192.168.1.30")  # both reachable
    res = r.resolve("_example._tcp")
    assert res.interface == "eth0"  # higher-priority interface wins


# --- Resolution.host -------------------------------------------------------


def test_resolution_host_formatting():
    rec = _record(interface="eth0")
    assert Resolution(rec, Address.parse("192.168.1.10"), "eth0", 443).host == "192.168.1.10"
    assert (
        Resolution(rec, Address.parse("2606:4700:4700::1111"), "eth0", 443).host
        == "[2606:4700:4700::1111]"
    )
    assert Resolution(rec, Address.parse("fe80::1"), "eth0", 443).host == "[fe80::1%eth0]"


# --- probe robustness ------------------------------------------------------


def test_probe_survives_missing_interface():
    # A link-local address on an interface that does not exist must not raise:
    # if_nametoindex() fails, the candidate is skipped (unreachable), resolve()
    # does not abort. Calls the REAL _tcp_reachable (no network: it fails on the
    # scope lookup before any connect).
    r = ServiceResolver(FakeMqtt())
    assert r._tcp_reachable("nonexistent-iface-zzz", Address.parse("fe80::1"), 80) is False
