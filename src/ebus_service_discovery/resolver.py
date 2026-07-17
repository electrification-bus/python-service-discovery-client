"""Subscribe to a v1 service-discovery bus and resolve a reachable address.

The resolver keeps a fresh in-memory view of active ``Record``s (dropping
tombstones) and resolves a target service to a reachable endpoint by ordering
candidate addresses routable-first and TCP-probing each. It also tracks the
publisher's Homie-style ``$state`` liveness (``bus_ready`` / ``publisher_state``),
the bus-level replacement for per-record TTL staleness: the records are worth
trusting only while the publisher is ``ready``. The probe binds to the
record's interface (``SO_BINDTODEVICE``) and connects to the address as-is, so
an unreachable IPv4 (for example an APIPA lease) is simply skipped in favor of a
working IPv6 -- no family-specific special-casing, and the reachability logic
lives here once for every consumer.
"""

from __future__ import annotations

import contextlib
import logging
import socket
import threading
from collections.abc import Callable
from dataclasses import dataclass
from urllib.parse import unquote

from ebus_service_discovery.record import (
    DEFAULT_TOPIC_BASE,
    Address,
    AddressFamily,
    Record,
)

logger = logging.getLogger(__name__)

_DEFAULT_PROBE_TIMEOUT = 5.0

_ViewKey = tuple[str, str, str]  # (service_type, interface, instance_name)


@dataclass(frozen=True)
class Resolution:
    """A reachable endpoint for a discovered service instance."""

    record: Record
    address: Address
    interface: str
    port: int

    @property
    def host(self) -> str:
        """Address in URL-host form: bracketed IPv6, zone-qualified link-local."""
        ip = self.address.address
        if self.address.family is AddressFamily.IPV4:
            return ip
        if self.address.is_link_local and "%" not in ip:
            return f"[{ip}%{self.interface}]"
        return f"[{ip}]"


class ServiceResolver:
    """A live view of a v1 discovery bus, with reachable-address resolution.

    Pass a started/connected ``ebus_mqtt_client.MqttClient``; the resolver only
    subscribes (the caller owns the connection lifecycle). Records can also be
    fed directly via :meth:`ingest` for testing or non-MQTT sources.
    """

    def __init__(
        self,
        mqtt_client,
        base: str = DEFAULT_TOPIC_BASE,
        *,
        probe_timeout: float = _DEFAULT_PROBE_TIMEOUT,
        prefer_family: AddressFamily | None = None,
        interface_priority: list[str] | None = None,
    ):
        self._mqtt = mqtt_client
        self._base = base.rstrip("/")
        self._probe_timeout = probe_timeout
        self._prefer_family = prefer_family
        self._interface_priority = list(interface_priority) if interface_priority else []
        self._lock = threading.RLock()
        self._records: dict[_ViewKey, Record] = {}
        self._watched: set[str] = set()
        self._publisher_state: str | None = None
        self._state_watched = False
        self._state_topic = f"{self._base}/$state"

    # --- subscription -----------------------------------------------------

    def watch(self, service_type: str) -> None:
        """Subscribe to every advertisement of ``service_type`` on the bus.

        Also ensures a subscription to the bus liveness topic ``{base}/$state``
        so ``publisher_state`` / ``bus_ready`` track the publisher.
        """
        self._watch_state()
        with self._lock:
            if service_type in self._watched:
                return
            self._watched.add(service_type)
        self._mqtt.subscribe(f"{self._base}/{service_type}/+/+", param=self._on_message)

    def _watch_state(self) -> None:
        """Subscribe to the ``{base}/$state`` liveness topic exactly once."""
        with self._lock:
            if self._state_watched:
                return
            self._state_watched = True
        self._mqtt.subscribe(self._state_topic, param=self._on_message)

    def _on_message(self, topic: str, payload: bytes | bytearray) -> None:
        if topic == self._state_topic:
            # {base}/$state is the publisher liveness signal, not a record. An
            # empty payload means the publisher cleared it (gone). Handle it
            # before the record path, which would otherwise log it unparseable.
            text = bytes(payload).decode("utf-8", "replace").strip() if payload else ""
            with self._lock:
                self._publisher_state = text or None
            return
        key = self._key_from_topic(topic)
        if key is None:
            logger.warning("reason=discoveryTopicUnparsed,topic=%s", topic)
            return
        if not payload or not bytes(payload).strip():
            self._remove(key)  # retained-clear tombstone (empty payload)
            return
        try:
            record = Record.from_json(bytes(payload))
        except Exception:
            logger.warning("reason=discoveryRecordUnparsed,topic=%s", topic, exc_info=True)
            return
        self.ingest(record)

    # --- view -------------------------------------------------------------

    def ingest(self, record: Record) -> None:
        """Apply a record to the view. A ``removed`` record drops its key."""
        key: _ViewKey = (record.service_type, record.interface, record.instance_name)
        with self._lock:
            if record.is_removed:
                self._records.pop(key, None)
            else:
                self._records[key] = record

    def _remove(self, key: _ViewKey) -> None:
        with self._lock:
            self._records.pop(key, None)

    def _key_from_topic(self, topic: str) -> _ViewKey | None:
        prefix = f"{self._base}/"
        if not topic.startswith(prefix):
            return None
        parts = topic[len(prefix) :].split("/")
        if len(parts) != 3 or not all(parts):
            return None
        service_type, interface, instance = parts
        return (service_type, interface, unquote(instance))

    def records(self, service_type: str | None = None) -> list[Record]:
        """Snapshot of the current active records, optionally filtered by type.

        Freshness is a bus-level property now (see ``bus_ready`` /
        ``publisher_state``), not a per-record one, so there is no stale filter.
        """
        with self._lock:
            recs = list(self._records.values())
        if service_type is not None:
            recs = [r for r in recs if r.service_type == service_type]
        return recs

    # --- publisher liveness ($state) --------------------------------------

    @property
    def publisher_state(self) -> str | None:
        """The publisher's last-seen Homie-style ``$state`` (``ready`` / ``init``
        / ``disconnected`` / ``lost``), or None if none seen or it was cleared."""
        with self._lock:
            return self._publisher_state

    @property
    def bus_ready(self) -> bool:
        """True iff the publisher is ``ready`` (live and maintaining the tree).

        Gate trust in the records on this: while not ready (init/disconnected/
        lost/unknown) the tree is either rebuilding or an unmaintained snapshot.
        A dead publisher sends no clears, so the view naturally keeps the
        last-known records; ``bus_ready`` tells the consumer not to trust them.
        """
        with self._lock:
            return self._publisher_state == "ready"

    # --- resolution -------------------------------------------------------

    def resolve(
        self,
        service_type: str,
        match: Callable[[Record], bool] | None = None,
        *,
        port: int | None = None,
    ) -> Resolution | None:
        """Return a reachable endpoint for the best-matching instance, or None.

        Candidates (across all matching records and interfaces) are ordered
        routable-first, then by interface priority, then by preferred family,
        and each is TCP-probed until one connects.
        """
        candidates: list[tuple[Record, Address]] = []
        for record in self.records(service_type):
            if match is not None and not match(record):
                continue
            for address in record.candidate_addresses():
                candidates.append((record, address))
        candidates.sort(key=lambda ra: self._sort_key(*ra))
        for record, address in candidates:
            use_port = record.port if port is None else port
            if self._tcp_reachable(record.interface, address, use_port):
                return Resolution(
                    record=record, address=address, interface=record.interface, port=use_port
                )
        return None

    def _sort_key(self, record: Record, address: Address) -> tuple[int, int, int]:
        family_rank = 0
        if self._prefer_family is not None:
            family_rank = 0 if address.family is self._prefer_family else 1
        if record.interface in self._interface_priority:
            interface_rank = self._interface_priority.index(record.interface)
        else:
            interface_rank = len(self._interface_priority)
        return (address.preference, interface_rank, family_rank)

    def _tcp_reachable(self, interface: str, address: Address, port: int) -> bool:
        """TCP-connect to ``address:port`` bound to ``interface``.

        Binding uses ``SO_BINDTODEVICE`` (Linux); on platforms without it the
        bind is skipped so the probe still works for local development.
        """
        ip = address.address.split("%", 1)[0]
        sock = None
        try:
            if address.family is AddressFamily.IPV6:
                # Link-local (fe80::/10) needs the interface scope id; global does not.
                # if_nametoindex() is inside the try so a vanished interface skips this
                # candidate (returns False) rather than aborting the whole resolve().
                scope = socket.if_nametoindex(interface) if address.is_link_local else 0
                family, sockaddr = socket.AF_INET6, (ip, port, 0, scope)
            else:
                family, sockaddr = socket.AF_INET, (ip, port)
            sock = socket.socket(family, socket.SOCK_STREAM)
            with contextlib.suppress(AttributeError):
                # SO_BINDTODEVICE is Linux-only; on other platforms probe unbound. A
                # bind failure on Linux (EPERM/ENODEV) is NOT suppressed: it falls to
                # the OSError handler and reports unreachable, since we then cannot
                # guarantee egress on the intended interface.
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_BINDTODEVICE, interface.encode())
            sock.settimeout(self._probe_timeout)
            sock.connect(sockaddr)
            return True
        except OSError as exc:
            logger.debug(
                "reason=probeFailed,interface=%s,addr=%s,port=%s,err=%s", interface, ip, port, exc
            )
            return False
        finally:
            if sock is not None:
                sock.close()
