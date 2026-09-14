"""Logical-shard responsibility images used by both handoff protocols.

Unlike the public baseline ``SettledImage``, this private image deliberately
contains unresolved locks.  Installing it transfers the *same* transaction
and read identities; it never treats physical values as a new logical base.
"""
from . import records as r


PARTICIPANT_TABLES = ("intents", "applied", "aborted",
                      "read_grants", "read_released")


def _descriptor(plan):
    return {
        "plan_id": plan["plan_id"],
        "expected": r.clone(plan["expected"]),
        "replacement": r.clone(plan["replacement"]),
        "state": plan["state"],
    }


def _plan(node, plan_id):
    return next((row for row in node.extension["plans"]
                 if row["plan_id"] == plan_id), None)


def merge_history(node, descriptor):
    """Merge a compact historical plan description into local durable state."""
    existing = _plan(node, descriptor["plan_id"])
    if existing is None:
        node.extension["plans"].append({
            **r.clone(descriptor), "exports": {}, "installed": {}, "ready": []})
        return
    if (existing["expected"] != descriptor["expected"]
            or existing["replacement"] != descriptor["replacement"]):
        raise ValueError("rebalance identity names incompatible owner vectors")
    if descriptor["state"] == "COMPLETE":
        existing["state"] = "COMPLETE"


def capture(node, shard):
    """Capture all durable responsibility belonging to one logical shard."""
    coordinator = {"type": "shard", "id": shard}
    begins = [row for row in node.state["txn_begins"]
              if row["coordinator"] == coordinator]
    coord_reads = [row for row in node.state["coord_reads"]
                   if row["coordinator"] == coordinator]
    participant = {
        table: [row for row in node.state[table] if row["shard"] == shard]
        for table in PARTICIPANT_TABLES
    }
    txn_ids = {row["txn_id"] for row in begins}
    for table in ("intents", "applied", "aborted"):
        txn_ids.update(row["txn_id"] for row in participant[table])
    decisions = [r.clone(node.decisions[txn_id]) for txn_id in sorted(txn_ids)
                 if txn_id in node.decisions]
    histories = [_descriptor(plan) for plan in node.extension["plans"]
                 if shard in plan["expected"]]
    return {
        "format_version": 2,
        "shard": shard,
        "values": r.clone(node.state["values"][shard]),
        "txn_begins": r.ordered("txn_begins", begins),
        "decisions": decisions,
        "intents": r.ordered("intents", participant["intents"]),
        "applied": r.ordered("applied", participant["applied"]),
        "aborted": r.ordered("aborted", participant["aborted"]),
        "read_grants": r.ordered("read_grants", participant["read_grants"]),
        "read_released": r.ordered("read_released", participant["read_released"]),
        "coord_reads": r.ordered("coord_reads", coord_reads),
        "migrations": r.ordered("migrations", [row for row in
                                  node.state["migrations"] if row["shard"] == shard]),
        "rebalance_history": histories,
    }


def _remove_where(rows, predicate):
    rows[:] = [row for row in rows if not predicate(row)]


def install(node, image):
    """Replace one stale local shard copy with an authoritative live image.

    The caller persists this state and ``image['decisions']`` in one batch.
    Existing append-only public facts are intentionally retained by the host.
    """
    if image.get("format_version") != 2:
        raise ValueError("unsupported live shard image")
    shard = image["shard"]
    coordinator = {"type": "shard", "id": shard}

    node.state["values"][shard] = r.clone(image["values"])
    for table in PARTICIPANT_TABLES:
        _remove_where(node.state[table], lambda row, s=shard: row["shard"] == s)
        for row in image[table]:
            r.put(node.state, table, row)

    _remove_where(node.state["txn_begins"],
                  lambda row, c=coordinator: row["coordinator"] == c)
    for row in image["txn_begins"]:
        r.put(node.state, "txn_begins", row)
        node.txns.pop(row["txn_id"], None)

    _remove_where(node.state["coord_reads"],
                  lambda row, c=coordinator: row["coordinator"] == c)
    for row in image["coord_reads"]:
        r.put(node.state, "coord_reads", row)
        node.reads.pop(row["read_id"], None)

    _remove_where(node.state["migrations"], lambda row, s=shard: row["shard"] == s)
    for row in image["migrations"]:
        r.put(node.state, "migrations", row)
    for descriptor in image["rebalance_history"]:
        merge_history(node, descriptor)


def abandon_local_tokens(node, shard):
    """End attempts whose request tokens cannot move to another worker."""
    coordinator = {"type": "shard", "id": shard}
    for begin in list(node.state["txn_begins"]):
        if begin["coordinator"] != coordinator:
            continue
        txn_id = begin["txn_id"]
        tokens = node.txn_waiters.pop(txn_id, set())
        for token in tokens:
            fact = node.decisions.get(txn_id)
            status = {"COMMIT": "COMMITTED", "ABORT": "ABORTED"}.get(
                fact["outcome"] if fact else None, "UNKNOWN")
            node.reply(token, {"txn_id": txn_id, "status": status})
    for read in list(node.state["coord_reads"]):
        if read["coordinator"] != coordinator:
            continue
        context = node.reads.get(read["read_id"])
        if context is None:
            continue
        for token in context["tokens"]:
            node.reply(token, {"read_id": read["read_id"], "status": "UNKNOWN"})
        context["tokens"].clear()
