"""Recoverable atomic rebalancing of A/B over the reusable data-worker pool."""
from . import handoff
from . import records as r


MESSAGE_TYPES = {
    "REBALANCE_BEGIN", "REBALANCE_INSTALL", "REBALANCE_READY",
    "REBALANCE_PROBE", "REBALANCE_COMPLETE",
}


def find(node, plan_id):
    return next((row for row in node.extension["plans"]
                 if row["plan_id"] == plan_id), None)


def descriptor(row):
    return {"plan_id": row["plan_id"], "expected": r.clone(row["expected"]),
            "replacement": r.clone(row["replacement"]), "state": row["state"]}


def members(row):
    return {record["owner"] for vector in (row["expected"], row["replacement"])
            for record in vector.values()}


def source_frozen(node, shard):
    return any(plan["state"] == "ACTIVE" and shard in plan["expected"]
               and plan["expected"][shard]["owner"] == node.id
               for plan in node.extension["plans"])


def coordinator_frozen(node, identity):
    return identity["type"] == "shard" and source_frozen(node, identity["id"])


def _new(desc):
    return {"plan_id": desc["plan_id"], "expected": r.clone(desc["expected"]),
            "replacement": r.clone(desc["replacement"]),
            "state": desc.get("state", "ACTIVE"),
            "exports": {}, "installed": {}, "ready": []}


def ensure(node, desc):
    row = find(node, desc["plan_id"])
    if row is not None:
        if (row["expected"] != desc["expected"]
                or row["replacement"] != desc["replacement"]):
            raise ValueError("rebalance plan identity was reused")
        return row
    row = _new(desc)
    node.extension["plans"].append(row)
    yield from node.save()
    return row


def _reply_complete(node, row):
    for token in node.rebalance_waiters.pop(row["plan_id"], set()):
        node.reply(token, {"plan_id": row["plan_id"], "status": "COMPLETE",
                           "owners": r.clone(row["replacement"])})


def _mark_complete(node, row):
    if row["state"] != "COMPLETE":
        row["state"] = "COMPLETE"
        yield from node.save()
    _reply_complete(node, row)


def _broadcast(node, row, body):
    for peer in members(row):
        if peer != node.id:
            node.send(peer, body)


def _actual(node, row):
    result = yield from node.io("read_owners", sorted(row["expected"]))
    return result["records"]


def drive(node, row, announce=True):
    if row["state"] == "COMPLETE":
        _reply_complete(node, row)
        return
    actual = yield from _actual(node, row)
    # Owner epochs never go backwards.  Thus any departure from this plan's
    # registered expected vector proves its one atomic CAS happened, even when
    # later plans have already changed the vector again.
    if actual != row["expected"]:
        yield from _mark_complete(node, row)
        _broadcast(node, row, {"type": "REBALANCE_COMPLETE",
                               "plan": descriptor(row)})
        return

    desc = descriptor(row)
    if announce:
        _broadcast(node, row, {"type": "REBALANCE_BEGIN", "plan": desc})

    changed = False
    for shard in sorted(row["expected"]):
        if row["expected"][shard]["owner"] != node.id or shard in row["exports"]:
            continue
        handoff.abandon_local_tokens(node, shard)
        image = handoff.capture(node, shard)
        row["exports"][shard] = {"digest": r.digest(image), "image": image}
        changed = True
    if changed:
        yield from node.save()

    for shard, export in list(row["exports"].items()):
        node.send(row["replacement"][shard]["owner"], {
            "type": "REBALANCE_INSTALL", "plan": descriptor(row),
            "shard": shard, "export": export})

    locally_ready = {shard for shard in row["installed"]
                     if row["replacement"][shard]["owner"] == node.id}
    new_ready = sorted(set(row["ready"]) | locally_ready)
    if new_ready != row["ready"]:
        row["ready"] = new_ready
        yield from node.save()

    for shard in sorted(row["expected"]):
        if shard not in row["ready"]:
            node.send(row["replacement"][shard]["owner"], {
                "type": "REBALANCE_PROBE", "plan": descriptor(row),
                "shard": shard})

    if set(row["ready"]) == set(row["expected"]):
        result = yield from node.io("cas_owners", r.clone(row["expected"]),
                                    r.clone(row["replacement"]))
        if result["records"] != row["expected"]:
            yield from _mark_complete(node, row)
            _broadcast(node, row, {"type": "REBALANCE_COMPLETE",
                                   "plan": descriptor(row)})


def client(node, event):
    metadata = event["rebalance"]
    desc = {"plan_id": metadata["plan_id"],
            "expected": r.clone(metadata["expected"]),
            "replacement": r.clone(metadata["replacement"]), "state": "ACTIVE"}
    row = yield from ensure(node, desc)
    node.rebalance_waiters.setdefault(row["plan_id"], set()).add(
        event["request_token"])
    yield from drive(node, row)


def _valid_sender(row, source):
    return source in members(row)


def _ready(node, source, row, body):
    shard = body["shard"]
    if (shard not in row["expected"]
            or source != row["replacement"][shard]["owner"]):
        return
    local = row["installed"].get(shard)
    # At the target, a READY is also accepted only for its own installation.
    if source == node.id and local != body["digest"]:
        return
    if shard not in row["ready"]:
        row["ready"] = sorted(set(row["ready"]) | {shard})
        yield from node.save()
    yield from drive(node, row, announce=False)


def _install(node, source, row, body):
    shard, export = body["shard"], body["export"]
    if (shard not in row["expected"]
            or source != row["expected"][shard]["owner"]
            or node.id != row["replacement"][shard]["owner"]
            or export.get("digest") != r.digest(export.get("image"))
            or export["image"].get("shard") != shard):
        return
    actual = yield from _actual(node, row)
    if actual != row["expected"]:
        # A delayed installation from a completed historical plan must never
        # overwrite work installed by a later plan.
        yield from _mark_complete(node, row)
        return
    previous = row["installed"].get(shard)
    if previous is not None:
        if previous != export["digest"]:
            raise ValueError("one rebalance installation changed its image")
    else:
        # Registration of this plan proves every other locally-known active plan
        # is older and had already returned COMPLETE.
        for old in node.extension["plans"]:
            if old is not row and old["state"] == "ACTIVE":
                old["state"] = "COMPLETE"
        handoff.install(node, export["image"])
        row = find(node, row["plan_id"])
        row["installed"][shard] = export["digest"]
        yield from node.save(export["image"]["decisions"])
    digest = row["installed"][shard]
    message = {"type": "REBALANCE_READY", "plan": descriptor(row),
               "shard": shard, "digest": digest}
    _broadcast(node, row, message)
    yield from _ready(node, node.id, row, message)


def _probe(node, source, row, body):
    shard = body["shard"]
    if (shard not in row["expected"]
            or node.id != row["replacement"][shard]["owner"]):
        return
    digest = row["installed"].get(shard)
    if digest is not None:
        node.send(source, {"type": "REBALANCE_READY", "plan": descriptor(row),
                           "shard": shard, "digest": digest})


def message(node, source, body):
    if body.get("type") not in MESSAGE_TYPES or not isinstance(body.get("plan"), dict):
        return
    row = yield from ensure(node, body["plan"])
    if not _valid_sender(row, source):
        return
    name = body["type"]
    if name == "REBALANCE_INSTALL":
        yield from _install(node, source, row, body)
    elif name == "REBALANCE_READY":
        yield from _ready(node, source, row, body)
    elif name == "REBALANCE_PROBE":
        _probe(node, source, row, body)
    else:
        # BEGIN and COMPLETE are both hints; authoritative metadata determines
        # whether the registered atomic transition has occurred.
        yield from drive(node, row, announce=False)


def tick(node):
    for row in list(node.extension["plans"]):
        if row["state"] != "COMPLETE":
            yield from drive(node, row)
