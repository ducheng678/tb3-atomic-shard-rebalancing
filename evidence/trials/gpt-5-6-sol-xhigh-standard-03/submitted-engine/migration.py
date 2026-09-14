"""Recoverable legacy migration implemented with transferable live state.

The public migrate API remains unchanged.  Internally it now uses the same
epoch-fenced responsibility image as a rebalance, so PREPARED transactions and
active read locks no longer have to settle before ownership can move.
"""
from . import records as r
from . import transfer


MESSAGE_TYPES = {"MIGRATE_TRANSFER", "MIGRATE_TRANSFER_READY", "MIGRATE_ACTIVE",
                 "MIGRATE_INSTALL", "MIGRATE_STORED"}


def _make_move(shard, migration_id, source, target, source_epoch, complete=False):
    return {
        "v": 2, "kind": "LegacyMove", "migration_id": migration_id,
        "shard": shard, "source": source, "target": target,
        "expected": {"owner": source, "epoch": source_epoch,
                     "migration_id": None},
        "replacement": {"owner": target, "epoch": source_epoch + 1,
                        "migration_id": migration_id},
        "complete": bool(complete),
    }


def upgrade(node):
    """Index every legal public v1 migration record without changing meaning."""
    for old in list(node.state.get("migrations", [])):
        current = r.find(node.state, "legacy_ops", migration_id=old["migration_id"])
        candidate = _make_move(old["shard"], old["migration_id"], old["source"],
                               old["target"], old["source_epoch"],
                               old["state"] == "COMPLETE")
        if current is None:
            r.put(node.state, "legacy_ops", candidate)
        else:
            for field in ("shard", "source", "target", "expected", "replacement"):
                if current[field] != candidate[field]:
                    raise ValueError("v1 migration contradicts private migration index")
            if candidate["complete"] and not current["complete"]:
                current["complete"] = True
                r.put(node.state, "legacy_ops", current)


def current(node, shard):
    return next((move for move in node.state["legacy_ops"]
                 if move["shard"] == shard and not move["complete"]), None)


def frozen(node, shard):
    epoch = node.state["shard_epochs"].get(shard)
    if epoch is None:
        return False
    for item in node.state["transfers"]:
        if (item["role"] == "SOURCE" and item["shard"] == shard
                and item["source"] == node.id and item["source_epoch"] == epoch):
            return True
    for plan in node.state["rebalance_plans"]:
        if shard in plan["expected"]:
            expected = plan["expected"][shard]
            if (not plan["complete"] and expected["owner"] == node.id
                    and expected["epoch"] == epoch):
                return True
    for move in node.state["legacy_ops"]:
        if (move["shard"] == shard and not move["complete"]
                and move["source"] == node.id
                and move["expected"]["epoch"] == epoch):
            return True
    return False


def admits(node, shard):
    return not frozen(node, shard)


def client_admits(node, shard):
    return admits(node, shard)


def _reply(node, move):
    for token in node.migration_waiters.pop(move["migration_id"], set()):
        node.reply(token, {"shard": move["shard"], "target": move["target"],
                           "status": "COMPLETE",
                           "epoch": move["replacement"]["epoch"]})


def _ensure_source(node, move):
    if node.id != move["source"]:
        return False
    _, changed = transfer.source_record(
        node, "legacy", move["migration_id"], move["shard"], move["source"],
        move["target"], move["expected"]["epoch"], move["replacement"]["epoch"])
    return changed


def _send_install(node, move):
    outgoing = transfer.source(node, "legacy", move["migration_id"], move["shard"])
    if outgoing is not None:
        node.send(move["target"], {"type": "MIGRATE_TRANSFER",
                                   "move": r.clone(move),
                                   "transfer": r.clone(outgoing)})


def _v1_target_ready(node, move):
    row = r.find(node.state, "migrations", shard=move["shard"],
                 migration_id=move["migration_id"])
    return (row is not None and row["role"] == "TARGET"
            and row["state"] in {"SETTLED", "COMPLETE"}
            and row["target"] == node.id)


def _finish(node, move):
    if not move["complete"]:
        move["complete"] = True
        r.put(node.state, "legacy_ops", move)
        yield from node.save()
        move = r.find(node.state, "legacy_ops", migration_id=move["migration_id"])
    _reply(node, move)
    peer = move["source"] if node.id == move["target"] else move["target"]
    if peer != node.id:
        node.send(peer, {"type": "MIGRATE_ACTIVE", "shard": move["shard"],
                         "migration_id": move["migration_id"]})


def drive(node, move):
    move = r.find(node.state, "legacy_ops", migration_id=move["migration_id"])
    if move["complete"]:
        _reply(node, move)
        return
    owner = yield from node.owner(move["shard"])
    if owner == move["replacement"] or owner["epoch"] > move["replacement"]["epoch"]:
        yield from _finish(node, move)
        return
    if owner != move["expected"]:
        return
    if _ensure_source(node, move):
        yield from node.save()
        move = r.find(node.state, "legacy_ops", migration_id=move["migration_id"])
    _send_install(node, move)
    installed = transfer.target(node, "legacy", move["migration_id"], move["shard"])
    if node.id == move["target"] and (installed is not None or _v1_target_ready(node, move)):
        result = yield from node.io("cas_owner", move["shard"], move["expected"],
                                   move["replacement"])
        if result["record"] == move["replacement"]:
            yield from _finish(node, move)


def client(node, event):
    request, migration_id = event["request"], event["migration_id"]
    move = r.find(node.state, "legacy_ops", migration_id=migration_id)
    owner = yield from node.owner(request["shard"])
    new_move = False
    if move is None:
        if owner["owner"] != node.id:
            node.reply(event["request_token"], {"shard": request["shard"],
                       "target": request["target"], "status": "UNKNOWN"})
            return
        move = _make_move(request["shard"], migration_id, node.id,
                          request["target"], owner["epoch"])
        r.put(node.state, "legacy_ops", move)
        new_move = True
        # Retain the public v1 registration shape for diagnostic compatibility.
        if r.find(node.state, "migrations", shard=request["shard"],
                  migration_id=migration_id) is None:
            public = r.record(
                "Migration", shard=request["shard"], migration_id=migration_id,
                source=node.id, target=request["target"], source_epoch=owner["epoch"],
                target_epoch=owner["epoch"] + 1, role="SOURCE", state="DRAINING",
                export_revision=None, export_digest=None, settled_image=None)
            r.put(node.state, "migrations", public)
    if move["shard"] != request["shard"] or move["target"] != request["target"]:
        raise ValueError("migration identity was reused")
    node.migration_waiters.setdefault(migration_id, set()).add(event["request_token"])
    changed = _ensure_source(node, move)
    if new_move or changed:
        yield from node.save()
        move = r.find(node.state, "legacy_ops", migration_id=migration_id)
    yield from drive(node, move)


def _install_transfer(node, source, body):
    incoming_move = body["move"]
    move = r.find(node.state, "legacy_ops", migration_id=incoming_move["migration_id"])
    newly_added = False
    if move is None:
        r.put(node.state, "legacy_ops", incoming_move)
        move = r.find(node.state, "legacy_ops", migration_id=incoming_move["migration_id"])
        newly_added = True
    for field in ("shard", "source", "target", "expected", "replacement"):
        if move[field] != incoming_move[field]:
            raise ValueError("migration transfer disagrees with registration")
    if source != move["source"] or node.id != move["target"]:
        return
    outcome = yield from transfer.install(node, body["transfer"])
    if outcome != "INSTALLED" and newly_added:
        yield from node.save()
    if outcome == "INSTALLED":
        installed = transfer.target(node, "legacy", move["migration_id"], move["shard"])
        node.send(source, {"type": "MIGRATE_TRANSFER_READY", "shard": move["shard"],
                           "migration_id": move["migration_id"],
                           "image_digest": installed["image_digest"]})
    yield from drive(node, r.find(node.state, "legacy_ops",
                                  migration_id=move["migration_id"]))


def _v1_transfer(node, source, incoming):
    if (incoming.get("role") != "SOURCE" or incoming.get("state") != "SETTLED"
            or source != incoming.get("source") or node.id != incoming.get("target")
            or incoming.get("export_digest") != r.digest(incoming.get("settled_image"))):
        return
    move = _make_move(incoming["shard"], incoming["migration_id"], incoming["source"],
                      incoming["target"], incoming["source_epoch"])
    if r.find(node.state, "legacy_ops", migration_id=move["migration_id"]) is None:
        r.put(node.state, "legacy_ops", move)
    old_image = incoming["settled_image"]
    cut = {
        "format_version": 2, "shard": move["shard"],
        "epoch": move["expected"]["epoch"], "values": r.clone(old_image["values"]),
        "txn_begins": r.clone(old_image["txn_begins"]), "intents": [],
        "applied": r.clone(old_image["applied"]), "aborted": r.clone(old_image["aborted"]),
        "read_grants": [], "read_released": r.clone(old_image["read_released"]),
        "coord_reads": r.clone(old_image["coord_reads"]),
        "decisions": r.clone(old_image["decisions"]), "rebalance_plans": [],
        "legacy_ops": [r.clone(move)],
    }
    exported = {
        "v": 2, "kind": "Transfer", "operation_kind": "legacy",
        "operation_id": move["migration_id"], "shard": move["shard"],
        "source": move["source"], "target": move["target"],
        "source_epoch": move["expected"]["epoch"],
        "target_epoch": move["replacement"]["epoch"], "role": "SOURCE",
        "state": "EXPORTED", "image_digest": r.digest(cut), "image": cut,
    }
    target_row = r.clone(incoming)
    target_row["role"] = "TARGET"
    r.put(node.state, "migrations", target_row)
    yield from transfer.install(node, exported)
    node.send(source, {"type": "MIGRATE_STORED", "shard": move["shard"],
                       "migration_id": move["migration_id"],
                       "export_digest": incoming["export_digest"]})
    yield from drive(node, r.find(node.state, "legacy_ops",
                                  migration_id=move["migration_id"]))


def message(node, source, body):
    name = body["type"]
    if name == "MIGRATE_TRANSFER":
        yield from _install_transfer(node, source, body)
        return
    if name == "MIGRATE_INSTALL":
        yield from _v1_transfer(node, source, body["migration"])
        return
    move = r.find(node.state, "legacy_ops", migration_id=body["migration_id"])
    if move is None or body.get("shard") != move["shard"]:
        return
    if name in {"MIGRATE_TRANSFER_READY", "MIGRATE_STORED"}:
        if source != move["target"] or node.id != move["source"]:
            return
        if name == "MIGRATE_TRANSFER_READY":
            outgoing = transfer.source(node, "legacy", move["migration_id"], move["shard"])
            if outgoing is None or outgoing["image_digest"] != body["image_digest"]:
                return
        result = yield from node.io("cas_owner", move["shard"], move["expected"],
                                   move["replacement"])
        if result["record"] == move["replacement"]:
            yield from _finish(node, move)
        return
    if name == "MIGRATE_ACTIVE" and source in {move["source"], move["target"]}:
        owner = yield from node.owner(move["shard"])
        if owner == move["replacement"]:
            yield from _finish(node, move)


def tick(node):
    for move in list(node.state["legacy_ops"]):
        if not move["complete"]:
            yield from drive(node, move)
