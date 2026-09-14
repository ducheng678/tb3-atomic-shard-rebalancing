"""Epoch-fenced transfer of one logical shard's complete responsibility.

Unlike the baseline settled image, a v2 image deliberately contains prepared
intents and live read grants.  A source becomes immutable at that epoch before
the image is sent.  The destination installs the image and its public decision
facts in one local transaction before ownership can be published.
"""
from . import records as r


PARTICIPANT_TABLES = ("intents", "applied", "aborted", "read_grants",
                      "read_released")


def _logical_coordinator(shard):
    return {"type": "shard", "id": shard}


def _plan_copy(plan):
    return {key: r.clone(plan[key]) for key in
            ("v", "kind", "plan_id", "expected", "replacement", "leader",
             "ready", "complete")}


def _legacy_copy(move):
    return {key: r.clone(move[key]) for key in
            ("v", "kind", "migration_id", "shard", "source", "target",
             "expected", "replacement", "complete")}


def image(node, shard, epoch):
    identity = _logical_coordinator(shard)
    begins = [row for row in node.state["txn_begins"]
              if row["coordinator"] == identity]
    coord_reads = [row for row in node.state["coord_reads"]
                   if row["coordinator"] == identity]
    participant = {table: [row for row in node.state[table]
                           if row["shard"] == shard]
                   for table in PARTICIPANT_TABLES}
    txn_ids = {row["txn_id"] for row in begins}
    for table in ("intents", "applied", "aborted"):
        txn_ids.update(row["txn_id"] for row in participant[table])
    facts = [r.clone(node.decisions[txn_id]) for txn_id in txn_ids
             if txn_id in node.decisions]
    facts.sort(key=lambda fact: r.canonical(
        [fact["txn_id"], fact["request_digest"], fact["outcome"]]))
    plans = [_plan_copy(plan) for plan in node.state["rebalance_plans"]
             if shard in plan["expected"]]
    plans.sort(key=lambda plan: r.canonical(plan["plan_id"]))
    legacy = [_legacy_copy(move) for move in node.state["legacy_ops"]
              if move["shard"] == shard]
    legacy.sort(key=lambda move: r.canonical(move["migration_id"]))
    return {
        "format_version": 2,
        "shard": shard,
        "epoch": epoch,
        "values": r.clone(node.state["values"][shard]),
        "txn_begins": r.ordered("txn_begins", begins),
        "intents": r.ordered("intents", participant["intents"]),
        "applied": r.ordered("applied", participant["applied"]),
        "aborted": r.ordered("aborted", participant["aborted"]),
        "read_grants": r.ordered("read_grants", participant["read_grants"]),
        "read_released": r.ordered("read_released", participant["read_released"]),
        "coord_reads": r.ordered("coord_reads", coord_reads),
        "decisions": facts,
        "rebalance_plans": plans,
        "legacy_ops": legacy,
    }


def source_record(node, operation_kind, operation_id, shard, source, target,
                  source_epoch, target_epoch):
    existing = r.find(node.state, "transfers", operation_kind=operation_kind,
                      operation_id=operation_id, shard=shard, role="SOURCE")
    if existing is not None:
        return existing, False
    if source != node.id or node.state["shard_epochs"].get(shard) != source_epoch:
        return None, False
    cut = image(node, shard, source_epoch)
    row = {
        "v": 2, "kind": "Transfer", "operation_kind": operation_kind,
        "operation_id": operation_id, "shard": shard, "source": source,
        "target": target, "source_epoch": source_epoch,
        "target_epoch": target_epoch, "role": "SOURCE", "state": "EXPORTED",
        "image_digest": r.digest(cut), "image": cut,
    }
    r.put(node.state, "transfers", row)
    return r.find(node.state, "transfers", operation_kind=operation_kind,
                  operation_id=operation_id, shard=shard, role="SOURCE"), True


def _merge_plan(state, incoming):
    current = r.find(state, "rebalance_plans", plan_id=incoming["plan_id"])
    if current is None:
        r.put(state, "rebalance_plans", incoming)
        return
    for field in ("expected", "replacement", "leader"):
        if current[field] != incoming[field]:
            raise ValueError("rebalance identity names incompatible owner vectors")
    current["ready"] = sorted(set(current["ready"]) | set(incoming["ready"]))
    current["complete"] = bool(current["complete"] or incoming["complete"])
    r.put(state, "rebalance_plans", current)


def _merge_legacy(state, incoming):
    current = r.find(state, "legacy_ops", migration_id=incoming["migration_id"])
    if current is None:
        r.put(state, "legacy_ops", incoming)
        return
    for field in ("shard", "source", "target", "expected", "replacement"):
        if current[field] != incoming[field]:
            raise ValueError("migration identity names incompatible ownership")
    current["complete"] = bool(current["complete"] or incoming["complete"])
    r.put(state, "legacy_ops", current)


def _remove_old_responsibility(state, shard):
    identity = _logical_coordinator(shard)
    state["txn_begins"][:] = [row for row in state["txn_begins"]
                              if row["coordinator"] != identity]
    state["coord_reads"][:] = [row for row in state["coord_reads"]
                               if row["coordinator"] != identity]
    for table in PARTICIPANT_TABLES:
        state[table][:] = [row for row in state[table] if row["shard"] != shard]


def install(node, incoming):
    """Install an authentic exported record; return INSTALLED or STALE."""
    row = r.clone(incoming)
    cut = row.get("image")
    required = ("operation_kind", "operation_id", "shard", "source", "target",
                "source_epoch", "target_epoch", "image_digest")
    if (row.get("role") != "SOURCE" or row.get("state") != "EXPORTED"
            or any(field not in row for field in required)
            or type(cut) is not dict or cut.get("format_version") != 2
            or cut.get("shard") != row["shard"]
            or cut.get("epoch") != row["source_epoch"]
            or row["target_epoch"] != row["source_epoch"] + 1
            or r.digest(cut) != row["image_digest"]):
        raise ValueError("invalid transferable shard image")
    if node.id != row["target"]:
        return "STALE"
    existing = r.find(node.state, "transfers",
                      operation_kind=row["operation_kind"],
                      operation_id=row["operation_id"], shard=row["shard"],
                      role="TARGET")
    if existing is not None:
        if (existing["image_digest"] != row["image_digest"]
                or existing["source_epoch"] != row["source_epoch"]
                or existing["target_epoch"] != row["target_epoch"]):
            raise ValueError("a transfer identity was reused for another image")
        return "INSTALLED"
    local_epoch = node.state["shard_epochs"].get(row["shard"], -1)
    if local_epoch > row["target_epoch"]:
        return "STALE"

    # Equality means that a compatible v1 target image was already installed,
    # or that a crash lost only the private transfer index.  Reinstalling in
    # that case could roll back writes served at the same ownership epoch.
    if local_epoch < row["target_epoch"]:
        _remove_old_responsibility(node.state, row["shard"])
        node.state["values"][row["shard"]] = r.clone(cut["values"])
        for table in ("txn_begins",) + PARTICIPANT_TABLES + ("coord_reads",):
            for item in cut[table]:
                r.put(node.state, table, item)
        node.state["shard_epochs"][row["shard"]] = row["target_epoch"]
    for plan in cut["rebalance_plans"]:
        _merge_plan(node.state, plan)
    for move in cut["legacy_ops"]:
        _merge_legacy(node.state, move)
    target = {key: r.clone(row[key]) for key in required}
    target.update({"v": 2, "kind": "Transfer", "role": "TARGET",
                   "state": "INSTALLED"})
    r.put(node.state, "transfers", target)
    yield from node.save(cut["decisions"])
    return "INSTALLED"


def source(node, operation_kind, operation_id, shard):
    return r.find(node.state, "transfers", operation_kind=operation_kind,
                  operation_id=operation_id, shard=shard, role="SOURCE")


def target(node, operation_kind, operation_id, shard):
    return r.find(node.state, "transfers", operation_kind=operation_kind,
                  operation_id=operation_id, shard=shard, role="TARGET")
