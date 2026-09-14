"""Atomic multi-shard placement plans over the reusable data-worker pool."""
from . import records as r
from . import transfer


MESSAGE_TYPES = {"REBALANCE_START", "REBALANCE_INSTALL", "REBALANCE_READY",
                 "REBALANCE_COMPLETE"}


def _new_plan(registration):
    expected = r.clone(registration["expected"])
    replacement = r.clone(registration["replacement"])
    shards = sorted(expected)
    if (not shards or set(expected) != set(replacement)
            or registration["plan_id"] == ""):
        raise ValueError("invalid rebalance registration")
    for shard in shards:
        if replacement[shard]["epoch"] != expected[shard]["epoch"] + 1:
            raise ValueError("rebalance epochs are not consecutive")
        if replacement[shard]["migration_id"] != registration["plan_id"]:
            raise ValueError("rebalance replacement has the wrong plan identity")
    return {
        "v": 2, "kind": "RebalancePlan", "plan_id": registration["plan_id"],
        "expected": expected, "replacement": replacement,
        "leader": expected[shards[0]]["owner"], "ready": [],
        "complete": False,
    }


def _registration(plan):
    return {"plan_id": plan["plan_id"], "expected": r.clone(plan["expected"]),
            "replacement": r.clone(plan["replacement"])}


def _ensure_plan(node, incoming):
    candidate = (_new_plan(incoming) if "leader" not in incoming
                 else r.clone(incoming))
    # Recompute this rather than trusting a message's physical leader field.
    candidate["leader"] = candidate["expected"][sorted(candidate["expected"])[0]]["owner"]
    candidate.setdefault("ready", [])
    candidate.setdefault("complete", False)
    current = r.find(node.state, "rebalance_plans", plan_id=candidate["plan_id"])
    if current is None:
        r.put(node.state, "rebalance_plans", candidate)
        return r.find(node.state, "rebalance_plans", plan_id=candidate["plan_id"]), True
    for field in ("expected", "replacement", "leader"):
        if current[field] != candidate[field]:
            raise ValueError("rebalance plan identity was reused")
    changed = False
    ready = sorted(set(current["ready"]) | set(candidate["ready"]))
    if ready != current["ready"]:
        current["ready"] = ready
        changed = True
    if candidate["complete"] and not current["complete"]:
        current["complete"] = True
        changed = True
    if changed:
        r.put(node.state, "rebalance_plans", current)
        current = r.find(node.state, "rebalance_plans", plan_id=candidate["plan_id"])
    return current, changed


def _endpoints(plan):
    return sorted({record["owner"] for vector in
                   (plan["expected"], plan["replacement"])
                   for record in vector.values()})


def _later_than_plan(records, plan):
    """Sequential registration makes an epoch successor proof historical."""
    if records == plan["replacement"]:
        return True
    if records == plan["expected"]:
        return False
    return all(records[shard]["epoch"] >= plan["replacement"][shard]["epoch"]
               for shard in plan["expected"])


def _ensure_exports(node, plan):
    changed = False
    for shard in sorted(plan["expected"]):
        old, new = plan["expected"][shard], plan["replacement"][shard]
        if old["owner"] != node.id:
            continue
        _, created = transfer.source_record(
            node, "rebalance", plan["plan_id"], shard, node.id, new["owner"],
            old["epoch"], new["epoch"])
        changed = changed or created
    return changed


def _send_starts(node, plan):
    body = {"type": "REBALANCE_START", "plan": r.clone(plan)}
    for peer in _endpoints(plan):
        if peer != node.id:
            node.send(peer, body)


def _send_installs(node, plan):
    for shard in sorted(plan["expected"]):
        outgoing = transfer.source(node, "rebalance", plan["plan_id"], shard)
        if outgoing is not None:
            node.send(outgoing["target"], {"type": "REBALANCE_INSTALL",
                                           "plan": r.clone(plan),
                                           "transfer": r.clone(outgoing)})


def _send_local_ready(node, plan):
    for shard in sorted(plan["expected"]):
        installed = transfer.target(node, "rebalance", plan["plan_id"], shard)
        if installed is not None:
            node.send(plan["leader"], {"type": "REBALANCE_READY",
                                       "plan_id": plan["plan_id"],
                                       "shard": shard,
                                       "image_digest": installed["image_digest"]})


def _reply(node, plan):
    tokens = node.rebalance_waiters.pop(plan["plan_id"], set())
    for token in tokens:
        node.reply(token, {"plan_id": plan["plan_id"], "status": "COMPLETE",
                           "owners": r.clone(plan["replacement"])})


def _finish(node, plan):
    if not plan["complete"]:
        plan["complete"] = True
        r.put(node.state, "rebalance_plans", plan)
        yield from node.save()
        plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    _reply(node, plan)
    body = {"type": "REBALANCE_COMPLETE", "plan": r.clone(plan)}
    for peer in _endpoints(plan):
        if peer != node.id:
            node.send(peer, body)


def drive(node, plan):
    plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    if plan["complete"]:
        _reply(node, plan)
        return
    shards = sorted(plan["expected"])
    result = yield from node.io("read_owners", shards)
    records = result["records"]
    if _later_than_plan(records, plan):
        yield from _finish(node, plan)
        return
    if records != plan["expected"]:
        return
    _send_starts(node, plan)
    if _ensure_exports(node, plan):
        yield from node.save()
        plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    _send_installs(node, plan)
    _send_local_ready(node, plan)
    if node.id != plan["leader"] or set(plan["ready"]) != set(shards):
        return
    swapped = yield from node.io("cas_owners", plan["expected"], plan["replacement"])
    if swapped["records"] == plan["replacement"]:
        yield from _finish(node, plan)


def client(node, event):
    registration = event["rebalance"]
    if registration["plan_id"] != event["request"]["plan_id"]:
        raise ValueError("request and registered rebalance identity disagree")
    requested = event["request"]["moves"]
    if requested != {shard: owner["owner"]
                     for shard, owner in registration["replacement"].items()}:
        raise ValueError("request and registered rebalance destinations disagree")
    plan, changed = _ensure_plan(node, registration)
    node.rebalance_waiters.setdefault(plan["plan_id"], set()).add(
        event["request_token"])
    changed = _ensure_exports(node, plan) or changed
    if changed:
        yield from node.save()
        plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    yield from drive(node, plan)


def _start(node, source, body):
    plan, changed = _ensure_plan(node, body["plan"])
    if source != plan["leader"] or node.id not in _endpoints(plan):
        return
    changed = _ensure_exports(node, plan) or changed
    if changed:
        yield from node.save()
        plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    yield from drive(node, plan)


def _install(node, source, body):
    plan, changed = _ensure_plan(node, body["plan"])
    incoming = body["transfer"]
    shard = incoming["shard"]
    if (shard not in plan["expected"]
            or source != plan["expected"][shard]["owner"]
            or incoming["source"] != source
            or incoming["target"] != plan["replacement"][shard]["owner"]
            or incoming["source_epoch"] != plan["expected"][shard]["epoch"]
            or incoming["target_epoch"] != plan["replacement"][shard]["epoch"]):
        return
    outcome = yield from transfer.install(node, incoming)
    # install() persists the plan along with a new target transfer.  A duplicate
    # or stale image may still have introduced a newly learned plan record.
    if outcome != "INSTALLED" and changed:
        yield from node.save()
    plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    if outcome == "INSTALLED":
        installed = transfer.target(node, "rebalance", plan["plan_id"], shard)
        node.send(plan["leader"], {"type": "REBALANCE_READY",
                                   "plan_id": plan["plan_id"], "shard": shard,
                                   "image_digest": installed["image_digest"]})
    if _ensure_exports(node, plan):
        yield from node.save()
        plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    yield from drive(node, plan)


def _ready(node, source, body):
    plan = r.find(node.state, "rebalance_plans", plan_id=body["plan_id"])
    if plan is None or node.id != plan["leader"]:
        return
    shard = body["shard"]
    if shard not in plan["replacement"] or source != plan["replacement"][shard]["owner"]:
        return
    local = transfer.target(node, "rebalance", plan["plan_id"], shard)
    # When the leader is itself the target, validate against its durable install.
    if source == node.id and (local is None or local["image_digest"] != body["image_digest"]):
        return
    if shard not in plan["ready"]:
        plan["ready"] = sorted(set(plan["ready"]) | {shard})
        r.put(node.state, "rebalance_plans", plan)
        yield from node.save()
        plan = r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"])
    yield from drive(node, plan)


def _complete(node, source, body):
    plan, changed = _ensure_plan(node, body["plan"])
    if source != plan["leader"] or not body["plan"].get("complete"):
        return
    if not plan["complete"]:
        plan["complete"] = True
        r.put(node.state, "rebalance_plans", plan)
        changed = True
    if changed:
        yield from node.save()
    _reply(node, r.find(node.state, "rebalance_plans", plan_id=plan["plan_id"]))


def message(node, source, body):
    handlers = {"REBALANCE_START": _start, "REBALANCE_INSTALL": _install,
                "REBALANCE_READY": _ready, "REBALANCE_COMPLETE": _complete}
    yield from handlers[body["type"]](node, source, body)


def tick(node):
    for plan in list(node.state["rebalance_plans"]):
        if not plan["complete"]:
            yield from drive(node, plan)
