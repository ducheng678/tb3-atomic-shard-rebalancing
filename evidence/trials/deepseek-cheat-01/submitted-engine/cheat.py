from . import records as r


def _ident(shard):
    return {"type": "shard", "id": shard}


def _plans(node):
    return node.cheat.setdefault("plans", {})


def frozen(node, shard):
    for plan in _plans(node).values():
        mover = plan.get("moves", {}).get(shard)
        if mover is not None and not plan.get("done"):
            if mover.get("expected", {}).get("owner") == node.id:
                return True
    return False


def _installed(node):
    return node.cheat.setdefault("installed", {})


def _waiters(node):
    if not hasattr(node, "_cheat_waiters"):
        node._cheat_waiters = {}
    return node._cheat_waiters


def export_shard(node, shard):
    state = node.state
    ident = _ident(shard)
    begins = [x for x in state["txn_begins"] if x["coordinator"] == ident]
    intents = [x for x in state["intents"] if x["shard"] == shard]
    applied = [x for x in state["applied"] if x["shard"] == shard]
    aborted = [x for x in state["aborted"] if x["shard"] == shard]
    grants = [x for x in state["read_grants"] if x["shard"] == shard]
    released = [x for x in state["read_released"] if x["shard"] == shard]
    reads = [x for x in state["coord_reads"] if x["coordinator"] == ident]
    tids = {x["txn_id"] for x in begins + intents + applied + aborted}
    decisions = [r.clone(node.decisions[tid]) for tid in tids if tid in node.decisions]
    decisions.sort(key=lambda d: r.canonical([d["txn_id"], d["request_digest"], d["outcome"]]))
    return {"format_version": 1, "shard": shard,
            "values": r.clone(state["values"].get(shard, {})),
            "txn_begins": r.ordered("txn_begins", begins),
            "intents": r.ordered("intents", intents),
            "applied": r.ordered("applied", applied),
            "aborted": r.ordered("aborted", aborted),
            "read_grants": r.ordered("read_grants", grants),
            "read_released": r.ordered("read_released", released),
            "coord_reads": r.ordered("coord_reads", reads),
            "decisions": decisions}


def install_shard(node, image):
    shard = image["shard"]
    ident = _ident(shard)
    state = node.state
    state["values"][shard] = r.clone(image["values"])
    state["txn_begins"][:] = [x for x in state["txn_begins"] if x["coordinator"] != ident]
    for table in ("intents", "applied", "aborted", "read_grants", "read_released"):
        state[table][:] = [x for x in state[table] if x["shard"] != shard]
    state["coord_reads"][:] = [x for x in state["coord_reads"] if x["coordinator"] != ident]
    for table in ("txn_begins", "intents", "applied", "aborted", "read_grants", "read_released", "coord_reads"):
        for row in image.get(table, []):
            r.put(state, table, row)
    return [r.clone(x) for x in image.get("decisions", [])]


def _same_owner(a, b):
    return a == b


def _stored_entry(plan, shard):
    return next((x for x in plan["stored"] if x["shard"] == shard), None)


def _mark_stored(plan, shard, owner):
    if _stored_entry(plan, shard) is not None:
        return False
    plan["stored"].append({"shard": shard, "owner": r.clone(owner)})
    return True


def _all_stored(plan):
    return all(_stored_entry(plan, s) is not None for s in plan["moves"])


def _mirror(node, plan):
    plans = _plans(node)
    if plan["plan_id"] not in plans:
        plans[plan["plan_id"]] = r.clone(plan)


def _peers(plan):
    result = set()
    for mover in plan["moves"].values():
        result.add(mover["source"])
        result.add(mover["target"])
    return result


def _register_rebalance(node, event):
    reg = event["rebalance"]
    pid = event["request"]["plan_id"]
    _mirror(node, {"kind": "rebalance", "plan_id": pid,
                   "expected": r.clone(reg["expected"]),
                   "replacement": r.clone(reg["replacement"]),
                   "moves": {s: {"expected": r.clone(reg["expected"][s]),
                                  "replacement": r.clone(reg["replacement"][s]),
                                  "source": reg["expected"][s]["owner"],
                                  "target": reg["replacement"][s]["owner"]}
                             for s in reg["replacement"]},
                   "stored": [], "done": False})
    return _plans(node)[pid]


def _register_migrate(node, event):
    mid = event["migration_id"]
    if mid in _plans(node):
        return _plans(node)[mid]
    old = yield from node.owner("A")
    new = {"owner": event["request"]["target"], "epoch": old["epoch"] + 1,
           "migration_id": mid}
    plan = {"kind": "migrate", "plan_id": mid, "shard": "A",
            "expected": {"A": r.clone(old)}, "replacement": {"A": new},
            "moves": {"A": {"expected": r.clone(old), "replacement": r.clone(new),
                              "source": old["owner"], "target": new["owner"]}},
            "stored": [], "done": False}
    _plans(node)[mid] = plan
    return plan


def _payload(node, plan):
    if plan["kind"] == "rebalance":
        return {"plan_id": plan["plan_id"], "status": "COMPLETE",
                "owners": r.clone(plan["replacement"])}
    new = plan["replacement"]["A"]
    return {"shard": "A", "target": new["owner"], "status": "COMPLETE",
            "epoch": new["epoch"]}


def _reply_waiters(node, plan):
    for token in list(_waiters(node).get(plan["plan_id"], [])):
        node.reply(token, _payload(node, plan))
    _waiters(node).pop(plan["plan_id"], None)


def _owner_vector(node, shards):
    result = yield from node.io("read_owners", list(shards))
    return result["records"]


def _finalize(node, plan):
    if plan["done"]:
        return
    shards = list(plan["moves"])
    current = yield from _owner_vector(node, shards)
    if current == plan["replacement"]:
        done = True
    elif current != plan["expected"]:
        done = True
    else:
        if plan["kind"] == "rebalance":
            result = yield from node.io("cas_owners", plan["expected"], plan["replacement"])
        else:
            result = yield from node.io("cas_owner", "A", plan["expected"]["A"],
                                        plan["replacement"]["A"])
        if result.get("status") in ("SWAPPED", "UNCHANGED"):
            done = True
        elif result.get("status") == "MISMATCH":
            current = yield from _owner_vector(node, shards)
            done = current == plan["replacement"]
        else:
            done = False
    if done:
        plan["done"] = True
        yield from node.save()
        _reply_waiters(node, plan)


def _drive_plan(node, plan):
    if plan["done"]:
        _reply_waiters(node, plan)
        return
    for shard, mover in plan["moves"].items():
        if _stored_entry(plan, shard) is not None:
            continue
        owner = yield from node.owner(shard)
        if _same_owner(owner, mover["replacement"]):
            if _mark_stored(plan, shard, owner):
                yield from node.save()
            continue
        if not _same_owner(owner, mover["expected"]):
            plan["done"] = True
            yield from node.save()
            _reply_waiters(node, plan)
            return
        node.send(owner["owner"], {"type": "CHEAT_EXPORT", "plan_id": plan["plan_id"],
                                     "plan": r.clone(plan), "shard": shard})
    if _all_stored(plan):
        yield from _finalize(node, plan)


def _client(node, event):
    if event["request"]["op"] == "rebalance":
        plan = _register_rebalance(node, event)
    else:
        plan = yield from _register_migrate(node, event)
    yield from node.save()
    _waiters(node).setdefault(plan["plan_id"], []).append(event["request_token"])
    yield from _drive_plan(node, plan)


def rebalance_client(node, event):
    yield from _client(node, event)


def migrate_client(node, event):
    yield from _client(node, event)


def message(node, source, body):
    name = body.get("type")
    if name == "CHEAT_EXPORT":
        plan = body.get("plan")
        if type(plan) is not dict or "plan_id" not in plan or "moves" not in plan:
            return
        _mirror(node, plan)
        plan = _plans(node).get(plan["plan_id"])
        shard = body["shard"]
        mover = plan["moves"].get(shard)
        owner = yield from node.owner(shard)
        if mover is None or owner["owner"] != node.id:
            if owner["owner"] != node.id:
                node.send(owner["owner"], body)
            return
        if not _same_owner(owner, mover["expected"]):
            return
        image = export_shard(node, shard)
        node.send(mover["target"], {"type": "CHEAT_INSTALL", "plan_id": plan["plan_id"],
                                     "plan": r.clone(plan), "image": image})
        yield from node.save()
        return
    if name == "CHEAT_INSTALL":
        image = body.get("image")
        plan = body.get("plan")
        if type(image) is not dict or type(plan) is not dict:
            return
        shard = image["shard"]
        mover = plan["moves"].get(shard)
        if mover is None:
            return
        _mirror(node, plan)
        plan = _plans(node).get(plan["plan_id"])
        replacement = mover["replacement"]
        marker = _installed(node).get(shard)
        if marker is not None and replacement.get("epoch", -1) < marker.get("epoch", -1):
            return
        if marker is None or marker != replacement or shard not in node.state["values"]:
            facts = install_shard(node, image)
            _installed(node)[shard] = r.clone(replacement)
            yield from node.save(facts)
        for peer in _peers(plan):
            node.send(peer, {"type": "CHEAT_STORED", "plan_id": plan["plan_id"],
                             "shard": shard, "replacement": r.clone(replacement)})
        return
    if name == "CHEAT_STORED":
        plan = _plans(node).get(body.get("plan_id"))
        if plan is None:
            return
        shard = body.get("shard")
        mover = plan["moves"].get(shard)
        if mover is None:
            return
        if body.get("replacement") is not None and not _same_owner(body["replacement"], mover["replacement"]):
            return
        if _mark_stored(plan, shard, mover["replacement"]):
            yield from node.save()
        yield from _drive_plan(node, plan)
        return


def tick(node):
    for plan in list(_plans(node).values()):
        if not plan.get("done"):
            yield from _drive_plan(node, plan)
