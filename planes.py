"""Three control planes for the same API: create(machine, key), delete(machine).

A machine is one volume, one IP and one VM on the node. `key` is the client's
idempotency key and also names the incarnation of the machine.
"""
from crashpoint import S, N, call


class Naive:
    """Do the steps in order and write the record at the end."""

    def create(self, mid, key):
        rec = yield S("get", mid)
        if rec:
            return "ok"
        vol = yield N("create_volume", None, mid, key)
        vm = "vm-" + vol
        yield N("alloc_ip", vm, mid, key)
        yield N("boot_vm", vm, mid, key, vol, vm)
        yield S("insert", mid, {"uid": key, "state": "running", "vm": vm, "vol": vol})
        return "ok"

    def delete(self, mid):
        rec = yield S("get", mid)
        if not rec:
            return "ok"
        yield N("kill_vm", rec["vm"])
        yield N("release_ip", rec["vm"])
        yield N("delete_volume", rec["vol"])
        yield S("remove", mid)
        return "ok"

    def recover(self, world):
        return []

    def wants(self, world):
        return []

    def settled(self, rec):
        return rec.get("state") == "running"


class Journal:
    """Write the intent first, name resources after the key so every step can
    be repeated, and resume unfinished operations after a restart."""

    def _build(self, mid, uid):
        vm = "%s.%s" % (mid, uid)
        yield from call(N("create_volume", vm + ".vol", mid, uid))
        yield from call(N("alloc_ip", vm, mid, uid))
        ok = yield from call(N("boot_vm", vm, mid, uid, vm + ".vol", vm))
        if not ok:
            return "error"
        yield S("put", mid, {"state": "running"})
        return "ok"

    def _teardown(self, mid, uid):
        vm = "%s.%s" % (mid, uid)
        yield from call(N("kill_vm", vm))
        yield from call(N("release_ip", vm))
        yield from call(N("delete_volume", vm + ".vol"))
        yield S("remove", mid)
        return "ok"

    def create(self, mid, key):
        rec = yield S("get", mid)
        if rec and rec["uid"] != key:
            if rec["state"] != "deleting":
                return "conflict"
            yield from self._teardown(mid, rec["uid"])
            rec = None
        if rec and rec["state"] == "running":
            return "ok"
        if rec is None:
            yield S("insert", mid, {"uid": key, "state": "creating"})
        return (yield from self._build(mid, key))

    def delete(self, mid):
        rec = yield S("get", mid)
        if not rec:
            return "ok"
        yield S("put", mid, {"state": "deleting"})
        return (yield from self._teardown(mid, rec["uid"]))

    def recover(self, world):
        out = []
        for mid, rec in sorted(world.store.items()):
            if rec["state"] == "creating":
                out.append(("resume create " + mid, self._build(mid, rec["uid"])))
            elif rec["state"] == "deleting":
                out.append(("resume delete " + mid, self._teardown(mid, rec["uid"])))
        return out

    def wants(self, world):
        return []

    def settled(self, rec):
        return rec.get("state") == "running"


class Reconcile:
    """Handlers only write the desired state with compare-and-swap. One
    reconcile task per machine observes the node and moves it toward the
    record. Nothing about progress is remembered between passes."""

    def create(self, mid, key):
        while True:
            rec = yield S("get", mid)
            if rec is None:
                if (yield S("insert", mid, {"uid": key, "want": "present", "status": "pending"})):
                    return "ok"
            elif rec["uid"] == key:
                return "ok"
            elif rec["want"] == "present":
                return "conflict"
            elif (yield S("cas", mid, rec["ver"], {"uid": key, "want": "present", "status": "pending"})):
                return "ok"

    def delete(self, mid):
        while True:
            rec = yield S("get", mid)
            if rec is None or rec["want"] == "absent":
                return "ok"
            if (yield S("cas", mid, rec["ver"], {"want": "absent", "status": "pending"})):
                return "ok"

    def reconcile(self, mid):
        rec = yield S("get", mid)
        obs = yield from call(N("observe", mid))
        keep = rec["uid"] if rec and rec["want"] == "present" else None
        for name, uid in obs["vms"]:
            if uid != keep:
                yield from call(N("kill_vm", name))
        for owner, uid in obs["ips"]:
            if uid != keep:
                yield from call(N("release_ip", owner))
        for name, uid in obs["vols"]:
            if uid != keep:
                yield from call(N("delete_volume", name))
        if rec is None:
            return
        if keep is None:
            yield S("cas_remove", mid, rec["ver"])
            return
        vm = "%s.%s" % (mid, keep)
        yield from call(N("create_volume", vm + ".vol", mid, keep))
        yield from call(N("alloc_ip", vm, mid, keep))
        if (yield from call(N("boot_vm", vm, mid, keep, vm + ".vol", vm))):
            yield S("cas", mid, rec["ver"], {"status": "running"})

    def recover(self, world):
        return []

    def wants(self, world):
        return [m for m, r in sorted(world.store.items())
                if not (r["want"] == "present" and r["status"] == "running")]

    def settled(self, rec):
        return rec.get("want") == "present" and rec.get("status") == "running"


PLANES = {"naive": Naive, "journal": Journal, "reconcile": Reconcile}

SCENARIOS = {
    "create": [[("create", "m1", "k1")]],
    "create-delete": [[("create", "m1", "k1"), ("delete", "m1")]],
    "recreate": [[("create", "m1", "k1"), ("delete", "m1"), ("create", "m1", "k2")]],
    "retry-race": [[("create", "m1", "k1")], [("create", "m1", "k1")]],
    "delete-race": [[("create", "m1", "k1")], [("delete", "m1")]],
}

# Too large to enumerate here. Used with random schedules only.
FUZZ_SCENARIOS = {
    "two-machines": [[("create", "m1", "k1"), ("delete", "m1")], [("create", "m2", "k2")]],
    "three-clients": [[("create", "m1", "k1"), ("delete", "m1"), ("create", "m1", "k3")],
                      [("create", "m2", "k2"), ("delete", "m2")],
                      [("delete", "m1"), ("create", "m2", "k4")]],
}
