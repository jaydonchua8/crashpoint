"""Control planes for the machine model in machines.py.

`Fenced` is the design I ended up with. The other classes each change one
decision in it. `Unfenced` is what I wrote first. The rest are seeded bugs
used to check that the checker can fail.
"""
from crashpoint import S, N, call, LostReply
from machines import ST, NODES, Unreachable


class Fenced:
    """Handlers only edit the record with compare-and-swap. One reconcile pass
    per machine observes both nodes and moves them toward the record.

    Every runtime start has an epoch. Storage lets only the newest epoch
    write, and a graceful stop closes its epoch, so a late or repeated call
    from an old pass cannot bring an old runtime back."""

    # ---- request handlers ----
    def create(self, mid, key):
        while True:
            rec = yield S("get", mid)
            if rec is not None:
                return "ok" if rec["create_key"] == key else "conflict"
            new = {"create_key": key, "desired_state": "running", "phase": "accepted",
                   "node": None, "epoch": 1, "reboot_gen": 0, "gen_applied": 0}
            if (yield S("insert", mid, new)):
                return "ok"

    wake_reuses_epoch = False
    keyed = True

    def _reject(self, key):
        """Save the rejection too, so a late duplicate cannot succeed."""
        if not self.keyed:
            return "conflict"
        return (yield S("idem_put", key, "conflict"))

    def _desire(self, mid, key, want, phase, new_epoch=False):
        while True:
            if self.keyed:
                saved = yield S("idem_get", key)
                if saved:
                    return saved
            rec = yield S("get", mid)
            if rec is None or (rec["desired_state"] == "destroyed" and want != "destroyed"):
                return (yield from self._reject(key))
            change = {}
            if rec["desired_state"] != want:
                change = {"desired_state": want, "phase": phase}
                if new_epoch and not self.wake_reuses_epoch:
                    # The epoch the machine slept in may have been closed while
                    # a VM of that epoch is still alive on a node I could not
                    # reach.
                    change["epoch"] = rec["epoch"] + 1
            elif not self.keyed:
                return "ok"
            if (yield S("cas", mid, rec["ver"], change, (key, "ok") if self.keyed else None)):
                return "ok"

    def sleep(self, mid, key):
        return (yield from self._desire(mid, key, "sleeping", "stopping"))

    def wake(self, mid, key):
        return (yield from self._desire(mid, key, "running", "starting", new_epoch=True))

    def delete(self, mid, key):
        return (yield from self._desire(mid, key, "destroyed", "destroying"))

    def reboot(self, mid, key):
        while True:
            saved = yield S("idem_get", key)
            if saved:
                return saved
            rec = yield S("get", mid)
            if rec is None or rec["desired_state"] != "running":
                return (yield from self._reject(key))      # 409: not running
            if rec["reboot_gen"] != rec["gen_applied"]:
                return (yield from self._reject(key))      # 409: another reboot in progress
            if (yield from self._record_reboot(mid, rec, key)):
                return "ok"

    def _record_reboot(self, mid, rec, key):
        # One transaction: only if the record is unchanged AND the key is
        # still unused. Checking the key in an earlier read is not enough.
        change = {"reboot_gen": rec["reboot_gen"] + 1, "reboot_key": key}
        return (yield S("cas", mid, rec["ver"], change, (key, "ok")))

    # ---- reconcile ----
    def _observe(self, n, mid):
        return (yield from call(N("observe", n, mid)))

    def _stop(self, n, mid, epoch):
        yield from call(N("stop_vm", n, mid, epoch))

    def _move(self, rec, node):
        return {"node": node, "epoch": rec["epoch"] + 1, "gen_applied": rec["reboot_gen"],
                "phase": "placement_pending"}

    def _close(self, mid, epoch):
        yield from call(ST("seal", mid, epoch))

    def _report(self, mid, rec, phase):
        yield S("cas", mid, rec["ver"], {"phase": phase})

    def reconcile(self, mid):
        rec = yield S("get", mid)
        if rec is None:
            return
        seen, down = {}, set()
        for n in NODES:
            try:
                vm = yield from self._observe(n, mid)
            except Unreachable:
                down.add(n)
                continue
            if vm:
                seen[n] = vm["epoch"]
        epoch, node, want = rec["epoch"], rec["node"], rec["desired_state"]
        pending = rec["reboot_gen"] != rec["gen_applied"]
        keep = (node, epoch) if want == "running" and not pending else None

        # Stop every reachable VM that is not the one the record names. A
        # graceful stop publishes a checkpoint if that VM is still the writer.
        for n, e in sorted(seen.items()):
            if (n, e) != keep:
                yield from self._stop(n, mid, e)

        if want != "running":
            # A VM of this epoch may still exist on a node I cannot reach, or
            # an old pass may be about to start one. Close the epoch first.
            yield from self._close(mid, epoch)
            if rec["phase"] != want:
                yield from self._report(mid, rec, want)
            return

        up = [n for n in NODES if n not in down]
        if not up:
            return
        if node is None or node in down:
            yield S("cas", mid, rec["ver"], self._move(rec, up[0]))
            return
        if pending:
            yield S("cas", mid, rec["ver"], {"epoch": epoch + 1, "gen_applied": rec["reboot_gen"],
                                             "phase": "starting"})
            return
        yield from call(ST("vol_create", mid))
        if not (yield from call(N("boot_vm", node, mid, epoch))):
            # This epoch was closed by a stop (sleep, or a stop I did not
            # see finish). Ask for the next one.
            yield S("cas", mid, rec["ver"], {"epoch": epoch + 1, "phase": "starting"})
            return
        if rec["phase"] != "running":
            yield from self._report(mid, rec, "running")

    # ---- what the scheduler asks ----
    def recover(self, world):
        return []

    def settled(self, rec):
        if rec["desired_state"] == "running":
            return rec["phase"] == "running" and rec["reboot_gen"] == rec["gen_applied"]
        return rec["phase"] == rec["desired_state"]

    def _drift(self, world, mid, rec):
        """Stands in for a periodic resync: does any reachable node disagree
        with the record?"""
        want_vm = rec["desired_state"] == "running"
        for n in NODES:
            mine = want_vm and rec["node"] == n
            if not world.nodes[n]["up"]:
                if mine:
                    return True
                continue
            vm = world.nodes[n]["vms"].get(mid)
            if vm and not (mine and vm["epoch"] == rec["epoch"]):
                return True
            if mine and not vm:
                return True
        return False

    def wants(self, world):
        return [mid for mid, rec in sorted(world.store.items())
                if not self.settled(rec) or self._drift(world, mid, rec)]


class Unfenced(Fenced):
    """My first version. When the node is unreachable, start the machine on
    the other node. No new epoch, so storage cannot tell the two apart."""

    def _move(self, rec, node):
        return {"node": node, "gen_applied": rec["reboot_gen"], "phase": "placement_pending"}


class StaleKeyRead(Fenced):
    """A bug the checker found in my reboot handler: the idempotency key was
    looked up in one read and the reboot recorded in a later write that did
    not check the key again."""

    def _record_reboot(self, mid, rec, key):
        change = {"reboot_gen": rec["reboot_gen"] + 1, "reboot_key": key}
        if not (yield S("cas", mid, rec["ver"], change)):
            return False
        yield S("idem_put", key, "ok")
        return True


class WakeReusesEpoch(Fenced):
    """A bug the checker found in my fencing: wake kept the epoch the machine
    went to sleep in."""
    wake_reuses_epoch = True


class UnkeyedRequests(Fenced):
    """A bug the checker found once old tasks could outlive a failover: sleep,
    wake and delete were applied without recording the request key, so a
    late copy of an old request was applied again."""
    keyed = False


class Impatient(Fenced):
    """Seeded bug: one lost reply from a node is treated as a dead node."""

    def _observe(self, n, mid):
        try:
            return (yield N("observe", n, mid))
        except LostReply:
            raise Unreachable(n)


class HardKill(Fenced):
    """Seeded bug: stop VMs without publishing a checkpoint."""

    def _stop(self, n, mid, epoch):
        yield from call(N("kill_vm", n, mid, epoch))


class NoResync(Fenced):
    """Seeded bug: only look at a machine again when its record is unsettled."""

    def _drift(self, world, mid, rec):
        return rec["desired_state"] == "running" and not world.nodes[rec["node"]]["up"] \
            if rec["node"] else False


class NoSeal(Fenced):
    """Seeded bug: report sleeping or destroyed without closing the epoch."""

    def _close(self, mid, epoch):
        return
        yield


class BlindPhase(Fenced):
    """Seeded bug: write the phase without checking the record version."""

    def _report(self, mid, rec, phase):
        yield S("put", mid, {"phase": phase})


PLANES = {"unfenced": Unfenced, "fenced": Fenced}
FOUND = [StaleKeyRead, WakeReusesEpoch, UnkeyedRequests]      # my own bugs, kept so the tests can replay them
MUTANTS = [Impatient, HardKill, NoResync, NoSeal, BlindPhase]
