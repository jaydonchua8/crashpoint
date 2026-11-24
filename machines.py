"""A machine-lifecycle model shaped like the public Dedalus Machines API.

What is taken from public material (see README for the URLs):
  * requests: create, sleep, wake, reboot, delete on /v1/machines
  * a machine has `desired_state` (running, sleeping, destroyed) and `phase`
    (the nine values in PHASES), and its lifecycle status carries a `revision`
  * reboot is rejected with 409 unless desired_state is running
  * repeating a reboot with the same Idempotency-Key must not restart again
  * files survive sleep, wake and reboot; processes and memory do not
  * a machine must reach phase running before anyone can use it

Everything else here is my own model and not a description of how Dedalus
is built: two nodes, a storage service that arbitrates who may write a
machine's volume, and a record store with compare-and-swap.

SYNTHETIC: there is no real VM, disk or network in this file.
"""
from crashpoint import WorldFault

DESIRED = ("running", "sleeping", "destroyed")
PHASES = ("accepted", "placement_pending", "starting", "running", "stopping",
          "sleeping", "destroying", "destroyed", "failed")
NODES = ("n1", "n2")


class Unreachable(WorldFault):
    """The node is partitioned from the control plane. The call never arrived.
    Whatever runs on that node keeps running."""

    def __str__(self):
        return "unreachable"


def ST(method, *args):
    """A call to the storage service. Replies can be lost, like node calls."""
    return ("storage", method, args)


class Cluster:
    """Everything that survives a control-plane crash."""

    def __init__(self):
        self.store = {}       # machine id -> record (with 'ver')
        self.idem = {}        # idempotency key -> saved reply
        self.ver = 0
        self.nodes = dict((n, {"up": True, "vms": {}}) for n in NODES)
        self.vols = {}        # machine id -> {'lease', 'sealed', 'committed'}
        self.partitioned = set()   # nodes that were ever cut off in this run
        self.reported = {}    # machine id -> epochs that were ever reported running
        self.bumps = {}       # reboot key -> times it advanced reboot_gen
        self.flags = []       # violations the world itself can see

    def _flag(self, kind, detail):
        if (kind, detail) not in self.flags:
            self.flags.append((kind, detail))

    # ---- record store (linearizable, replies are never lost) ----
    def get(self, mid):
        rec = self.store.get(mid)
        return dict(rec) if rec else None

    def _written(self, mid, changes):
        rec = self.store[mid]
        if rec["desired_state"] not in DESIRED or rec["phase"] not in PHASES:
            self._flag("bad-enum", "%s desired_state=%s phase=%s" % (mid, rec["desired_state"], rec["phase"]))
        if rec["phase"] == "running":
            self.reported.setdefault(mid, set()).add(rec["epoch"])
        if "reboot_key" in changes:
            key = changes["reboot_key"]
            self.bumps[key] = self.bumps.get(key, 0) + 1
            if self.bumps[key] > 1:
                self._flag("double-reboot", "key %s restarted %s %d times" % (key, mid, self.bumps[key]))

    def insert(self, mid, rec):
        if mid in self.store:
            return False
        self.ver += 1
        self.store[mid] = dict(rec, ver=self.ver)
        self._written(mid, rec)
        return True

    def cas(self, mid, ver, changes, idem=None):
        """Compare-and-swap on the record version. With `idem=(key, reply)` the
        write only happens if that key has no saved reply yet, and the reply
        is saved in the same transaction."""
        rec = self.store.get(mid)
        if rec is None or rec["ver"] != ver:
            return False
        if idem and idem[0] in self.idem:
            return False
        self.ver += 1
        rec.update(changes, ver=self.ver)
        if idem:
            self.idem[idem[0]] = idem[1]
        self._written(mid, changes)
        return True

    def put(self, mid, changes):
        """Unconditional write. The correct plane never uses it."""
        if mid not in self.store:
            return False
        self.ver += 1
        self.store[mid].update(changes, ver=self.ver)
        self._written(mid, changes)
        return True

    def idem_get(self, key):
        return self.idem.get(key)

    def idem_put(self, key, reply):
        """Save a reply unless one is saved already. Returns the saved one."""
        return self.idem.setdefault(key, reply)

    # ---- storage service ----
    def _writers(self, mid):
        vol = self.vols.get(mid)
        out = []
        if vol:
            for n in NODES:
                vm = self.nodes[n]["vms"].get(mid)
                if vm and vm["epoch"] == vol["lease"] and vm["epoch"] > vol["sealed"]:
                    out.append((n, vm["epoch"]))
        return out

    def _revoke(self, mid, lease=None, sealed=None):
        """Move the fence. A VM that loses write access this way never got to
        publish a checkpoint. That is only acceptable if the VM was never
        usable (never reported running) or its node had been cut off."""
        vol = self.vols[mid]
        before = self._writers(mid)
        if lease is not None:
            vol["lease"] = max(vol["lease"], lease)
        if sealed is not None:
            vol["sealed"] = max(vol["sealed"], sealed)
        after = self._writers(mid)
        for n, epoch in before:
            if (n, epoch) in after or epoch not in self.reported.get(mid, ()):
                continue
            if self.nodes[n]["up"] and n not in self.partitioned:
                self._flag("data-loss", "%s epoch %d on healthy node %s was fenced without a checkpoint" % (mid, epoch, n))

    def vol_create(self, mid):
        self.vols.setdefault(mid, {"lease": 0, "sealed": 0, "committed": 0})
        return True

    def seal(self, mid, epoch):
        """No VM of this epoch or an older one may write or start any more.
        The fence is recorded even if the volume was never created."""
        self.vol_create(mid)
        self._revoke(mid, sealed=epoch)
        return True

    # ---- nodes ----
    def _node(self, n):
        if not self.nodes[n]["up"]:
            raise Unreachable(n)
        return self.nodes[n]["vms"]

    def observe(self, n, mid):
        vm = self._node(n).get(mid)
        return dict(vm) if vm else None

    def boot_vm(self, n, mid, epoch):
        """Attach the volume under `epoch` and start. Repeating it is safe.
        Refused if storage has moved past this epoch."""
        vms = self._node(n)
        vol = self.vols.get(mid)
        if mid in vms:
            return vms[mid]["epoch"] == epoch
        if vol is None or epoch < vol["lease"] or epoch <= vol["sealed"]:
            return False
        self._revoke(mid, lease=epoch)
        vms[mid] = {"epoch": epoch, "base": vol["committed"]}
        return True

    def stop_vm(self, n, mid, epoch):
        """Graceful stop: publish a checkpoint if this VM is the writer, then
        close the epoch so it cannot be started again."""
        vms = self._node(n)
        vm = vms.get(mid)
        if vm and vm["epoch"] == epoch:
            vol = self.vols[mid]
            if (n, epoch) in self._writers(mid):
                vol["committed"] += 1
            del vms[mid]
            vol["sealed"] = max(vol["sealed"], epoch)
        return True

    def kill_vm(self, n, mid, epoch):
        """Hard stop, no checkpoint."""
        vms = self._node(n)
        vm = vms.get(mid)
        if vm and vm["epoch"] == epoch:
            if (n, epoch) in self._writers(mid) and epoch in self.reported.get(mid, ()):
                self._flag("data-loss", "%s epoch %d was killed without a checkpoint" % (mid, epoch))
            del vms[mid]
        return True

    # ---- environment ----
    def partition(self, n):
        self.nodes[n]["up"] = False
        self.partitioned.add(n)

    def heal(self, n):
        self.nodes[n]["up"] = True

    def machines(self):
        mids = set(self.store) | set(self.vols)
        for n in NODES:
            mids.update(self.nodes[n]["vms"])
        return sorted(mids)


def to_api(mid, rec):
    """Project a record onto the field names of the public Machine object."""
    return {"machine_id": mid, "desired_state": rec["desired_state"], "phase": rec["phase"],
            "status": {"phase": rec["phase"], "revision": str(rec["ver"]),
                       "retryable": rec["phase"] != "failed"}}


def expected(scenario, acks):
    """What the acknowledged replies promise. Returns
    ({machine: desired_state} for machines used by one session only,
     set of reboot keys that got at least one 'ok',
     number of acknowledged wakes per machine)."""
    owners = {}
    for i, reqs in enumerate(scenario):
        for req in reqs:
            owners.setdefault(req[1], set()).add(i)
    state, keys, wakes = {}, set(), {}
    effect = {"create": "running", "sleep": "sleeping", "wake": "running", "delete": "destroyed"}
    for sess, req, reply in acks:
        if reply != "ok":
            continue
        if req[0] == "reboot":
            keys.add(req[2])
        if req[0] == "wake":
            wakes[req[1]] = wakes.get(req[1], 0) + 1
        if req[0] in effect and len(owners[req[1]]) == 1:
            state[req[1]] = effect[req[0]]
    return state, keys, wakes


class MachineModel:
    """World, environment faults and invariants for the machine planes.

    strict_restarts=True also applies the unrequested-restart check to runs
    with a control-plane failover. The fenced plane does not pass that. It is
    there to measure the gap, not to hide it."""

    def __init__(self, strict_restarts=False):
        self.strict_restarts = strict_restarts

    def new_world(self):
        return Cluster()

    def env_options(self, world, budget):
        down = [n for n in NODES if not world.nodes[n]["up"]]
        if down:      # at most one node is cut off at a time
            return [("node %s is reachable again" % n, lambda w, n=n: w.heal(n), False) for n in down]
        return [("node %s is partitioned from the control plane" % n,
                 lambda w, n=n: w.partition(n), True) for n in NODES]

    def step_check(self, world, violations):
        for mid in world.vols:
            writers = world._writers(mid)
            if len(writers) > 1 and ("split-brain", mid) not in violations:
                violations.append(("split-brain", mid))
            rec = world.store.get(mid)
            if rec and writers and rec["phase"] in ("sleeping", "destroyed") \
                    and ("writer-while-stopped", mid) not in violations:
                violations.append(("writer-while-stopped", mid))

    def settle(self, world):
        down = [n for n in NODES if not world.nodes[n]["up"]]
        for n in down:
            world.heal(n)
        return bool(down)

    def final_check(self, world, plane, scenario, res):
        out = list(world.flags)
        state, keys, wakes = expected(scenario, res.acks)
        for mid in world.machines():
            rec = world.store.get(mid)
            vms = [(n, world.nodes[n]["vms"][mid]["epoch"]) for n in NODES if mid in world.nodes[n]["vms"]]
            if rec is None:
                if vms:
                    out.append(("leak", "%s has no record but a VM on %s" % (mid, vms[0][0])))
                continue
            want = rec["desired_state"]
            if want == "running":
                good = [(rec["node"], rec["epoch"])]
                if vms != good or world._writers(mid) != good or rec["phase"] != "running" \
                        or rec["reboot_gen"] != rec["gen_applied"]:
                    kind = "leak" if len(vms) > 1 else "incomplete"
                    out.append((kind, "%s wants running at %s, phase %s, VMs %s" % (mid, good[0], rec["phase"], vms)))
            else:
                if vms:
                    out.append(("leak", "%s is %s but still has VMs %s" % (mid, want, vms)))
                if rec["phase"] != want:
                    out.append(("incomplete", "%s wants %s but phase is %s" % (mid, want, rec["phase"])))
            if mid in state and state[mid] != want:
                out.append(("lost", "%s was acknowledged as %s but desired_state is %s" % (mid, state[mid], want)))
            mine = [k for k in keys if world.bumps.get(k)]
            if len(mine) != len(keys):
                out.append(("lost", "a reboot was acknowledged but never recorded"))
            # A restart nobody asked for. Only decidable when nothing forced one.
            if not world.partitioned and (self.strict_restarts or not res.used["failover"]):
                allowed = 1 + wakes.get(mid, 0) + rec["reboot_gen"]
                seen = len(world.reported.get(mid, ()))
                if seen > allowed:
                    out.append(("unrequested-restart", "%s was started %d times, requests explain %d" % (mid, seen, allowed)))
        return out


# Every request carries the client's Idempotency-Key as its last field.
SCENARIOS = {
    "create": [[("create", "m1", "c1")]],
    "sleep-wake": [[("create", "m1", "c1"), ("sleep", "m1", "s1"), ("wake", "m1", "w1")]],
    "reboot": [[("create", "m1", "c1"), ("reboot", "m1", "r1")]],
    "delete": [[("create", "m1", "c1"), ("delete", "m1", "d1")]],
    "reboot-retry": [[("create", "m1", "c1"), ("reboot", "m1", "r1")], [("reboot", "m1", "r1")]],
    "sleep-race": [[("create", "m1", "c1"), ("sleep", "m1", "s1")], [("wake", "m1", "w1")]],
}


def random_scenario(rng, sessions=None, machines=2, length=4):
    """A generated scenario: every session creates or uses one of the machines
    and then issues random lifecycle requests. Used for the held-out runs."""
    sessions = sessions or rng.choice((2, 3))
    out = []
    for i in range(sessions):
        mid = "m%d" % rng.randrange(1, machines + 1)
        reqs = [("create", mid, "c-" + mid)]
        for j in range(rng.randrange(1, length + 1)):
            op = rng.choice(("sleep", "wake", "reboot", "reboot", "delete", "sleep", "wake"))
            key = "%s%d%d" % (op[0], i, j)
            if op == "reboot" and rng.random() < 0.3:
                key = "r-shared-" + mid       # two sessions retrying one reboot
            reqs.append((op, mid, key))
        out.append(reqs)
    return out


def fuzz_generated(plane_cls, seed, scenarios, runs, bounds, machines=2, length=4, p=0.15):
    """Random schedules over generated scenarios. Returns
    (schedules run, failing, {kind: count}, (scenario, choices) of the first failure)."""
    import random
    from crashpoint import run
    rng = random.Random(seed)
    model = MachineModel()
    n = bad = 0
    kinds, first = {}, None
    for _ in range(scenarios):
        sc = random_scenario(rng, machines=machines, length=length)
        for _ in range(runs):
            res = run(plane_cls, sc, (), bounds, rng, p, model)
            n += 1
            if res.violations:
                bad += 1
                for k in res.kinds:
                    kinds[k] = kinds.get(k, 0) + 1
                if first is None:
                    first = (sc, [c for c, _ in res.choices])
    return n, bad, kinds, first
