"""crashpoint: enumerate crashes, lost replies, interleavings, control-plane
failovers and node partitions against a small machine-lifecycle control
plane, and check what is left behind.

Control-plane code is written as generators. Every store or node call is a
`yield`, so the scheduler decides what runs next and can kill the process or
drop a reply at exactly that point. World state (the record store and the
node's resources) survives a crash. In-flight handlers do not.
"""
import random
import sys

STEP_CAP = 400


class LostReply(Exception):
    """The node applied the call but the caller never saw the answer."""


class WorldFault(Exception):
    """Raised by a world method when the call could not be delivered at all
    (for example the target node is partitioned). Nothing was applied."""


class World:
    """Everything that survives a control-plane crash."""

    def __init__(self):
        self.store = {}   # machine id -> record dict (with 'ver')
        self.vols = {}    # volume name -> {'mid', 'uid'}
        self.ips = {}     # owner name -> {'mid', 'uid', 'addr'}
        self.vms = {}     # vm name -> {'mid', 'uid', 'vol', 'ip'}
        self.seq = 0
        self.ver = 0

    # ---- record store ----
    def get(self, mid):
        rec = self.store.get(mid)
        return dict(rec) if rec else None

    def insert(self, mid, rec):
        if mid in self.store:
            return False
        self.ver += 1
        self.store[mid] = dict(rec, ver=self.ver)
        return True

    def put(self, mid, changes):
        if mid not in self.store:
            return False
        self.ver += 1
        self.store[mid].update(changes, ver=self.ver)
        return True

    def cas(self, mid, ver, changes):
        rec = self.store.get(mid)
        if rec is None or rec["ver"] != ver:
            return False
        return self.put(mid, changes)

    def remove(self, mid):
        self.store.pop(mid, None)
        return True

    def cas_remove(self, mid, ver):
        rec = self.store.get(mid)
        if rec is None or rec["ver"] != ver:
            return False
        del self.store[mid]
        return True

    # ---- node ----
    def create_volume(self, name, mid, uid):
        if name is None:                      # anonymous: a new one per call
            self.seq += 1
            name = "vol%d" % self.seq
        self.vols.setdefault(name, {"mid": mid, "uid": uid})
        return name

    def alloc_ip(self, owner, mid, uid):
        if owner not in self.ips:
            self.seq += 1
            self.ips[owner] = {"mid": mid, "uid": uid, "addr": "10.0.0.%d" % self.seq}
        return self.ips[owner]["addr"]

    def boot_vm(self, name, mid, uid, vol, ip_owner):
        if name in self.vms:
            return True
        if vol not in self.vols or ip_owner not in self.ips:
            return False
        self.vms[name] = {"mid": mid, "uid": uid, "vol": vol, "ip": ip_owner}
        return True

    def kill_vm(self, name):
        self.vms.pop(name, None)
        return True

    def release_ip(self, owner):
        if any(v["ip"] == owner for v in self.vms.values()):
            return False
        self.ips.pop(owner, None)
        return True

    def delete_volume(self, name):
        if any(v["vol"] == name for v in self.vms.values()):
            return False
        self.vols.pop(name, None)
        return True

    def observe(self, mid):
        pick = lambda d: sorted((n, r["uid"]) for n, r in d.items() if r["mid"] == mid)
        return {"vms": pick(self.vms), "ips": pick(self.ips), "vols": pick(self.vols)}

    def machines(self):
        mids = set(self.store)
        for d in (self.vols, self.ips, self.vms):
            mids.update(r["mid"] for r in d.values())
        return sorted(mids)


def S(method, *args):
    return ("store", method, args)


def N(method, *args):
    return ("node", method, args)


def call(effect):
    """Issue a node call and repeat it until a reply arrives."""
    while True:
        try:
            return (yield effect)
        except LostReply:
            continue


class Bounds:
    """Fault budget for one schedule.

    crashes:    the control plane dies, in-flight work is dropped, it restarts
    lost:       a node or storage call is applied but its reply is dropped
    preempts:   switches away from a task that could have kept running
    failovers:  a new control-plane instance takes over while the old one's
                in-flight tasks keep running as zombies (a paused or
                partitioned old leader)
    node_faults: environment faults offered by the model (a node partition)
    total:      optional cap on all of the above added together
    """

    def __init__(self, crashes=1, lost=1, preempts=1, failovers=0, node_faults=0, total=None):
        self.crashes, self.lost, self.preempts = crashes, lost, preempts
        self.failovers, self.node_faults = failovers, node_faults
        self.total = total      # cap on faults and preemptions together, any mix


class ToyModel:
    """World and invariants of the first version: one node, three resource
    kinds per machine. Other models (machines.py) replace these five hooks."""

    def new_world(self):
        return World()

    def env_options(self, world, budget):
        """[(label, function(world), costs_a_node_fault)] offered at this step."""
        return []

    def step_check(self, world, violations):
        for mid in world.machines():
            n = sum(1 for v in world.vms.values() if v["mid"] == mid)
            if n > 1 and ("two-vms", mid) not in violations:
                violations.append(("two-vms", mid))

    def settle(self, world):
        """Called when nothing is running. Return True if the environment
        changed something and the control plane should get another turn."""
        return False

    def final_check(self, world, plane, scenario, res):
        return check(world, plane, scenario)


class Result:
    def __init__(self):
        self.choices = []      # (index chosen, number of options)
        self.events = []       # readable trace
        self.violations = []   # (kind, detail)
        self.acks = []         # (session index, request, reply) in order
        self.used = {"crash": 0, "lost": 0, "preempt": 0, "failover": 0, "node": 0}
        self.steps = 0

    @property
    def kinds(self):
        return sorted(set(k for k, _ in self.violations))


def run(plane_cls, scenario, prefix=(), bounds=None, rng=None, p=0.15, model=None):
    """Execute one schedule.

    scenario: list of sessions, each a list of requests (name, args...). A
    request is dispatched to the plane method of the same name. A session
    sends its next request only after the previous one was acknowledged, and
    resends the same request after a crash, a failover or an error.
    prefix: choice indices to replay. After the prefix, the default (index 0:
    keep running the same task, no fault) is taken. With rng, each choice is
    instead random with probability p.
    model: world factory, environment faults and invariants (ToyModel if None).
    """
    bounds = bounds or Bounds()
    model = model or ToyModel()
    world, plane, res = model.new_world(), plane_cls(), Result()
    tasks = {}                       # id -> [gen, pending effect, label, session, key]
    pos = [0] * len(scenario)
    budget = {"crash": bounds.crashes, "lost": bounds.lost, "preempt": bounds.preempts,
              "failover": bounds.failovers, "node": bounds.node_faults}
    counter = [0]
    last = [None]

    def spend(what):
        budget[what] -= 1
        res.used[what] += 1
        if bounds.total is not None and sum(res.used.values()) >= bounds.total:
            for k in budget:
                budget[k] = 0

    def finish(tid, value):
        _, _, label, sess, _ = tasks.pop(tid)
        if sess is not None:
            if value in ("ok", "conflict"):
                res.acks.append((sess, scenario[sess][pos[sess]], value))
                pos[sess] += 1
            res.events.append("%s -> %s" % (label, value))

    def start(gen, label, sess=None, key=None):
        counter[0] += 1
        tid = counter[0]
        tasks[tid] = [gen, None, label, sess, key]
        try:
            tasks[tid][1] = next(gen)
        except StopIteration as stop:
            finish(tid, stop.value)

    def spawn():
        active = set(t[3] for t in tasks.values())
        for i, reqs in enumerate(scenario):
            if pos[i] < len(reqs) and i not in active:
                req = reqs[pos[i]]
                label = "%s:%s" % ("ABCDEFGH"[i], " ".join(req))
                start(getattr(plane, req[0])(*req[1:]), label, sess=i)
        keys = set(t[4] for t in tasks.values())
        for mid in plane.wants(world):
            if mid not in keys:
                start(plane.reconcile(mid), "reconcile " + mid, key=mid)

    def step(tid, lose):
        gen, (target, method, args), label = tasks[tid][0], tasks[tid][1], tasks[tid][2]
        fault = None
        try:
            value = getattr(world, method)(*args)
        except WorldFault as exc:
            fault, lose = exc, False
        shown = ", ".join("-" if a is None else str(a) for a in args if not isinstance(a, (dict, tuple)))
        note = "  [%s]" % fault if fault else ("  [reply lost]" if lose else "")
        res.events.append("%s: %s(%s)%s" % (label, method, shown, note))
        model.step_check(world, res.violations)
        try:
            if fault is not None:
                tasks[tid][1] = gen.throw(fault)
            elif lose:
                tasks[tid][1] = gen.throw(LostReply())
            else:
                tasks[tid][1] = gen.send(value)
        except StopIteration as stop:
            finish(tid, stop.value)
        except (LostReply, WorldFault):
            finish(tid, "error")
        return lose

    while True:
        spawn()
        if not tasks:
            if model.settle(world):
                res.events.append("--- environment settles: every node reachable again ---")
                continue
            break
        if res.steps >= STEP_CAP:
            res.violations.append(("no-convergence", "step cap"))
            return res
        ids = sorted(tasks)
        cont = last[0] if last[0] in tasks else None
        options = []
        if cont is not None:
            options.append(("step", cont, False))
        if cont is None or budget["preempt"] > 0:
            options += [("step", t, False) for t in ids if t != cont]
        if budget["lost"] > 0:
            options += [("step", t, True) for t in ids if tasks[t][1][0] != "store"
                        and (t == cont or cont is None or budget["preempt"] > 0)]
        if budget["crash"] > 0 and res.steps > 0:
            options.append(("crash", None, False))
        if budget["failover"] > 0 and res.steps > 0:
            options.append(("failover", None, False))
        for label, fn, costs in model.env_options(world, budget):
            if not costs or budget["node"] > 0:
                options.append(("env", (label, fn, costs), False))
        i = len(res.choices)
        if i < len(prefix):
            c = prefix[i]
        elif rng is not None:
            c = rng.randrange(len(options)) if rng.random() < p else 0
        else:
            c = 0
        res.choices.append((c, len(options)))
        kind, tid, lose = options[c]
        res.steps += 1
        if kind == "crash":
            spend("crash")
            tasks.clear()
            last[0] = None
            res.events.append("*** control plane crashes and restarts ***")
            for label, gen in plane.recover(world):
                start(gen, label)
            continue
        if kind == "failover":
            # The old instance is not dead. Its tasks keep running, but nobody
            # waits for their replies and the new instance does not know them.
            spend("failover")
            for t in tasks.values():
                if not t[2].startswith("zombie "):
                    t[2] = "zombie " + t[2]
                t[3] = t[4] = None
            last[0] = None
            res.events.append("*** a new control-plane instance takes over, old tasks keep running ***")
            for label, gen in plane.recover(world):
                start(gen, label)
            continue
        if kind == "env":
            label, fn, costs = tid
            if costs:
                spend("node")
            fn(world)
            res.events.append("*** %s ***" % label)
            model.step_check(world, res.violations)
            continue
        if cont is not None and tid != cont:
            spend("preempt")
        last[0] = tid
        if step(tid, lose):
            spend("lost")

    res.violations += model.final_check(world, plane, scenario, res)
    return res


def check(world, plane, scenario):
    """Invariants once every request is acknowledged and nothing is running."""
    out = []
    touched = {}
    for i, reqs in enumerate(scenario):
        for req in reqs:
            touched.setdefault(req[1], set()).add(i)
    final = {}
    for i, reqs in enumerate(scenario):
        for req in reqs:
            if len(touched[req[1]]) == 1:
                final[req[1]] = req[0]
    for mid in world.machines():
        rec = world.store.get(mid)
        obs = world.observe(mid)
        total = sum(len(v) for v in obs.values())
        if rec is None:
            if total:
                out.append(("orphan", "%s has no record but %d resources remain" % (mid, total)))
            continue
        counts = [sum(1 for _, uid in obs[k] if uid == rec["uid"]) for k in ("vms", "ips", "vols")]
        if max(counts) > 1 or total > sum(counts):
            out.append(("leak", "%s holds %d resources, expected 3" % (mid, total)))
        if min(counts) < 1 or not plane.settled(rec):
            out.append(("incomplete", "%s record says %s, resources vm/ip/vol=%s" % (
                mid, rec.get("state") or rec.get("want"), counts)))
    for mid, op in final.items():
        present = mid in world.store
        if op == "create" and not present:
            out.append(("lost", "%s was acknowledged as created but has no record" % mid))
        if op == "delete" and present:
            out.append(("lost", "%s was acknowledged as deleted but still has a record" % mid))
    return out


def explore(plane_cls, scenario, bounds=None, stop_at_first=False, model=None):
    """Depth-first over every schedule inside the bounds. Returns
    (schedules run, failing schedules, {kind: count}, first failing prefix)."""
    prefix, n, bad, kinds, first = [], 0, 0, {}, None
    while True:
        res = run(plane_cls, scenario, prefix, bounds, model=model)
        n += 1
        if res.violations:
            bad += 1
            for k in res.kinds:
                kinds[k] = kinds.get(k, 0) + 1
            if first is None:
                first = [c for c, _ in res.choices]
                if stop_at_first:
                    break
        i = len(res.choices) - 1
        while i >= 0 and res.choices[i][0] + 1 >= res.choices[i][1]:
            i -= 1
        if i < 0:
            break
        prefix = [c for c, _ in res.choices[:i]] + [res.choices[i][0] + 1]
    return n, bad, kinds, first


def fuzz(plane_cls, scenario, runs, seed=0, bounds=None, p=0.15, model=None):
    """Random schedules with a larger fault budget than explore can afford."""
    bounds = bounds or Bounds(crashes=3, lost=3, preempts=10 ** 6)
    rng = random.Random(seed)
    bad, kinds, first = 0, {}, None
    for _ in range(runs):
        res = run(plane_cls, scenario, (), bounds, rng, p, model)
        if res.violations:
            bad += 1
            for k in res.kinds:
                kinds[k] = kinds.get(k, 0) + 1
            if first is None:
                first = [c for c, _ in res.choices]
    return runs, bad, kinds, first


def shrink(plane_cls, scenario, choices, bounds=None, model=None):
    """Reset non-default choices to the default while the run still fails."""
    choices = list(choices)

    def fails(cs):
        try:
            return bool(run(plane_cls, scenario, cs, bounds, model=model).violations)
        except IndexError:        # the edit changed how many options exist
            return False

    changed = True
    while changed:
        changed = False
        for i in range(len(choices)):
            if choices[i] != 0:
                trial = choices[:i] + [0]
                if fails(trial):
                    choices, changed = trial, True
                    break
                trial = choices[:i] + [0] + choices[i + 1:]
                if fails(trial):
                    choices, changed = trial, True
                    break
    while choices and choices[-1] == 0:
        choices.pop()
    return choices


def main(argv):
    # Import by module name so there is one copy of LostReply when run as a script.
    from crashpoint import Bounds, explore, shrink, run
    import planes
    import machines
    import machine_planes
    v1 = len(argv) == 3 and argv[1] in planes.PLANES and argv[2] in planes.SCENARIOS
    v2 = len(argv) == 3 and argv[1] in machine_planes.PLANES and argv[2] in machines.SCENARIOS
    if not (v1 or v2):
        print("usage: python crashpoint.py <plane> <scenario>")
        print("  one-node model:  planes %s, scenarios %s" % ("|".join(planes.PLANES), "|".join(planes.SCENARIOS)))
        print("  machine model:   planes %s, scenarios %s" % ("|".join(machine_planes.PLANES), "|".join(machines.SCENARIOS)))
        return 2
    if v1:
        plane, scenario = planes.PLANES[argv[1]], planes.SCENARIOS[argv[2]]
        bounds, model = Bounds(), None
        print("bounds: at most 1 crash, 1 lost reply and 1 preemption")
    else:
        plane, scenario = machine_planes.PLANES[argv[1]], machines.SCENARIOS[argv[2]]
        bounds, model = Bounds(1, 1, 1, 1, 1, total=1), machines.MachineModel()
        print("bounds: any single crash, lost reply, preemption, failover or node partition")
    n, bad, kinds, first = explore(plane, scenario, bounds, model=model)
    print("%d schedules, %d failing %s" % (n, bad, kinds or ""))
    if first is not None:
        small = shrink(plane, scenario, first, bounds, model=model)
        res = run(plane, scenario, small, bounds, model=model)
        print("\na failing schedule after shrinking (%d steps):" % res.steps)
        for e in res.events:
            print("  " + e)
        for kind, detail in res.violations:
            print("  VIOLATION %s: %s" % (kind, detail))
    return 1 if bad else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
