"""Run with: python bench.py   (several minutes). Writes bench_results.json.

All inputs are synthetic: hand-written scenarios, generated scenarios, and a
simulated cluster. Every number in the README comes from this file's output.
"""
import json
import sys
import time

from crashpoint import Bounds, explore, run, shrink
import planes as v1
from machines import MachineModel, SCENARIOS, fuzz_generated
from machine_planes import Fenced, Unfenced, FOUND, MUTANTS

M = MachineModel()
ONE = Bounds(1, 1, 1, 1, 1, total=1)
HELD_OUT = dict(seed=2027, scenarios=40, runs=300, machines=3, length=5, p=0.2)
HELD_OUT_BOUNDS = Bounds(crashes=2, lost=2, preempts=10 ** 6, failovers=2, node_faults=2)
out = {"note": "synthetic scenarios and a simulated cluster", "python": sys.version.split()[0]}
t_all = time.time()


def table(title, header):
    print("\n" + title + "\n")
    print("| " + " | ".join(header) + " |")
    print("|" + "---|" * len(header))


def row(*cells):
    print("| " + " | ".join(str(c) for c in cells) + " |", flush=True)


def kindstr(kinds):
    return ", ".join("%s %d" % kv for kv in sorted(kinds.items())) or "none"


# ---- A. the one-node model from the first version, unchanged bounds ----
table("A. One-node model, at most 1 crash, 1 lost reply, 1 preemption", ["scenario", "plane", "schedules", "failing", "kinds"])
out["v1_exhaustive"], tot = [], {}
for sname, sc in v1.SCENARIOS.items():
    for pname, plane in v1.PLANES.items():
        n, bad, kinds, _ = explore(plane, sc)
        row(sname, pname, n, bad, kindstr(kinds))
        out["v1_exhaustive"].append({"scenario": sname, "plane": pname, "schedules": n, "failing": bad, "kinds": kinds})
        t = tot.setdefault(pname, [0, 0])
        t[0] += n
        t[1] += bad
out["v1_totals"] = dict((p, {"schedules": t[0], "failing": t[1]}) for p, t in tot.items())
print("\ntotals: " + ", ".join("%s %d of %d" % (p, t[1], t[0]) for p, t in tot.items()))

# ---- B. the plane that passed in A, with a failover that leaves old tasks running ----
table("B. One-node reconcile plane, 1 failover (old tasks keep running) and 1 preemption",
      ["scenario", "schedules", "failing", "kinds"])
out["v1_reconcile_failover"], tn, tb = [], 0, 0
for sname, sc in v1.SCENARIOS.items():
    n, bad, kinds, _ = explore(v1.Reconcile, sc, Bounds(0, 0, 1, 1, 0))
    row(sname, n, bad, kindstr(kinds))
    out["v1_reconcile_failover"].append({"scenario": sname, "schedules": n, "failing": bad, "kinds": kinds})
    tn += n
    tb += bad
out["v1_reconcile_failover_total"] = {"schedules": tn, "failing": tb}
print("\ntotal: %d of %d" % (tb, tn))

# ---- C. machine model, development scenarios, exhaustive ----
table("C. Machine model, every schedule with any single crash, lost reply, preemption, failover or node partition",
      ["scenario", "plane", "schedules", "failing", "kinds", "seconds"])
out["machines_exhaustive"] = []
tot = {"unfenced": [0, 0], "fenced": [0, 0]}
for sname, sc in SCENARIOS.items():
    for pname, plane in (("unfenced", Unfenced), ("fenced", Fenced)):
        t = time.time()
        n, bad, kinds, _ = explore(plane, sc, ONE, model=M)
        dt = time.time() - t
        row(sname, pname, n, bad, kindstr(kinds), "%.1f" % dt)
        out["machines_exhaustive"].append({"scenario": sname, "plane": pname, "schedules": n, "failing": bad,
                                           "kinds": kinds, "seconds": round(dt, 1)})
        tot[pname][0] += n
        tot[pname][1] += bad
out["machines_totals"] = dict((p, {"schedules": t[0], "failing": t[1]}) for p, t in tot.items())
print("\ntotals: " + ", ".join("%s %d of %d" % (p, t[1], t[0]) for p, t in tot.items()))

t = time.time()
n, bad, kinds, _ = explore(Fenced, SCENARIOS["create"], Bounds(1, 1, 1, 1, 1, total=2), model=M)
out["machines_two_faults_create"] = {"schedules": n, "failing": bad, "kinds": kinds, "seconds": round(time.time() - t, 1)}
print("\nfenced, scenario create, any two faults: %d schedules, %d failing" % (n, bad))

# ---- D. buggy planes: enumeration on development scenarios, random on held-out ----
table("D. Planes with a known bug", ["plane", "origin", "first caught in", "schedules before", "fault", "kinds",
                                      "held-out failing of %d" % (HELD_OUT["scenarios"] * HELD_OUT["runs"])])
out["buggy"] = []
for plane, origin in [(Unfenced, "first design")] + [(p, "found in my fix") for p in FOUND] + [(p, "seeded") for p in MUTANTS]:
    hit = None
    for sname in ("create", "delete", "reboot", "sleep-wake"):
        n, bad, kinds, first = explore(plane, SCENARIOS[sname], ONE, stop_at_first=True, model=M)
        if bad:
            res = run(plane, SCENARIOS[sname], shrink(plane, SCENARIOS[sname], first, ONE, model=M), ONE, model=M)
            hit = (sname, n, ([k for k, v in res.used.items() if v] or ["none"])[0], res.kinds)
            break
    hn, hbad, hkinds, _ = fuzz_generated(plane, bounds=HELD_OUT_BOUNDS, **HELD_OUT)
    row(plane.__name__, origin, hit[0] if hit else "NOT CAUGHT", hit[1] if hit else "", hit[2] if hit else "",
        ", ".join(hit[3]) if hit else "", hbad)
    out["buggy"].append({"plane": plane.__name__, "origin": origin, "caught_in": hit and hit[0],
                         "schedules_before": hit and hit[1], "fault": hit and hit[2], "kinds": hit and hit[3],
                         "held_out_runs": hn, "held_out_failing": hbad, "held_out_kinds": hkinds})

# ---- E. held-out: generated scenarios the fenced plane was never developed against ----
t = time.time()
n, bad, kinds, _ = fuzz_generated(Fenced, bounds=HELD_OUT_BOUNDS, **HELD_OUT)
out["held_out"] = dict(HELD_OUT, bounds="2 crashes, 2 lost replies, 2 failovers, 2 node partitions, any interleaving",
                       schedules=n, failing=bad, kinds=kinds, seconds=round(time.time() - t, 1))
print("\nE. Held-out, fenced: %d random schedules over %d generated scenarios (seed %d, up to 3 machines), %d failing %s"
      % (n, HELD_OUT["scenarios"], HELD_OUT["seed"], bad, kindstr(kinds) if bad else ""))

# ---- F. the gap I did not close ----
table("F. Fenced plane, 1 failover, with the unrequested-restart check kept on", ["scenario", "schedules", "failing", "kinds"])
strict = MachineModel(strict_restarts=True)
out["known_gap"], tn, tb = [], 0, 0
for sname in ("create", "delete", "reboot", "sleep-wake"):
    n, bad, kinds, _ = explore(Fenced, SCENARIOS[sname], Bounds(0, 0, 0, 1, 0), model=strict)
    row(sname, n, bad, kindstr(kinds))
    out["known_gap"].append({"scenario": sname, "schedules": n, "failing": bad, "kinds": kinds})
    tn += n
    tb += bad
out["known_gap_total"] = {"schedules": tn, "failing": tb}
print("\ntotal: %d of %d" % (tb, tn))

out["seconds"] = round(time.time() - t_all, 1)
print("\nbench took %.0f s" % out["seconds"])
with open("bench_results.json", "w") as f:
    json.dump(out, f, indent=1)
