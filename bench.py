"""Run with: python bench.py   (about a minute). Writes bench_results.json."""
import json
import time

from crashpoint import Bounds, explore, fuzz, run, shrink
from planes import PLANES, SCENARIOS, FUZZ_SCENARIOS
from test_crashpoint import MUTANTS

FUZZ_RUNS = 20000
out = {"exhaustive": [], "fuzz": [], "mutants": [], "shrink": []}

print("Exhaustive: every schedule with at most 1 crash, 1 lost reply, 1 preemption\n")
print("| scenario | plane | schedules | failing | kinds | seconds |")
print("|---|---|---|---|---|---|")
total_n = total_t = 0
for sname, sc in SCENARIOS.items():
    for pname, plane in PLANES.items():
        t = time.time()
        n, bad, kinds, first = explore(plane, sc)
        dt = time.time() - t
        total_n += n
        total_t += dt
        print("| %s | %s | %d | %d | %s | %.2f |" % (sname, pname, n, bad, ", ".join(sorted(kinds)) or "none", dt))
        out["exhaustive"].append({"scenario": sname, "plane": pname, "schedules": n, "failing": bad,
                                  "kinds": kinds, "seconds": round(dt, 2)})
print("\n%d schedules in %.1f s (%d per second)" % (total_n, total_t, total_n / total_t))
out["exhaustive_total"] = {"schedules": total_n, "seconds": round(total_t, 1)}

print("\nRandom: %d schedules each, up to 3 crashes and 3 lost replies, any interleaving\n" % FUZZ_RUNS)
print("| scenario | plane | failing | kinds | seconds |")
print("|---|---|---|---|---|")
fuzz_sets = dict(FUZZ_SCENARIOS)
fuzz_sets["create-delete"] = SCENARIOS["create-delete"]
for sname, sc in fuzz_sets.items():
    for pname, plane in PLANES.items():
        t = time.time()
        n, bad, kinds, _ = fuzz(plane, sc, FUZZ_RUNS, seed=1)
        dt = time.time() - t
        print("| %s | %s | %d | %s | %.1f |" % (sname, pname, bad, ", ".join(sorted(kinds)) or "none", dt))
        out["fuzz"].append({"scenario": sname, "plane": pname, "runs": n, "failing": bad,
                            "kinds": kinds, "seconds": round(dt, 1)})

print("\nSeeded bugs in the reconcile plane (first scenario that exposes each)\n")
print("| seeded bug | caught by | schedules run before first failure | kinds |")
print("|---|---|---|---|")
for m in MUTANTS:
    row = None
    for sname, sc in SCENARIOS.items():
        n, bad, kinds, first = explore(m, sc, stop_at_first=True)
        if bad:
            row = (sname, n, run(m, sc, first).kinds)
            break
    print("| %s | %s | %s | %s |" % (m.__name__, row[0] if row else "NOT CAUGHT",
                                    row[1] if row else "", ", ".join(row[2]) if row else ""))
    out["mutants"].append({"mutant": m.__name__, "caught_by": row[0] if row else None,
                           "schedules_to_first": row[1] if row else None})

print("\nShrinking the first failing random schedule\n")
print("| scenario | plane | faults and preemptions before | after | steps |")
print("|---|---|---|---|---|")
fb = Bounds(crashes=3, lost=3, preempts=10 ** 6)
for sname, sc in FUZZ_SCENARIOS.items():
    for pname in ("naive", "journal"):
        first = fuzz(PLANES[pname], sc, FUZZ_RUNS, seed=1)[3]
        small = shrink(PLANES[pname], sc, first, fb)
        res = run(PLANES[pname], sc, small, fb)
        row = {"scenario": sname, "plane": pname, "before": sum(1 for c in first if c),
               "after": sum(1 for c in small if c), "steps": res.steps}
        out["shrink"].append(row)
        print("| %s | %s | %d | %d | %d |" % (sname, pname, row["before"], row["after"], row["steps"]))

json.dump(out, open("bench_results.json", "w"), indent=1)
