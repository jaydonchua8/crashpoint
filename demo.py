"""Run with: python demo.py   (well under a minute, nothing to install)

Shows one failing schedule for my first failover design, the three bugs the
checker then found in my fix, and the checked result for the final version.
All data is synthetic. Nothing here talks to a real machine.
"""
import json
import time

from crashpoint import Bounds, explore, run, shrink
from machines import MachineModel, SCENARIOS, fuzz_generated, to_api
from machine_planes import Fenced, Unfenced, StaleKeyRead, WakeReusesEpoch, UnkeyedRequests

M = MachineModel()
ONE = Bounds(1, 1, 1, 1, 1, total=1)
t0 = time.time()


def failing(plane, name, bounds=ONE):
    n, bad, kinds, first = explore(plane, SCENARIOS[name], bounds, stop_at_first=True, model=M)
    small = shrink(plane, SCENARIOS[name], first, bounds, model=M)
    return n, run(plane, SCENARIOS[name], small, bounds, model=M)


print("1. First design: if the node is unreachable, start the machine on the other node.")
print("   One machine, one create request, one node partition:\n")
n, res = failing(Unfenced, "create", Bounds(0, 0, 0, 0, 1))
for e in res.events:
    print("     " + e)
for kind, detail in res.violations:
    print("     VIOLATION %s: %s" % (kind, detail))
print("\n   Two VMs could write the same volume. Found after %d schedules.\n" % n)

print("2. Bugs the checker found in my fenced version before it passed:\n")
for plane, name, what in (
        (StaleKeyRead, "reboot", "reboot key checked in an earlier read"),
        (WakeReusesEpoch, "sleep-wake", "wake kept the epoch the machine slept in"),
        (UnkeyedRequests, "sleep-wake", "sleep/wake/delete applied without recording the key")):
    n, res = failing(plane, name)
    fault = [k for k, v in res.used.items() if v] or ["none"]
    print("   %-52s -> %-13s (fault: %s, schedule %d, %d steps)" % (
        what, "+".join(res.kinds), fault[0], n, res.steps))

print("\n3. Final version, every schedule with any single fault or preemption:\n")
total = 0
for name in ("create", "delete", "reboot"):
    n, bad, kinds, _ = explore(Fenced, SCENARIOS[name], ONE, model=M)
    total += n
    print("   %-8s %5d schedules, %d failing" % (name, n, bad))
b = Bounds(crashes=2, lost=2, preempts=10 ** 6, failovers=1, node_faults=2)
n, bad, kinds, _ = fuzz_generated(Fenced, 99, 10, 200, b)
print("   random   %5d schedules over 10 generated scenarios, %d failing" % (n, bad))

world = M.new_world()
model = MachineModel()
model.new_world = lambda: world
run(Fenced, SCENARIOS["reboot"], model=model)
print("\n4. The machine record after create + reboot, in the field names of the public API:\n")
print("   " + json.dumps(to_api("m1", world.store["m1"])))
print("\ndone in %.1f s" % (time.time() - t0))
