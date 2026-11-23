"""Run with: python test_crashpoint.py"""
import random
import unittest

from crashpoint import Bounds, World, explore, fuzz, run, shrink
from planes import PLANES, SCENARIOS, FUZZ_SCENARIOS, Naive, Journal, Reconcile


class BlindRemove(Reconcile):
    """Seeded bug: drop the record without checking its version."""
    def reconcile(self, mid):
        gen = Reconcile.reconcile(self, mid)
        eff = next(gen)
        while True:
            if eff[0] == "store" and eff[1] == "cas_remove":
                eff = ("store", "remove", (eff[2][0],))
            try:
                eff = gen.send((yield eff))
            except StopIteration:
                return


class KeepsOldIncarnation(Reconcile):
    """Seeded bug: never clean up resources of an earlier incarnation."""
    def reconcile(self, mid):
        gen = Reconcile.reconcile(self, mid)
        eff = next(gen)
        rec = None
        while True:
            value = yield eff
            if eff[1] == "get":
                rec = value
            if eff[1] == "observe" and rec and rec["want"] == "present":
                value = {k: [(n, u) for n, u in v if u == rec["uid"]] for k, v in value.items()}
            try:
                eff = gen.send(value)
            except StopIteration:
                return


class TwoWorkers(Reconcile):
    """Seeded bug: after a restart, a second reconcile task runs for each machine."""
    def recover(self, world):
        return [("stale reconcile " + m, self.reconcile(m)) for m in sorted(world.store)]


MUTANTS = [BlindRemove, KeepsOldIncarnation, TwoWorkers]


class NodeModel(unittest.TestCase):
    def test_named_calls_are_idempotent(self):
        w = World()
        self.assertEqual(w.create_volume("v", "m", "k"), "v")
        self.assertEqual(w.create_volume("v", "m", "k"), "v")
        a = w.alloc_ip("vm", "m", "k")
        self.assertEqual(w.alloc_ip("vm", "m", "k"), a)
        self.assertTrue(w.boot_vm("vm", "m", "k", "v", "vm"))
        self.assertTrue(w.boot_vm("vm", "m", "k", "v", "vm"))
        self.assertEqual((len(w.vols), len(w.ips), len(w.vms)), (1, 1, 1))

    def test_anonymous_volume_is_new_each_call(self):
        w = World()
        self.assertNotEqual(w.create_volume(None, "m", "k"), w.create_volume(None, "m", "k"))

    def test_teardown_order_is_enforced(self):
        w = World()
        w.create_volume("v", "m", "k"); w.alloc_ip("vm", "m", "k"); w.boot_vm("vm", "m", "k", "v", "vm")
        self.assertFalse(w.delete_volume("v"))
        self.assertFalse(w.release_ip("vm"))
        w.kill_vm("vm")
        self.assertTrue(w.release_ip("vm"))
        self.assertTrue(w.delete_volume("v"))

    def test_boot_needs_volume_and_ip(self):
        self.assertFalse(World().boot_vm("vm", "m", "k", "v", "vm"))

    def test_cas_rejects_stale_version_and_reinsert(self):
        w = World()
        w.insert("m", {"uid": "a"})
        ver = w.get("m")["ver"]
        w.remove("m"); w.insert("m", {"uid": "b"})
        self.assertFalse(w.cas("m", ver, {"x": 1}))
        self.assertFalse(w.cas_remove("m", ver))
        self.assertTrue(w.cas("m", w.get("m")["ver"], {"x": 1}))


class Scheduler(unittest.TestCase):
    def test_fault_free_run_passes_for_every_plane(self):
        for name, plane in PLANES.items():
            for sname, sc in list(SCENARIOS.items()) + list(FUZZ_SCENARIOS.items()):
                res = run(plane, sc)
                self.assertEqual(res.violations, [], (name, sname))

    def test_replay_is_deterministic(self):
        rng = random.Random(7)
        b = Bounds(3, 3, 99)
        a = run(Journal, FUZZ_SCENARIOS["three-clients"], (), b, rng)
        again = run(Journal, FUZZ_SCENARIOS["three-clients"], [c for c, _ in a.choices], b)
        self.assertEqual(a.events, again.events)
        self.assertEqual(a.violations, again.violations)

    def test_budgets_are_respected(self):
        rng = random.Random(1)
        for _ in range(200):
            res = run(Reconcile, SCENARIOS["recreate"], (), Bounds(1, 1, 1), rng, p=0.5)
            self.assertLessEqual(sum("crashes" in e for e in res.events), 1)
            self.assertLessEqual(sum("reply lost" in e for e in res.events), 1)

    def test_lost_reply_still_applies_the_call(self):
        # choice 1 at the third step of naive create is "create_volume, reply lost"
        res = run(Naive, SCENARIOS["create"], [0, 1], Bounds(0, 1, 0))
        self.assertIn("[reply lost]", res.events[1])
        self.assertIn("leak", res.kinds)

    def test_crash_drops_handlers_but_keeps_state(self):
        n, bad, kinds, first = explore(Naive, SCENARIOS["create"], Bounds(1, 0, 0))
        self.assertGreater(bad, 0)
        res = run(Naive, SCENARIOS["create"], shrink(Naive, SCENARIOS["create"], first, Bounds(1, 0, 0)), Bounds(1, 0, 0))
        self.assertTrue(any("crashes" in e for e in res.events))
        self.assertTrue(res.violations)

    def test_explore_counts_without_faults(self):
        # one session, no faults, no preemption: exactly one schedule
        self.assertEqual(explore(Journal, SCENARIOS["create-delete"], Bounds(0, 0, 0))[0], 1)

    def test_shrunk_schedule_still_fails_and_is_not_longer(self):
        b = Bounds()
        _, _, _, first = explore(Journal, SCENARIOS["delete-race"], b, stop_at_first=True)
        small = shrink(Journal, SCENARIOS["delete-race"], first, b)
        self.assertTrue(run(Journal, SCENARIOS["delete-race"], small, b).violations)
        self.assertLessEqual(sum(1 for c in small if c), sum(1 for c in first if c))


class Findings(unittest.TestCase):
    def test_naive_fails_every_scenario(self):
        for sname, sc in SCENARIOS.items():
            self.assertGreater(explore(Naive, sc, stop_at_first=True)[1], 0, sname)

    def test_journal_survives_single_create_and_duplicate_create(self):
        self.assertEqual(explore(Journal, SCENARIOS["create"])[1], 0)
        self.assertEqual(explore(Journal, SCENARIOS["retry-race"])[1], 0)

    def test_journal_resume_races_the_client_retry(self):
        b = Bounds(crashes=1, lost=0, preempts=1)
        n, bad, kinds, first = explore(Journal, SCENARIOS["create-delete"], b)
        self.assertGreater(bad, 0)
        res = run(Journal, SCENARIOS["create-delete"], shrink(Journal, SCENARIOS["create-delete"], first, b), b)
        self.assertTrue(any(e.startswith("resume") for e in res.events))
        self.assertEqual(res.kinds, ["orphan"])

    def test_reconcile_passes_small_scenarios(self):
        for sname in ("create", "create-delete", "retry-race", "delete-race"):
            n, bad, _, _ = explore(Reconcile, SCENARIOS[sname])
            self.assertEqual(bad, 0, sname)
            self.assertGreater(n, 50)

    def test_reconcile_passes_random_schedules(self):
        for sname, sc in FUZZ_SCENARIOS.items():
            self.assertEqual(fuzz(Reconcile, sc, 1500, seed=3)[1], 0, sname)

    def test_seeded_bugs_are_caught(self):
        for mutant in MUTANTS:
            hit = any(explore(mutant, sc, stop_at_first=True)[1] for sc in SCENARIOS.values())
            hit = hit or any(fuzz(mutant, sc, 3000, seed=1)[1] for sc in FUZZ_SCENARIOS.values())
            self.assertTrue(hit, mutant.__name__)


if __name__ == "__main__":
    unittest.main()
