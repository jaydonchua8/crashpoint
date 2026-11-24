"""Run with: python -m unittest   (or python test_machines.py)"""
import random
import unittest

from crashpoint import Bounds, explore, run, shrink
from planes import Journal, Reconcile, SCENARIOS as V1_SCENARIOS
from machines import (Cluster, MachineModel, Unreachable, SCENARIOS, PHASES, DESIRED,
                      to_api, expected, random_scenario, fuzz_generated)
from machine_planes import (Fenced, Unfenced, StaleKeyRead, WakeReusesEpoch, UnkeyedRequests,
                            FOUND, MUTANTS)

M = MachineModel()
ONE = Bounds(1, 1, 1, 1, 1, total=1)      # any single fault or preemption


def first_failure(plane, scenario, bounds=ONE, model=M):
    n, bad, kinds, first = explore(plane, scenario, bounds, stop_at_first=True, model=model)
    if not bad:
        return None
    return run(plane, scenario, shrink(plane, scenario, first, bounds, model=model), bounds, model=model)


def rec(**kw):
    base = {"create_key": "c", "desired_state": "running", "phase": "accepted",
            "node": "n1", "epoch": 1, "reboot_gen": 0, "gen_applied": 0}
    base.update(kw)
    return base


class StoreModel(unittest.TestCase):
    def test_enums_match_the_public_sdk(self):
        self.assertEqual(DESIRED, ("running", "sleeping", "destroyed"))
        self.assertEqual(len(PHASES), 9)
        for p in ("accepted", "placement_pending", "starting", "running", "stopping",
                  "sleeping", "destroying", "destroyed", "failed"):
            self.assertIn(p, PHASES)

    def test_write_outside_the_enums_is_flagged(self):
        w = Cluster()
        w.insert("m", rec())
        self.assertEqual(w.flags, [])
        w.put("m", {"phase": "booting"})
        self.assertEqual(w.flags[0][0], "bad-enum")

    def test_cas_rejects_stale_version(self):
        w = Cluster()
        w.insert("m", rec())
        ver = w.get("m")["ver"]
        self.assertTrue(w.cas("m", ver, {"phase": "starting"}))
        self.assertFalse(w.cas("m", ver, {"phase": "running"}))
        self.assertEqual(w.get("m")["phase"], "starting")

    def test_cas_with_key_is_one_transaction(self):
        w = Cluster()
        w.insert("m", rec())
        self.assertTrue(w.cas("m", w.get("m")["ver"], {"reboot_gen": 1, "reboot_key": "r"}, ("r", "ok")))
        self.assertEqual(w.idem_get("r"), "ok")
        # fresh version, same key: refused, nothing written
        self.assertFalse(w.cas("m", w.get("m")["ver"], {"reboot_gen": 2, "reboot_key": "r"}, ("r", "ok")))
        self.assertEqual(w.get("m")["reboot_gen"], 1)
        self.assertEqual(w.bumps, {"r": 1})

    def test_first_saved_reply_wins(self):
        w = Cluster()
        self.assertEqual(w.idem_put("k", "conflict"), "conflict")
        self.assertEqual(w.idem_put("k", "ok"), "conflict")

    def test_api_projection_uses_public_field_names(self):
        w = Cluster()
        w.insert("m", rec(phase="running"))
        api = to_api("m", w.get("m"))
        self.assertEqual(sorted(api), ["desired_state", "machine_id", "phase", "status"])
        self.assertEqual(sorted(api["status"]), ["phase", "retryable", "revision"])
        self.assertIn(api["phase"], PHASES)


class NodeAndStorageModel(unittest.TestCase):
    def setUp(self):
        self.w = Cluster()
        self.w.insert("m", rec())
        self.w.vol_create("m")

    def report_running(self):
        self.w.put("m", {"phase": "running"})

    def test_boot_is_repeatable_and_takes_the_lease(self):
        self.assertTrue(self.w.boot_vm("n1", "m", 1))
        self.assertTrue(self.w.boot_vm("n1", "m", 1))
        self.assertEqual(self.w._writers("m"), [("n1", 1)])

    def test_boot_needs_a_volume(self):
        self.assertFalse(Cluster().boot_vm("n1", "m", 1))

    def test_newer_epoch_fences_the_older_vm(self):
        self.w.boot_vm("n1", "m", 1)
        self.assertTrue(self.w.boot_vm("n2", "m", 2))
        self.assertEqual(self.w._writers("m"), [("n2", 2)])
        self.assertIn("m", self.w.nodes["n1"]["vms"])      # still running, but cannot write

    def test_stale_epoch_cannot_boot(self):
        self.w.boot_vm("n2", "m", 2)
        self.assertFalse(self.w.boot_vm("n1", "m", 1))

    def test_graceful_stop_checkpoints_and_closes_the_epoch(self):
        self.w.boot_vm("n1", "m", 1)
        self.w.stop_vm("n1", "m", 1)
        self.assertEqual(self.w.vols["m"]["committed"], 1)
        self.assertFalse(self.w.boot_vm("n1", "m", 1))
        self.assertTrue(self.w.boot_vm("n1", "m", 2))
        self.assertEqual(self.w.nodes["n1"]["vms"]["m"]["base"], 1)   # restored from the checkpoint

    def test_stop_of_a_fenced_vm_does_not_checkpoint(self):
        self.w.boot_vm("n1", "m", 1)
        self.w.boot_vm("n2", "m", 2)
        self.w.stop_vm("n1", "m", 1)
        self.assertEqual(self.w.vols["m"]["committed"], 0)

    def test_stop_with_wrong_epoch_is_ignored(self):
        self.w.boot_vm("n1", "m", 2)
        self.w.stop_vm("n1", "m", 1)
        self.assertIn("m", self.w.nodes["n1"]["vms"])

    def test_hard_kill_of_a_used_writer_is_data_loss(self):
        self.w.boot_vm("n1", "m", 1)
        self.report_running()
        self.w.kill_vm("n1", "m", 1)
        self.assertEqual(self.w.flags[0][0], "data-loss")

    def test_hard_kill_before_running_was_reported_is_not(self):
        self.w.boot_vm("n1", "m", 1)
        self.w.kill_vm("n1", "m", 1)
        self.assertEqual(self.w.flags, [])

    def test_fencing_a_healthy_used_writer_is_data_loss(self):
        self.w.boot_vm("n1", "m", 1)
        self.report_running()
        self.w.seal("m", 1)
        self.assertEqual(self.w.flags[0][0], "data-loss")

    def test_fencing_after_a_partition_is_accepted(self):
        self.w.boot_vm("n1", "m", 1)
        self.report_running()
        self.w.partition("n1")
        self.w.seal("m", 1)
        self.assertEqual(self.w.flags, [])
        self.assertEqual(self.w._writers("m"), [])

    def test_seal_before_the_volume_exists_still_holds(self):
        w = Cluster()
        w.seal("m", 3)
        w.vol_create("m")
        self.assertFalse(w.boot_vm("n1", "m", 3))
        self.assertTrue(w.boot_vm("n1", "m", 4))

    def test_partitioned_node_refuses_calls_but_keeps_its_vm(self):
        self.w.boot_vm("n1", "m", 1)
        self.w.partition("n1")
        for call in (lambda: self.w.observe("n1", "m"), lambda: self.w.stop_vm("n1", "m", 1),
                     lambda: self.w.boot_vm("n1", "m", 1), lambda: self.w.kill_vm("n1", "m", 1)):
            self.assertRaises(Unreachable, call)
        self.assertIn("m", self.w.nodes["n1"]["vms"])
        self.w.heal("n1")
        self.assertEqual(self.w.observe("n1", "m")["epoch"], 1)


class ApiContract(unittest.TestCase):
    """Fault-free request sequences against the documented behaviour."""

    def replies(self, *reqs):
        res = run(Fenced, [list(reqs)], model=M)
        self.assertEqual(res.violations, [])
        return [reply for _, _, reply in res.acks]

    def test_reboot_needs_desired_state_running(self):
        self.assertEqual(self.replies(("create", "m1", "c"), ("sleep", "m1", "s"), ("reboot", "m1", "r")),
                         ["ok", "ok", "conflict"])

    def test_same_reboot_key_restarts_once(self):
        sc = [[("create", "m1", "c"), ("reboot", "m1", "r"), ("reboot", "m1", "r")]]
        world = M.new_world()
        model = MachineModel()
        model.new_world = lambda: world
        res = run(Fenced, sc, model=model)
        self.assertEqual([r for _, _, r in res.acks], ["ok", "ok", "ok"])
        self.assertEqual(world.store["m1"]["reboot_gen"], 1)
        self.assertEqual(world.bumps, {"r": 1})

    def test_new_reboot_key_restarts_again(self):
        sc = [[("create", "m1", "c"), ("reboot", "m1", "r1"), ("reboot", "m1", "r2")]]
        world = M.new_world()
        model = MachineModel()
        model.new_world = lambda: world
        res = run(Fenced, sc, model=model)
        self.assertEqual(res.violations, [])
        accepted = sum(1 for _, req, reply in res.acks if req[0] == "reboot" and reply == "ok")
        self.assertGreaterEqual(accepted, 1)
        self.assertEqual(world.store["m1"]["reboot_gen"], accepted)
        self.assertEqual(sorted(world.bumps.values()), [1] * accepted)

    def test_nothing_works_after_delete_except_delete(self):
        self.assertEqual(self.replies(("create", "m1", "c"), ("delete", "m1", "d"), ("wake", "m1", "w"),
                                      ("sleep", "m1", "s"), ("delete", "m1", "d2")),
                         ["ok", "ok", "conflict", "conflict", "ok"])

    def test_create_with_another_key_conflicts(self):
        self.assertEqual(self.replies(("create", "m1", "c"), ("create", "m1", "c"), ("create", "m1", "x")),
                         ["ok", "ok", "conflict"])

    def test_request_for_unknown_machine_is_rejected(self):
        res = run(Fenced, [[("sleep", "m9", "s")]], model=M)
        self.assertEqual([r for _, _, r in res.acks], ["conflict"])

    def test_files_survive_sleep_and_wake(self):
        world = M.new_world()
        model = MachineModel()
        model.new_world = lambda: world
        sc = SCENARIOS["sleep-wake"]
        best = None
        prefix = []
        while True:      # walk the fault-free schedules, find one where a VM ran before the sleep
            world.__init__()
            res = run(Fenced, sc, prefix, Bounds(0, 0, 0), model=model)
            if world.vols["m1"]["committed"]:
                best = world.nodes[world.store["m1"]["node"]]["vms"]["m1"]
                break
            i = len(res.choices) - 1
            while i >= 0 and res.choices[i][0] + 1 >= res.choices[i][1]:
                i -= 1
            if i < 0:
                break
            prefix = [c for c, _ in res.choices[:i]] + [res.choices[i][0] + 1]
        self.assertIsNotNone(best, "no schedule slept a running machine")
        self.assertEqual(best["base"], world.vols["m1"]["committed"])


class Scheduler(unittest.TestCase):
    def test_v1_counts_did_not_change(self):
        n, bad, kinds, _ = explore(Journal, V1_SCENARIOS["create-delete"])
        self.assertEqual((n, bad, kinds), (1607, 318, {"orphan": 318}))

    def test_total_caps_all_faults_together(self):
        rng = random.Random(4)
        for _ in range(300):
            res = run(Fenced, SCENARIOS["sleep-wake"], (), Bounds(2, 2, 5, 2, 2, total=2), rng, 0.4, M)
            self.assertLessEqual(sum(res.used.values()), 2)

    def test_default_bounds_offer_no_failover_or_node_fault(self):
        rng = random.Random(5)
        for _ in range(200):
            res = run(Fenced, SCENARIOS["delete"], (), Bounds(), rng, 0.5, M)
            self.assertEqual(res.used["failover"] + res.used["node"], 0)

    def test_failover_leaves_old_tasks_running(self):
        res = first_failure(UnkeyedRequests, SCENARIOS["sleep-wake"], Bounds(0, 0, 0, 1, 0))
        self.assertIsNotNone(res)
        self.assertTrue(any("takes over" in e for e in res.events))
        self.assertTrue(any(e.startswith("zombie ") for e in res.events))

    def test_crash_leaves_no_zombies(self):
        rng = random.Random(6)
        for _ in range(200):
            res = run(Fenced, SCENARIOS["reboot"], (), Bounds(2, 0, 3), rng, 0.4, M)
            self.assertFalse(any(e.startswith("zombie ") for e in res.events))

    def test_partition_reaches_the_plane_as_unreachable_and_heals(self):
        res = first_failure(Unfenced, SCENARIOS["create"], Bounds(0, 0, 0, 0, 1))
        self.assertTrue(any("[unreachable]" in e for e in res.events))
        self.assertTrue(any("partitioned" in e for e in res.events))

    def test_replay_is_deterministic(self):
        rng = random.Random(7)
        b = Bounds(2, 2, 99, 1, 2)
        sc = random_scenario(random.Random(3))
        a = run(Fenced, sc, (), b, rng, 0.3, M)
        again = run(Fenced, sc, [c for c, _ in a.choices], b, model=M)
        self.assertEqual(a.events, again.events)
        self.assertEqual(a.used, again.used)

    def test_generated_scenarios_are_well_formed(self):
        rng = random.Random(11)
        for _ in range(50):
            for reqs in random_scenario(rng, machines=3, length=5):
                self.assertEqual(reqs[0][0], "create")
                for req in reqs:
                    self.assertIn(req[0], ("create", "sleep", "wake", "reboot", "delete"))
                    self.assertEqual(len(req), 3)

    def test_expected_ignores_rejected_and_shared_machines(self):
        sc = [[("create", "m1", "c"), ("sleep", "m1", "s")], [("create", "m2", "c2")], [("wake", "m2", "w")]]
        acks = [(0, sc[0][0], "ok"), (0, sc[0][1], "conflict"), (1, sc[1][0], "ok"), (2, sc[2][0], "ok")]
        state, keys, wakes = expected(sc, acks)
        self.assertEqual(state, {"m1": "running"})
        self.assertEqual(wakes, {"m2": 1})


class Findings(unittest.TestCase):
    def test_v1_reconcile_fails_once_old_tasks_survive_a_failover(self):
        b = Bounds(0, 0, 1, 1, 0)
        n, bad, kinds, first = explore(Reconcile, V1_SCENARIOS["create-delete"], b)
        self.assertGreater(bad, 0)
        self.assertIn("orphan", kinds)
        res = run(Reconcile, V1_SCENARIOS["create-delete"], shrink(Reconcile, V1_SCENARIOS["create-delete"], first, b), b)
        self.assertTrue(any(e.startswith("zombie reconcile") for e in res.events))

    def test_unfenced_failover_gives_two_writers(self):
        res = first_failure(Unfenced, SCENARIOS["create"], Bounds(0, 0, 0, 0, 1))
        self.assertIn("split-brain", res.kinds)

    def test_unfenced_is_fine_without_node_faults(self):
        self.assertEqual(explore(Unfenced, SCENARIOS["delete"], Bounds(1, 1, 1), model=M)[1], 0)

    def test_stale_key_read_restarts_twice(self):
        res = first_failure(StaleKeyRead, SCENARIOS["reboot"])
        self.assertEqual(res.kinds, ["double-reboot"])

    def test_wake_in_the_old_epoch_adopts_a_fenced_vm(self):
        res = first_failure(WakeReusesEpoch, SCENARIOS["sleep-wake"], Bounds(0, 0, 0, 0, 1))
        self.assertEqual(res.kinds, ["incomplete"])
        self.assertTrue(any("partitioned" in e for e in res.events))

    def test_unkeyed_request_is_applied_again_by_a_zombie(self):
        res = first_failure(UnkeyedRequests, SCENARIOS["sleep-wake"], Bounds(0, 0, 0, 1, 0))
        self.assertEqual(res.kinds, ["lost"])

    def test_fenced_passes_every_single_fault_schedule_of_small_scenarios(self):
        for name in ("create", "delete"):
            n, bad, kinds, _ = explore(Fenced, SCENARIOS[name], ONE, model=M)
            self.assertEqual((bad, kinds), (0, {}), name)
            self.assertGreater(n, 200)

    def test_fenced_passes_every_fault_free_interleaving(self):
        for name, sc in SCENARIOS.items():
            n, bad, _, _ = explore(Fenced, sc, Bounds(0, 0, 1), model=M)
            self.assertEqual(bad, 0, name)

    def test_fenced_passes_generated_scenarios(self):
        b = Bounds(crashes=2, lost=2, preempts=10 ** 6, failovers=1, node_faults=2)
        n, bad, kinds, _ = fuzz_generated(Fenced, 5, 10, 100, b)
        self.assertEqual((n, bad, kinds), (1000, 0, {}))

    def test_seeded_bugs_are_caught(self):
        for mutant in MUTANTS:
            hit = None
            for name in ("create", "delete", "sleep-wake"):
                hit = first_failure(mutant, SCENARIOS[name])
                if hit:
                    break
            self.assertIsNotNone(hit, mutant.__name__)

    def test_shrunk_failure_replays_and_uses_one_fault(self):
        for plane in FOUND:
            for name in ("reboot", "sleep-wake"):
                res = first_failure(plane, SCENARIOS[name])
                if res:
                    self.assertTrue(res.violations)
                    self.assertLessEqual(sum(res.used.values()), 1)
                    break
            else:
                self.fail(plane.__name__)

    def test_known_gap_stale_pass_can_restart_a_healthy_machine(self):
        # Not fixed. This test pins the limitation so it cannot be forgotten.
        strict = MachineModel(strict_restarts=True)
        n, bad, kinds, _ = explore(Fenced, SCENARIOS["create"], Bounds(0, 0, 0, 1, 0), model=strict)
        self.assertEqual(kinds, {"unrequested-restart": bad})
        self.assertGreater(bad, 0)


if __name__ == "__main__":
    unittest.main()
