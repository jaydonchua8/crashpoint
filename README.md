# crashpoint

A small model checker for machine-lifecycle control planes. It runs control-plane code under a scheduler that enumerates crashes, lost replies, interleavings, control-plane failovers and node partitions, and checks what is left behind.

This is an independent prototype. It is not affiliated with, endorsed by, or reviewed by Dedalus Labs. Everything it runs against is a simulated cluster and synthetic scenarios. It has never touched a real VM, Kubernetes, or the Dedalus API.

## The problem

Creating, sleeping, waking, rebooting and deleting a machine each take several steps across a record store, a node and storage. The bugs show up only when something happens between two of those steps: the process dies, a reply is lost, two requests overlap, an old worker keeps running after a new one took over, or a node stops answering while its VM is still alive. Ordinary tests almost never hit those points. I wanted a tool that hits all of them inside a stated bound, and then tells me which guarantee broke.

## Why this matters to Dedalus Labs

What I verified from public pages:

- Dedalus Machines are persistent, isolated computers for AI agents that keep files and state over time. (https://www.ycombinator.com/companies/dedalus-labs)
- The Systems Engineer Intern posting lists control-plane correctness (retries, cancellation, concurrent requests, cleanup) and failure testing (inject a crash, a delayed response or an interrupted operation, and explain which guarantees the test establishes). It also says model checking experience is a plus. (https://www.ycombinator.com/companies/dedalus-labs/jobs/EQcMolJ-systems-engineer-intern-co-op)
- The public Go SDK defines the machine endpoints (create, sleep, wake, reboot, delete under `v1/machines`), a `desired_state` of `running`, `sleeping` or `destroyed`, a `phase` with nine values from `accepted` to `failed`, and a lifecycle status with a `revision`. (https://github.com/dedalus-labs/dedalus-go/blob/main/machine.go)
- The lifecycle docs say a reboot is rejected with 409 unless `desired_state` is `running`, that repeating a completed reboot with the same `Idempotency-Key` returns the saved response without restarting again, and that files survive sleep and wake while processes do not. (https://docs.dedaluslabs.ai/dcs/dm/lifecycle)
- The SDK README says a machine must reach `running` before processes can be started on it. (https://github.com/dedalus-labs/dedalus-go)

So the second model in this repo uses that public contract as its specification: the same requests, the same two enums, the 409 rule for reboot, and the idempotency rule. Everything underneath the API in my model (two nodes, a storage service that decides who may write a volume, a compare-and-swap record store) is my own assumption. I do not know how Dedalus implements any of it.

## What it does

There are two models.

**One-node model (the first version of this project).** Three control planes for create and delete. `naive` leaks on almost any crash. `journal` resumes unfinished work after a restart and still fails. `reconcile` passed every schedule I enumerated, and I called it correct.

**Machine model (new).** Requests are create, sleep, wake, reboot and delete, each with an idempotency key. The world has two nodes and a storage service. Two faults were added that the first version did not have:

- failover: a new control-plane instance takes over while the old instance's in-flight tasks keep running. The first version only had crashes, where in-flight work disappears.
- node partition: calls to the node fail, and the VM on it keeps running.

Invariants in the machine model:

| name | meaning |
|---|---|
| `split-brain` | two VMs can write the same machine's volume at the same time (checked after every step) |
| `writer-while-stopped` | the record says `sleeping` or `destroyed` while some VM can still write (every step) |
| `data-loss` | a VM that was reported `running` lost write access without a checkpoint, on a node that was never cut off |
| `double-reboot` | one idempotency key restarted a machine more than once |
| `lost` | the final `desired_state` is not what the acknowledged replies promised |
| `leak`, `incomplete` | at the end, VMs exist that the record does not name, or the named one is missing or cannot write |
| `unrequested-restart` | the machine was started more often than create, wake and reboot explain |
| `bad-enum` | a record was written with a `desired_state` or `phase` outside the public enums |

## Results

All numbers below are from `python bench.py` on Python 3.13 in one run that took 335 seconds. They are in `bench_results.json`.

### The plane I called correct fails under the new fault

The one-node `reconcile` plane passed 16,086 of 16,086 schedules in the first version (1 crash, 1 lost reply, 1 preemption). With one failover and one preemption instead, it fails 13,629 of 52,300. An old reconcile pass keeps creating resources for a machine that the new instance has already deleted. Nothing in that design stops a late call from an old pass.

### First attempt at failover, and three bugs in my fix

In the machine model my first design (`unfenced`) restarts the machine on the other node when its node stops answering. With any single fault it fails 1,787 of 180,857 schedules, all of them `split-brain`: the old VM is still alive behind the partition and both can write.

The fix gives every runtime start an epoch. Storage lets only the newest epoch write, and a graceful stop closes its epoch. I expected that to pass. The checker found three more bugs first:

| bug | fault needed | violation | first failing schedule |
|---|---|---|---|
| reboot looked up the idempotency key in one read and recorded the reboot in a later write | crash | `double-reboot` | 228th of scenario `reboot` |
| wake kept the epoch the machine went to sleep in | failover (also node partition) | `data-loss`, `incomplete` | 2,424th of `sleep-wake` |
| sleep, wake and delete did not record their request key | failover | `lost` | 1,172nd of `sleep-wake` |

I kept each one as a class in `machine_planes.py` so the tests replay them.

### The hardest one

Wake reusing the epoch. This is the trace for a node partition, shortened and with my notes on the right (`machines` scenario `sleep-wake`, 64 of 5,438 schedules fail with one node fault):

```
reconcile m1: boot_vm(n1, m1, 2)
reconcile m1: cas(m1, 2)                       <- reported running on n1, epoch 2
A:sleep m1 s1 -> ok
reconcile m1: observe(n1, m1)
*** node n1 is partitioned from the control plane ***
reconcile m1: stop_vm(n1, m1, 2)  [unreachable]
reconcile m1: observe(n1, m1)  [unreachable]
reconcile m1: seal(m1, 2)                      <- cannot stop it, so fence epoch 2
reconcile m1: cas(m1, 4)                       <- reported sleeping
A:wake m1 w1 -> ok
*** node n1 is reachable again ***
reconcile m1: observe(n1, m1)                  <- sees a VM with the epoch the record names
reconcile m1: boot_vm(n1, m1, 2)               <- "already running", returns true
reconcile m1: cas(m1, 6)                       <- reported running
VIOLATION incomplete: m1 wants running at ('n1', 2), phase running, VMs [('n1', 2)]
```

The machine is reported `running`, but its VM is the one that was fenced while it slept, so it cannot write its disk. My first guess was that the node model was wrong and `boot_vm` should not answer "already running" for a fenced VM. That would have hidden the problem. The record was naming an epoch that storage had already closed, and the same mistake let an old pass fence a healthy VM after a failover. The fix is in the wake handler: wake always moves to a new epoch, in the same compare-and-swap that changes `desired_state`. After that, 0 of 4,816 schedules fail under the same bound.

### Final version

| check | schedules | failing |
|---|---|---|
| 6 development scenarios, any single crash, lost reply, preemption, failover or node partition | 180,567 | 0 |
| scenario `create`, any two of those | 41,157 | 0 |
| held-out: 40 generated scenarios (seed 2027, up to 3 machines, up to 6 requests per client), random schedules with up to 2 crashes, 2 lost replies, 2 failovers, 2 node partitions | 12,000 | 0 |

The development scenarios are the ones I fixed bugs against. The held-out scenarios come from a random generator with settings I did not use while developing, and the plane was not changed after running them.

### Can the checker fail?

Nine planes with a known bug: my first design, the three bugs above, and five seeded bugs. Enumeration caught all nine. Random schedules on the held-out scenarios also caught all nine, but some of them rarely:

| plane | first caught by enumeration in | held-out random, failing of 12,000 |
|---|---|---|
| Unfenced | `create`, schedule 5 | 558 |
| StaleKeyRead | `reboot`, schedule 228 | 83 |
| WakeReusesEpoch | `sleep-wake`, schedule 2,424 | 20 |
| UnkeyedRequests | `sleep-wake`, schedule 1,172 | 3 |
| Impatient (one lost reply treated as a dead node) | `delete`, schedule 429 | 55 |
| HardKill (stop without checkpoint) | `create`, schedule 10 | 3,628 |
| NoResync | `create`, schedule 5 | 865 |
| NoSeal | `delete`, schedule 351 | 539 |
| BlindPhase (phase written without compare-and-swap) | `sleep-wake`, schedule 343 | 41 |

Three hits in 12,000 for the unkeyed-request bug is the reason I enumerate first and use random schedules only where enumeration is too large.

### A gap I did not close

With a failover, an old reconcile pass can still stop the current, healthy VM. The stop is graceful, so a checkpoint is published and no files are lost, but the machine restarts without anyone asking. With the `unrequested-restart` check kept on for failover runs, the final plane fails 98 of 5,488 schedules across four scenarios. The normal runs above switch that check off when a failover happened. Closing it needs the node to reject calls from an old control-plane instance, which this model does not have. `test_known_gap_stale_pass_can_restart_a_healthy_machine` pins this so I cannot forget it.

## Architecture

| file | what it holds |
|---|---|
| `crashpoint.py` | scheduler, depth-first enumeration, random schedules, shrinking, fault budgets, CLI |
| `planes.py` | one-node model: `naive`, `journal`, `reconcile` |
| `machines.py` | machine model: record store, nodes, storage fence, invariants, scenarios, scenario generator |
| `machine_planes.py` | `Fenced`, `Unfenced`, the three found bugs, five seeded bugs |
| `test_crashpoint.py`, `test_machines.py` | 65 tests |
| `bench.py`, `bench_results.json` | every number in this README |
| `demo.py` | a 10 second tour |

Control-plane code is written as Python generators. Every call to the store, a node or storage is a `yield`, so the scheduler sees each one before it happens:

```python
def reboot(self, mid, key):
    while True:
        saved = yield S("idem_get", key)
        if saved:
            return saved
        rec = yield S("get", mid)
        if rec is None or rec["desired_state"] != "running":
            return (yield from self._reject(key))      # 409: not running
        ...
```

At each step the scheduler lists its options: run one of the tasks, apply a call but drop the reply, crash, fail over, or let the model change the environment. A schedule is the list of option indices, so any run replays exactly. A model object supplies the world, the environment faults and the invariants, which is how the same scheduler serves both models.

## Key decisions and tradeoffs

- **Generators instead of threads.** The code under test has to be written in this style, which is a real cost. In return every schedule is deterministic and replayable, and a failing one is a short list of integers.
- **Re-execution instead of state hashing.** Enumeration re-runs from the start with a longer prefix each time. Generators cannot be copied or hashed, so I have no visited-state table and no partial-order reduction. Many enumerated schedules are equivalent.
- **A cap on total faults.** Two faults of five kinds on a 25-step run is already millions of schedules. I enumerate any single fault on all scenarios, any two on the smallest one, and use random schedules above that. "0 failing" means none inside those bounds.
- **Failover is a separate fault from a crash.** The first version passed because a crash removes in-flight work. Keeping old tasks alive is what broke it.
- **The data-loss rule depends on what was reported.** A VM that is fenced before the record ever said `running` cannot hold user writes, because the SDK says a machine has to reach `running` before it can be used. Without that rule, a late start from an old pass would count as data loss.
- **The reconcile trigger reads the simulated nodes directly.** `wants()` stands in for a periodic resync. I did not model how a real controller learns that a node disagrees with a record.
- **Python, not Rust or Go.** The posting prefers Rust for runtime work. I chose Python to get a working checker and its test suite finished. The scheduler design does not depend on the language, but I have not ported it.

## Install and usage

Python 3 standard library only. Nothing to install.

```
python demo.py                          # about 10 seconds
python -m unittest                      # 65 tests, about 10 seconds
python crashpoint.py unfenced create    # prints a failing schedule, exits 1
python crashpoint.py fenced delete      # 1876 schedules, 0 failing, exits 0
python crashpoint.py journal create-delete
python bench.py                         # about 6 minutes, rewrites bench_results.json
```

## A real example

`python crashpoint.py unfenced create`, with the first eleven lines of the trace removed:

```
bounds: any single crash, lost reply, preemption, failover or node partition
281 schedules, 5 failing {'incomplete': 5, 'split-brain': 5}

a failing schedule after shrinking (29 steps):
  ...
  reconcile m1: boot_vm(n1, m1, 1)
  *** node n1 is partitioned from the control plane ***
  reconcile m1: cas(m1, 2)
  reconcile m1: get(m1)
  reconcile m1: observe(n1, m1)  [unreachable]
  reconcile m1: observe(n2, m1)
  reconcile m1: cas(m1, 3)
  reconcile m1: get(m1)
  reconcile m1: observe(n1, m1)  [unreachable]
  reconcile m1: observe(n2, m1)
  reconcile m1: vol_create(m1)
  reconcile m1: boot_vm(n2, m1, 1)
  reconcile m1: cas(m1, 4)
  --- environment settles: every node reachable again ---
  reconcile m1: get(m1)
  reconcile m1: observe(n1, m1)
  reconcile m1: observe(n2, m1)
  reconcile m1: stop_vm(n1, m1, 1)
  reconcile m1: vol_create(m1)
  reconcile m1: boot_vm(n2, m1, 1)
  VIOLATION split-brain: m1
  VIOLATION incomplete: m1 wants running at ('n2', 1), phase running, VMs [('n2', 1)]
```

## Tests and verified results

`python -m unittest` runs 65 tests: the 18 from the first version, unchanged, and 47 new ones. They also pass with `python -W error -m unittest`.

The new tests cover the storage fence and node model (including calls to a partitioned node, a seal that arrives before the volume exists, and a stop with the wrong epoch), the documented API behaviour without faults (reboot on a sleeping machine is rejected, the same reboot key restarts once, nothing but delete works after delete), the scheduler (the fault cap, zombies after a failover and none after a crash, deterministic replay), and the findings above. One test checks that the schedule counts of the first version did not change when I rewrote the scheduler (1,607 schedules and 318 failures for `journal create-delete`, and `bench.py` reproduces the old total of 40,918).

What is not verified: anything about real systems. I have not run this against Kubernetes, a VMM or the Dedalus API, and I have no evidence that the bugs it finds in my planes exist in anyone else's code.

## Limitations and next steps

- It is a model. Nodes are dictionaries. Calls are atomic, the record store is linearizable and never loses a reply, and there is no clock.
- The plane can tell a lost reply from an unreachable node. A real controller cannot, and has to decide with timeouts. The `Impatient` seeded bug shows what goes wrong when one lost reply counts as a dead node, but I did not model a failure detector.
- Machine ids are chosen by the client here. The real API generates them, so "the same create sent twice makes two machines" is a bug class this model cannot express.
- Applying the idempotency key to sleep, wake and delete is my assumption. The docs I read state the rule for reboot.
- The `failed` phase is never used, and resize, autosleep, forced reboot, forced delete, executions and SSH are not modelled.
- The unrequested restart after a failover, described above, is open.
- At most one node is partitioned at a time, and there are two nodes.
- Shrinking only resets faults and preemptions. It does not remove requests, so traces are 20 to 30 steps.

Next I would add a control-plane term that nodes check, to close the restart gap, then port the scheduler to Rust with an explicit state machine per task so states can be hashed and equivalent schedules skipped.
