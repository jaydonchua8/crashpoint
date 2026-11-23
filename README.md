# crashpoint

Enumerate crashes, lost replies and interleavings against a small control plane, and check what is left behind.

The problem that inspired this is control-plane correctness for machine lifecycles: a create or delete is several steps (volume, IP, VM, record), and the interesting bugs only show up when the process dies halfway, a reply is lost, or two requests overlap. Those are hard to hit with ordinary tests. `crashpoint` runs the control-plane code under a scheduler that decides, at every store or node call, which task runs next, whether the reply is dropped, and whether the whole process crashes and restarts. After every request is acknowledged and nothing is left running, it checks the invariants.

It is two files of Python with no dependencies: `crashpoint.py` (the simulator, 386 lines) and `planes.py` (three control planes for the same create/delete API, 178 lines).

## Results

`python bench.py` enumerates every schedule with at most 1 crash, 1 lost reply and 1 preemption for five scenarios and three control planes. That is 40,918 schedules.

| scenario | naive | journal | reconcile |
|---|---|---|---|
| create | 33 of 35 fail | 0 of 353 | 0 of 54 |
| create, then delete | 57 of 100 | 318 of 1,607 | 0 of 1,548 |
| create, delete, create again | 144 of 195 | 1,808 of 4,303 | 0 of 11,135 |
| two clients send the same create | 3,090 of 3,104 | 0 of 7,712 | 0 of 1,678 |
| create and delete race | 414 of 470 | 1,432 of 6,953 | 0 of 1,671 |
| total | 3,738 of 3,904 | 3,558 of 20,928 | 0 of 16,086 |

The three planes:

- `naive` does the steps in order and writes the record at the end. It leaks on almost any crash.
- `journal` was my first fix: write the intent first, name every resource after the idempotency key so each step can be repeated, and resume unfinished operations after a restart. This is the one I expected to pass.
- `reconcile` is what I ended up with: handlers only write the desired state with compare-and-swap, and one reconcile task per machine observes the node and moves it toward the record.

### The bug in the journal version

I expected `journal` to fail only when a create and a delete from different clients overlap. It also fails with a single client that does nothing concurrent. This is the trace `python crashpoint.py journal create-delete` prints, with two stretches shortened:

```
A:create m1 k1: get(m1)
A:create m1 k1: insert(m1)
A:create m1 k1: create_volume(m1.k1.vol, m1, k1)
A:create m1 k1: alloc_ip(m1.k1, m1, k1)
A:create m1 k1: boot_vm(m1.k1, m1, k1, m1.k1.vol, m1.k1)
*** control plane crashes and restarts ***
resume create m1: create_volume(m1.k1.vol, m1, k1)
A:create m1 k1: get(m1)
  ... the client retries, the create finishes and is acknowledged ...
A:delete m1: get(m1)
  ... the delete finishes and is acknowledged ...
resume create m1: alloc_ip(m1.k1, m1, k1)
resume create m1: boot_vm(m1.k1, m1, k1, m1.k1.vol, m1.k1)
VIOLATION orphan: m1 has no record but 1 resources remain
```

After the restart there are two things driving the same create: the resume task and the client's retry. Both are safe on their own because the steps are idempotent. But the resume task is slow, the client finishes and then deletes the machine, and the resume task allocates an IP for a machine that no longer exists. In the "create again" scenario the same race removes the record of the new incarnation, so an acknowledged create is lost.

That is why the third version has no resume logic at all. A reconcile pass does not remember what it was doing. It reads the record, looks at the node, and acts on the difference, and there is never more than one pass per machine.

### Random schedules

Enumeration gets expensive quickly, so larger scenarios use random schedules with up to 3 crashes and 3 lost replies. 20,000 schedules each, seed 1:

| scenario | naive | journal | reconcile |
|---|---|---|---|
| two machines, two clients | 8,537 fail | 5 | 0 |
| three clients, two machines, seven requests | 12,278 | 1,590 | 0 |
| create, then delete (same as above) | 5,749 | 17 | 0 |

The last row is the useful comparison. Enumeration found the journal bug in 318 of 1,607 schedules. Random scheduling hit it 17 times in 20,000, because it needs a crash at one point followed by one task being starved for a long time.

### Does the checker catch anything in the version that passes?

A clean result only means something if the checker can fail. I put three bugs into `reconcile` and each was caught by enumeration:

| seeded bug | caught in scenario | schedules run before the first failure |
|---|---|---|
| remove the record without checking its version | create, delete, create again | 70 |
| never clean up resources of an earlier incarnation | create, delete, create again | 617 |
| a second reconcile task per machine after a restart | create, then delete | 908 |

## How it works

Control-plane code is written as Python generators. Every call to the record store or the node is a `yield`:

```python
def delete(self, mid):
    while True:
        rec = yield S("get", mid)
        if rec is None or rec["want"] == "absent":
            return "ok"
        if (yield S("cas", mid, rec["ver"], {"want": "absent", "status": "pending"})):
            return "ok"
```

The scheduler holds the pending call of every task. At each step it lists the options: run one of the tasks, run a node call but throw away the reply, or crash. A crash drops every in-flight task and keeps the store and the node. Clients are outside the process, so they resend the same request with the same key after a crash or an error.

A schedule is the list of option indices chosen, so any run can be replayed exactly. `explore` walks all of them depth first by re-running from the start with a longer prefix each time. `fuzz` picks randomly. `shrink` resets choices to the default one at a time while the run still fails.

Invariants checked once everything is quiet:

- no resources exist for a machine that has no record (`orphan`)
- a machine with a record has exactly one VM, one IP and one volume of the current incarnation and nothing else (`leak`, `incomplete`)
- a machine used by one client ends in the state of that client's last acknowledged request (`lost`)

And one checked after every step: a machine never has two VMs at once (`two-vms`).

## What did not work

- The journal design, as described above.
- Running the script directly first gave me different schedule counts than importing it (2,063 instead of 1,607 for the same scenario). `crashpoint.py` was loaded twice, once as `__main__` and once as `crashpoint`, so there were two `LostReply` classes and the retry helper did not catch the one the scheduler threw. Every lost reply turned into a failed request. `main` now imports from the module by name.
- Shrinking does little. On the first failing random schedule it took the naive plane from 2 non-default choices to 1 and from 3 to 1, and did not reduce either journal failure (4 stayed 4, 1 stayed 1). The traces are still 19 to 41 steps long because it only removes faults and preemptions, not requests.
- I wrote a fourth seeded bug, a delete that overwrites the record without compare-and-swap. Nothing caught it. When I looked, it is not a bug in this model: the overwrite only sets `want` to absent, which is a legal outcome of a delete racing a create. I removed it instead of counting it.
- The two-machine scenario was too big to enumerate in the benchmark. One run of it on the journal plane was 103,712 schedules and took 74 seconds, so it is only fuzzed.

## Limits

- This is a model. The node is a Python object with three dictionaries. It has not been run against Kubernetes, a real VMM, or any real control plane.
- The bounds are small: 1 crash, 1 lost reply, 1 preemption. A bug that needs two crashes is only reachable through the random mode. "0 failing" means none inside those bounds on these five scenarios, not a proof.
- Calls are atomic and the store is linearizable. There are no partial writes, no network partitions, no clock, and no stale task that survives a crash and keeps running, which is what fencing tokens are for.
- No partial-order reduction, so many enumerated schedules are equivalent. The same 40,918 schedules took 5.8 seconds on an idle machine and 28.4 seconds when the machine was busy with other jobs.
- The reconcile plane acknowledges a create when the desired state is written, not when the VM is up. The check only requires that it gets there.

## Run it

```
python test_crashpoint.py
python bench.py
python crashpoint.py journal create-delete
```

Tested on Python 3.13. Nothing to install. The CLI takes a plane (`naive`, `journal`, `reconcile`) and a scenario (`create`, `create-delete`, `recreate`, `retry-race`, `delete-race`), and exits 1 if any schedule fails.
