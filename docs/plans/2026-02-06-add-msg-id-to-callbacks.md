# Add msg_id To Task Callbacks Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Allow `Task.on_success` and `Task.on_failure` callbacks to optionally receive `msg_id` without breaking existing two-argument handlers.

**Architecture:** Detect callback arity when decorators register functions, cache parameter counts, and conditionally pass `msg_id` inside `Task.run`. Expand typing/docstrings plus regression tests to cover sync/async success + failure scenarios.

**Tech Stack:** Python 3.8+, asyncio, pytest

---

### Task 1: Add regression tests for callback arity

**Files:**
- Create: `tests/test_task_callbacks.py`

**Step 1: Write failing tests**

Add pytest cases covering:
- Sync `on_success` with/without `msg_id`
- Async `on_success` with `msg_id`
- Sync/async `on_failure` variants raising and capturing msg_id
Each test should instantiate `Task`, attach execute/on_success/on_failure functions, invoke `await task.run(..., msg_id=42)` (use `pytest.mark.asyncio`). Use simple state dicts/flags to assert msg_id handling and that legacy 2-arg functions still run.

**Step 2: Run tests to verify they fail**

`pytest tests/test_task_callbacks.py -k msg_id -vv`
Expected: failures complaining about unexpected parameters / missing msg_id behavior.

---

### Task 2: Implement signature detection & caching in `Task`

**Files:**
- Modify: `flowmix/runner/task.py`

**Step 1: Cache callback parameter counts**

In `__init__`, add `_on_success_param_count` and `_on_failure_param_count`. Within `on_success`/`on_failure` decorators, compute `len(inspect.signature(func).parameters)` and store counts.

**Step 2: Use cached counts in `run`**

Update success/failure sections to:
- Determine target argument count (default 2 when cache missing)
- Pass `msg_id` only when `param_count >= 3`
- Support sync + async functions without re-evaluating signatures.

**Step 3: Maintain behavior when `msg_id` is `None`**

Even if ID missing, still pass `None` when handler expects third arg so tests cover this.

**Step 4: Run focused tests**

`pytest tests/test_task_callbacks.py -k msg_id -vv`
Expected: green.

---

### Task 3: Update type hints and docs

**Files:**
- Modify: `flowmix/common/types.py`
- Modify: `flowmix/runner/task.py`

**Step 1: Extend callback type aliases**

Introduce unions (sync + async) to allow two- or three-argument signatures for success/failure callbacks.

**Step 2: Refresh docstrings/examples**

Update decorator docstrings + examples in `Task.on_success` / `Task.on_failure` to mention optional msg_id third parameter and show usage for both sync and async cases.

**Step 3: Run lint/tests**

`pytest tests/test_task_callbacks.py -vv`

---

### Task 4: Document advanced usage (optional but recommended)

**Files:**
- Modify: `README.md` (or `QUICK_START.md` if more appropriate)

**Step 1: Add short subsection under advanced features describing how to add third arg to callbacks to persist msg_id, referencing backwards compatibility.

**Step 2: Run full test suite**

`pytest -vv`

---

### Final Verification

1. `pytest -vv`
2. `git status` should show only intentional files
