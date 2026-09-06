---
name: dev_loop
description: Autonomous engineering loop that coordinates Developer Agent and QA Agent to implement, test, and self-correct Rust code based on spec.md. Trigger with /dev_loop or /startcycle.
---

# Skill / Workflow: Autonomous Engineering Loop (`dev_loop`)

This skill orchestrates the end-to-end spec-to-code automated development loop in Antigravity.
It coordinates the **Developer Agent** and **QA Agent** to iteratively implement, verify, and fix code until all tests pass.

## Triggers
- Slash command: `/dev_loop` or `/startcycle`
- CLI execution: `agy -p "Run workflow /dev_loop" --cwd .`

## Configuration
- Target Specification: `spec.md`
- Target Project: `datafusion-sql-processor`
- Max Iterations: `5`
- Feedback Communication Log: `test_results.log`

---

## Execution Steps

### 1. Initialize Phase
- Read `spec.md` to baseline functional requirements, CLI arguments, and acceptance criteria.
- Check workspace state and verify build prerequisites (`Cargo.toml`, `src/`).
- Initialize loop counter: `iteration = 1`.
- Clean up any stale `test_results.log` from prior completed runs.

### 2. Autonomous Loop Iteration (Max: 5 Cycles)

#### Step A: Implementation (Developer Agent)
- Run skill `implement` (`.agents/skills/implement.md`).
- If `test_results.log` exists, parse the compiler diagnostics or test assertion panics to apply targeted fixes.
- If this is the initial run, implement code satisfying `spec.md`.

#### Step B: Audit & Verification (QA Agent)
- Run skill `audit_and_test` (`.agents/skills/audit_and_test.md`).
- Execute `cargo check`, `cargo test`, and `cargo clippy --all-targets -- -D warnings`.

#### Step C: Loop Evaluation & Branching
- **Branch 1 (PASS)**:
  - All tests and checks passed.
  - Delete `test_results.log`.
  - Output: `🎉 Autonomous Loop Completed: All requirements in spec.md satisfied and verified.`
  - **STOP** workflow.
- **Branch 2 (FAIL & iteration < 5)**:
  - Save failure output and remediation guidance into `test_results.log`.
  - Output: `🔄 Iteration {iteration}/5 failed. Feeding error details back to Developer Agent for correction.`
  - Increment `iteration = iteration + 1`.
  - Return to **Step A**.
- **Branch 3 (FAIL & iteration >= 5)**:
  - Maximum retry attempts reached without resolution.
  - Output: `⚠️ Escalation: Maximum retry limit (5) reached. Human review required.`
  - Display error summary and pause execution for user review.

