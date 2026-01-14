# Workflow

You must continuously track and update `.codex/progress/daemon.md` until all listed tasks are completed. It contains three sections:

- Work in Progress
- To Do
- Completed

1. You must first continue any **Work in Progress** (if any).

2. If there is no ongoing work, choose a task from **To Do** to begin. It does **not** have to be the first one in the list. Rough priority from highest to lowest:

   1. Architectural decisions and core abstractions
   2. Integration points between modules
   3. Unknown unknowns and rapid validation
   4. Standard features and implementation
   5. Optimization, cleanup, and quick wins

   You may start multiple tasks concurrently.

3. When starting a task, move it from **To Do** to **Work in Progress** to indicate you are actively working on it.

4. When completing a task, append the corresponding progress to the **Completed** section of `.codex/progress/daemon.md`, including:

   - What task was completed (concise, accurate summary with essential details)
   - Key decisions and reasoning
   - Files modified
   - Blockers or notes for next iterations

   Keep the writing concise, even at the expense of grammar. This file helps future iterations skip the exploration phase.

   Then remove the task(s) from **To Do** or **Work in Progress**.

   You must run necessary cmake/ctest and make/ninja commands and ensure all checks pass.

   After completion, commit your changes with a concise commit message describing the task.

5. You may add or remove tasks in **To Do** as needed based on your understanding of the project. Before removing a task, ensure it is no longer necessary. Break down large tasks into smaller focused tasks, or adjust task details according to the current state of the project. Each task must clearly define its implementation steps.

6. Refactor proactively based on your understanding of the codebase to maintain good structure and clarity.

7. If initially there are no **To Do** and no **Completed** tasks, thoroughly explore the codebase, document your understanding, and—based on the “Overall Work Objectives” in `.codex/progress/daemon.md`—produce a detailed implementation plan and derive the initial **To Do** list.

8. If all **To Do** tasks are completed, add the following task:

   Based on the current implementation, thoroughly review the “Overall Work Objectives” and confirm all objectives have been achieved. If any objective has not been achieved, break it into actionable tasks and add them to **To Do**. If all objectives have been achieved, analyze shortcomings of the current implementation (e.g. code simplicity, engineering quality, readability, testability, etc.) and derive a new **To Do** list.

9. When **Completed** accumulates more than 10 entries, consolidate and summarize them, keeping only important and essential content, reducing the list to 3 entries or fewer.

## Requirements

- Do **not** ask questions. Continue working until all tasks in `.codex/progress/daemon.md` are completed.
