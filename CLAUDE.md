# CLAUDE.md: AI Agent Instructions

## Project Context
You are acting as a Senior Data Engineer building a Reddit Anomaly Detection Engine. 
We are currently in **Phase 1: Local Backtesting**. We are processing a massive 30GB+ `.zst` historical data file locally to validate our math and NLP models.

## Strict Engineering Rules
1. **NO CLOUD YET:** Do not write any code for AWS, Kafka, or Cloud Vector Databases. We will do this in Phase 2.
2. **MEMORY MANAGEMENT IS CRITICAL:** NEVER load the entire dataset into memory. Do not use `pandas.read_csv` or load the full JSON file. You must use streaming decompression (e.g., `zstandard`) and process files line-by-line using Python generators (`yield`).
3. **MODULARITY:** Follow the design laid out in `@ARCHITECTURE.md`. Use strict Object-Oriented design, the Strategy Pattern for data ingestion, and Interface classes.
4. **TYPING & ERRORS:** Use strict Python type hinting (`-> Iterator[Dict]`, etc.) and robust `try/except` blocks for broken JSON lines. 

## Workflow
* Always check `@TODO.md` to see our current objective.
* Update the checkboxes in `@TODO.md` automatically when you successfully finish and test a step.
* Ask for clarification before installing massive new libraries.
* **Error Documentation (STRICT CRITERIA):** Do NOT log minor bugs, syntax errors, typos, or basic API misuse to `@ISSUES.md`. 
  * **Threshold:** ONLY log an issue if it demonstrates senior-level problem-solving. Acceptable categories include: Memory leaks (OOM), streaming bottlenecks, time/space complexity improvements, data ingestion architecture shifts, or concurrency limits. 
  * **Deduplication:** Before logging a new issue, you MUST read `@ISSUES.md`. If a highly similar issue already exists, do not create a new one. Instead, silently adapt the fix or ask me if we should update the existing entry.