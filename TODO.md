# TODO.md: Project Roadmap

## Phase 1: Local Backtesting Pipeline

### Step 0: Repository Setup
- [ ] Initialize a local Git repository.

- [ ] Create a .gitignore file to block .zst, .sqlite, .json, .env, venv/, and __pycache__.

### Step 1: The Ingestion Layer (Component 1)
- [ ] Install required lightweight libraries (`zstandard`).
- [ ] Create `ingestion.py`.
- [ ] Define the base `DataIngestor` interface.
- [ ] Implement `ZstFileIngestor` that streams a `.zst` file line-by-line, parses the JSON, and yields a normalized dictionary.
- [ ] **TEST:** Run a script that prints exactly the first 5 comments from `RC_2023-12.zst` and then safely closes the stream. 

### Step 2: The Filter Layer (Component 2)
- [ ] Create `filter.py`.
- [ ] Implement logic to accept the generator from Step 1 and yield ONLY comments where the `subreddit` matches a target list (e.g., `gaming`, `popculturechat`, `movies`).
- [ ] **TEST:** Run the stream until it finds and prints 3 valid comments from the target subreddits, bypassing the noise.

### Step 3: The Time-Series Math Engine (Component 3)
- [ ] Create `tripwire.py`.
- [ ] Implement logic to group filtered comments into 1-hour tumbling windows based on the `timestamp` field.
- [ ] Calculate the rolling mean and standard deviation of volume for each subreddit.
- [ ] Calculate the Z-Score for the current window.
- [ ] **TEST:** Feed 24 hours of streamed data into the engine and print a log whenever a subreddit crosses a Z-Score of 3.0.

### Step 4: The NLP Context Engine (Component 4)
- [ ] Install `sentence-transformers` and `scikit-learn`.
- [ ] Create `nlp_brain.py`.
- [ ] Implement logic that ONLY triggers when Step 3 fires an anomaly. 
- [ ] Extract the text from the anomalous 1-hour window, generate embeddings using `all-MiniLM-L6-v2`, and cluster them using DBSCAN.
- [ ] **TEST:** Manually pass a list of 50 fake comments (40 about a game delay, 10 random) and ensure the clustering accurately groups the 40 comments.

### Step 5: Integration & Output (Component 5)
- [ ] Create `main.py` to link Components 1 through 4 together cleanly.
- [ ] Implement a SQLite or JSON logger to save detected anomalies.
- [ ] **FINAL TEST:** Run the pipeline on the `RC_2023-12.zst` file specifically targeting December 4, 2023, to see if it successfully detects and clusters the GTA VI trailer leak.