# ARCHITECTURE.md: Reddit Anomaly Detection Engine (Phase 1)

## 1. System Overview
This project is a high-performance Data Engineering and Machine Learning pipeline designed to detect localized surges (anomalies) in textual social media data. 

**Current Phase:** Phase 1 (Local Backtesting).
**Objective:** Process a 30GB+ `.zst` compressed file of historical Reddit data locally, utilizing streaming decompression to keep RAM usage strictly under 4GB.

## 2. Core Architectural Principles
* **Separation of Concerns:** Each component must be completely decoupled. The AI model should not know how the data was ingested, and the ingestor should not do any math.
* **Interface-Driven Design (Encapsulation):** All data sources must implement a base interface. This ensures we can swap from local `.zst` files to cloud Kafka streams in Phase 2 with zero changes to the core logic.
* **No Memory Bloat:** NEVER load the entire dataset into memory. Do not use `pandas.read_csv` or `json.loads` on the whole file. Use Python generators (`yield`) to stream data line-by-line.

## 3. Pipeline Components

### Component 1: The Ingestion Layer
* **Pattern:** Strategy Pattern / Interface.
* **Implementation:** Create a base `DataIngestor` class. Create a `ZstFileIngestor` subclass.
* **Mechanism:** Use the `zstandard` library to open the file stream. Read the file line-by-line. Parse the JSON. 
* **Output:** Yields a normalized dictionary/dataclass representing a single comment (Fields: `timestamp`, `subreddit`, `body`, `score`, `id`).

### Component 2: The Filter Layer
* **Mechanism:** Sits directly after the Ingestor. 
* **Logic:** Discards any comment that does not belong to a pre-defined list of target subreddits (e.g., `['gaming', 'games', 'pcgaming', 'popculturechat', 'movies']`). 
* **Purpose:** Drops 95% of the noise before it hits the math engine to save CPU cycles.

### Component 3: The Time-Series Math Engine (The Tripwire)
* **Mechanism:** Consumes the filtered stream and buckets the data into 1-hour Tumbling Windows based on the comment `timestamp`.
* **Logic:** Maintains a rolling state. Calculates the moving average and standard deviation of comment volume per subreddit. 
* **Trigger:** Calculates the Z-Score for the current 1-hour window. If the Z-Score > 3.0 (configurable threshold), it flags an **Anomaly Event** and passes the raw text from that 1-hour window to the NLP layer.

### Component 4: The NLP Context Engine (The Brain)
* **Trigger-Only:** This layer is highly CPU-intensive and MUST ONLY run when Component 3 triggers an Anomaly Event.
* **Model:** Use `sentence-transformers` (specifically `all-MiniLM-L6-v2`) to convert the anomalous text into vector embeddings.
* **Clustering:** Run a lightweight clustering algorithm (like DBSCAN or Agglomerative Clustering) on the vectors to group similar comments. 
* **Extraction:** Extract the most frequent nouns/entities from the largest cluster to summarize *why* the surge is happening.

### Component 5: The Output Layer
* **Mechanism:** Logs the detected anomalies to a local `sqlite3` database or structured `anomalies.json` file.
* **Data to Save:** Timestamp, Subreddit, Z-Score, Total Volume, Cluster Summary, and top 5 representative comments.

## 4. Tech Stack (Phase 1)
* **Language:** Python 3.10+
* **Decompression:** `zstandard`
* **Math/Clustering:** `numpy`, `scikit-learn`
* **NLP:** `sentence-transformers`, `spacy` (optional for entity extraction)