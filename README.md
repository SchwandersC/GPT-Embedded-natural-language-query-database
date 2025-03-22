# 🏀 Natural Language SQL Query Engine for NBA Data  
### DSCI 551 Final Project

This project implements a **natural language interface** to query NBA statistics from CSV files using **large language models**, **Spark**, and **custom distributed join and query logic**.

With just a plain English question (e.g., _"Which team had the most wins in 2016?"_), the system generates and executes the appropriate SQL over distributed data and returns the result — no SQL knowledge required.

---

## 🚀 Features

- **Natural Language to SQL**  
  Converts free-form questions into SQL queries using OpenAI's GPT-3.5.

- **Retrieval-Augmented Generation (RAG)**  
  Dynamically provides example queries from a vector database (FAISS) for in-context learning (3-shot prompting).

- **Custom Spark Execution Engine**  
  - Custom **sort-merge join** implementation for learning purposes.  
  - Uses **MapReduce-style transformations** to simulate SQL behavior like `GROUP BY`, `ORDER BY`, `HAVING`, and `LIMIT`.

- **Insert / Update / Delete Support**  
  Simulates full SQL interactivity on local CSV files, including data manipulation operations.

---

## 📁 Dataset

Uses NBA game data from 2011 to 2018, downloadable from Kaggle:  
🔗 [Kaggle Dataset – NBA Games](https://www.kaggle.com/datasets/nathanlauga/nba-games)

Place the CSV files in the root directory (e.g., `games.csv`, `teams.csv`, etc.).

---

## 🧠 Tech Stack

| Component         | Technology                       |
|------------------|----------------------------------|
| Query Generation | OpenAI (GPT-3.5), LangChain       |
| Vector Search     | FAISS, OpenAI Embeddings         |
| Data Processing  | PySpark                          |
| Prompt Retrieval | Retrieval-Augmented Generation   |
| Join Algorithm   | Custom Sort-Merge Join (RDDs)     |

---

## 🛠️ Setup & Usage

1. **Clone the Repository**
   ```bash
   git clone https://github.com/your-username/DSCI-551-Project.git
   cd DSCI-551-Project
   ```

2. **Install Dependencies**
   ```bash
   pip install -r requirements.txt
   ```

3. **Set your OpenAI API Key**  
   Update the `OPENAI_KEY` variable in `config.py`.

4. **Download Dataset**  
   Place all CSVs from the [Kaggle NBA dataset](https://www.kaggle.com/datasets/nathanlauga/nba-games) into the project folder.

5. **Run the Project**
   ```bash
   python main.py
   ```

6. **Start Asking Questions!**
   ```
   DB> Which player scored the most points in 2017?
   DB> List all games where the Lakers won.
   DB> exit
   ```

---

## 📚 Educational Purpose

This system was built to explore:
- The internal mechanics of distributed data processing with Spark.
- SQL-like querying with no SQL engine backend.
- How LLMs can augment structured query tasks through few-shot learning and prompt engineering.

---

## 📌 Notes

- All joins are done using a **manual sort-merge algorithm**.
- Aggregations, filtering, and ordering are done using raw RDD operations.
- This is an educational project and not optimized for large-scale production systems.

---

## 📄 License

MIT License
