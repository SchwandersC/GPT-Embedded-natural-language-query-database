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

This project is licensed under the [MIT License](./LICENSE).

---

## 🧪 Example Workflow

Below is a real example showing the full interaction from natural language to output:

1. **Insert a Player**

**Query:**  
`insert, players, Steph Curry, 999, 30, 2018`

→ Inserts a new player into the "players" table with team ID 999.

---

2. **Find Team's Players**

**Natural Language Query:**  
`show me all players on the aces`

**Generated SQL:**  
```sql
SELECT p.PLAYER_NAME
FROM players p
INNER JOIN teams t ON p.TEAM_ID = t.TEAM_ID
WHERE t.NICKNAME = 'Aces';
```

---

3. **Update a Team Name**

**Natural Language Query:**  
`update the team name aces to vikings`

**Generated SQL:**  
```sql
UPDATE teams
SET NICKNAME = 'Vikings'
WHERE NICKNAME = 'Aces';
```

---

4. **Count Seasons per Player on New Team**

**Natural Language Query:**  
`show each player and the number of seasons they’ve played for the vikings. order it in ascending order`

**Generated SQL:**  
```sql
SELECT p.PLAYER_NAME, COUNT(p.SEASON) as cnt
FROM players p
INNER JOIN teams t ON p.TEAM_ID = t.TEAM_ID
WHERE t.NICKNAME = 'Vikings'
GROUP BY p.PLAYER_NAME
ORDER BY cnt ASC;
```

---

5. **Delete a Player Record**

**Natural Language Query:**  
`delete steph curry's player record from 2018`

**Generated SQL:**  
```sql
DELETE FROM players
WHERE PLAYER_NAME = 'Steph Curry' AND SEASON = 2018;
```

---

6. **Filter Players by Season Count**

**Natural Language Query:**  
`show all players on the vikings and the number of seasons they've played. Only show players with less than 2 seasons played.`

**Generated SQL:**  
```sql
SELECT p.PLAYER_NAME, COUNT(p.SEASON) as cnt
FROM players p
INNER JOIN teams t ON p.TEAM_ID = t.TEAM_ID
WHERE t.NICKNAME = 'Vikings'
GROUP BY p.PLAYER_NAME
HAVING COUNT(p.SEASON) < 2;
```

---

As shown, the system handles full SQL interactivity: INSERT, UPDATE, DELETE, SELECT, JOIN, GROUP BY, ORDER BY, and HAVING — all from plain English.

