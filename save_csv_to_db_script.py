import sqlite3
import pandas as pd

# Load CSV data
csv_file_path = 'data/feedback_log.csv'   #'data/feedback_log.csv'  # Adjusted path for your environment
df = pd.read_csv(csv_file_path)

# Database connection
db_name = 'data/feedback_log.db'  # Adjusted path for your environment
conn = sqlite3.connect(db_name)
cursor = conn.cursor()

# Create table if it doesn't exist
cursor.execute('''
CREATE TABLE IF NOT EXISTS feedback (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    timestamp TEXT,
    name TEXT,
    code TEXT,
    expert TEXT,
    model_name TEXT,
    model_index INTEGER,
    prompt_index INTEGER,
    prompt_detail TEXT,
    comparison TEXT,
    feedback TEXT,
    satisfaction INTEGER,
    response_a TEXT,
    response_b TEXT,
    action TEXT
)
''')

# Load existing records to check for duplicates
existing_records = pd.read_sql_query("SELECT timestamp, code FROM feedback", conn)

# Check if existing_records is empty
if existing_records.empty:
    new_records = df
else:
    # Find new records by excluding existing ones based on 'timestamp' and 'code'
    new_records = df.merge(existing_records, on=['timestamp', 'code'], how='left', indicator=True)
    new_records = new_records[new_records['_merge'] == 'left_only'].drop(columns='_merge')

# Insert new records into the database
new_records.to_sql('feedback', conn, if_exists='append', index=False)

# Commit and close the connection
conn.commit()
conn.close()
