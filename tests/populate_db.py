import duckdb
import pandas as pd

df = pd.DataFrame({
    "id": [1, 2, 3, 4],
    "age": [20, 25, None, 40],
    "status": ["active", "active", "inactive", "pending"]
})

con = duckdb.connect("my_test_data.db")
con.execute("CREATE TABLE users AS SELECT * FROM df")
con.close()
print("Created my_test_data.db with table 'users'")
