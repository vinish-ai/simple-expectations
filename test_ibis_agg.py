import ibis
import pandas as pd

df = pd.DataFrame({"age": [25, 30, 28, 22, 35, None]})
con = ibis.duckdb.connect()
con.create_table("users", ibis.memtable(df))
t = con.table("users")

col = t["age"]
try:
    print(f"std: {col.std().name('std_val')}")
    print(f"approx_median: {col.approx_median().name('med_val')}")
    
    res = t.aggregate(
        std_val=col.std(),
        med_val=col.approx_median()
    ).execute().to_dict('records')[0]
    print(res)
except Exception as e:
    print(f"Error: {e}")
