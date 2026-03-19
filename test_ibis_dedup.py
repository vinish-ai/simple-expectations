import ibis
import pandas as pd

df = pd.DataFrame({"age": [25, 30, None]})
con = ibis.duckdb.connect()
con.create_table("users", ibis.memtable(df))
t = con.table("users")

expr1 = (~t["age"].isnull()).ifelse(1, 0).sum()
expr2 = (~t["age"].isnull()).ifelse(1, 0).sum()
expr3 = t.count()
expr4 = t.count()

print(f"expr1.equals(expr2): {expr1.equals(expr2)}")
print(f"expr3.equals(expr4): {expr3.equals(expr4)}")
print(f"str(expr1) == str(expr2): {str(expr1) == str(expr2)}")
