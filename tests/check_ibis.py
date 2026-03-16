import ibis
import polars as pl

df = pl.DataFrame({"a": [1, 2, 3]})
print("Creating polars connection")
con = ibis.polars.connect()
print("Connection created. Available methods:")
print([m for m in dir(con) if 'table' in m or 'register' in m or 'read' in m])
try:
    con.create_table("test", df)
    print("create_table succeeded!")
except Exception as e:
    print("create_table failed:", e)
    
try:
    con.register_df(df, "test")
    print("register_df succeeded!")
except Exception as e:
    print("register_df failed:", e)
    
try:
    con.read_polars(df, "test")
    print("read_polars succeeded!")
except Exception as e:
    print("read_polars failed:", e)
