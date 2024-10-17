# %%
import sqlite3
import pandas as pd
con = sqlite3.connect("proj6_readings.sqlite")
cur = con.cursor()
result = cur.execute("SELECT count(*) from readings;").fetchall()
df = pd.DataFrame(result)
df

# %%
cur.execute("""CREATE INDEX IF NOT EXISTS detector_id ON readings (detector_id);""").fetchall() 
cur.execute("""CREATE INDEX IF NOT EXISTS starttime ON readings (starttime); """).fetchall()

# %%
query = "SELECT COUNT(DISTINCT detector_id) as detector_count FROM readings;"
df_result = pd.read_sql(query, con)

# %%
print(df_result)
df_result.to_pickle("proj6_ex01_detector_no.pkl")

# %%
query = """
        SELECT 
            detector_id, 
            COUNT(*) AS 'count(count)',
            MIN(starttime) AS 'min(starttime)',
            MAX(starttime) AS 'max(starttime)'
        FROM readings
        WHERE count IS NOT NULL
        GROUP BY detector_id;
        """

# %%
df_result = pd.read_sql(query, con)


# %%

df_result

# %%
df_result.to_pickle("proj6_ex02_detector_stat.pkl")

# %%
query = """
        SELECT detector_id,
               count,
               LAG(count) OVER (PARTITION BY detector_id ORDER BY starttime) AS prev_count
        FROM readings
        WHERE detector_id = 146
        ORDER BY starttime
        LIMIT 500;
        """

# %%
df_result = pd.read_sql(query, con)

# %%

df_result

# %%
df_result.to_pickle("proj6_ex03_detector_146_lag.pkl")

# %%
query = """
        SELECT detector_id,
               count,
               SUM(count) OVER (PARTITION BY detector_id ORDER BY starttime ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) AS window_sum
        FROM readings
        WHERE detector_id = 146
        ORDER BY starttime
        LIMIT 500;
        """


# %%
df_result = pd.read_sql(query, con)

# %%

df_result

# %%
df_result.to_pickle("proj6_ex04_detector_146_sum.pkl")
con.close

# %%
df_4 = pd.read_sql("""SELECT detector_id, count, 
SUM(count) OVER (PARTITION BY detector_id ORDER BY starttime ROWS BETWEEN CURRENT ROW AND 10 FOLLOWING) AS window_sum
FROM readings
WHERE detector_id = 146
LIMIT 500;""", con)
df_4.to_pickle("proj6_ex04_detector_146_sum.pkl")


