import polars as pl

# load initial (and clean) data
df = pl.read_csv("preprocessed_data/eHRs-gen2.csv")

# aggregate by age
df = df\
  .group_by(["p_id","age"])\
  .agg([
    pl.col(col).max().alias(col) for col in df.columns if col not in ["age","p_id"]
  ])

# save the result
df\
  .sort("p_id")\
  .write_csv("preprocessed_data/eHRs-gen2-aggregated-1y.csv")
pass