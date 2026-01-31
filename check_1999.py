import polars as pl
import json
import os

with open('raw_pop_cache.json', 'r', encoding='utf-8') as f:
    pop_data = json.load(f)

with open('raw_cancer_cache.json', 'r', encoding='utf-8') as f:
    cancer_data = json.load(f)

df_pop = pl.DataFrame(pop_data)
df_cancer = pl.DataFrame(cancer_data)

print("1999 Population unique C3/C3_NM:")
print(df_pop.filter(pl.col('PRD_DE') == '1999').select(['C3', 'C3_NM']).unique().sort('C3'))

print("\n2000 Population unique C3/C3_NM:")
print(df_pop.filter(pl.col('PRD_DE') == '2000').select(['C3', 'C3_NM']).unique().sort('C3'))

print("\n1999 Cancer unique C3/C3_NM:")
print(df_cancer.filter(pl.col('PRD_DE') == '1999').select(['C3', 'C3_NM']).unique().sort('C3'))
