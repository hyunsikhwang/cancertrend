import polars as pl
import json

def clean_age(age_str):
    if not age_str: return ""
    # "0 - 4세" -> "0-4"
    # "0-4세" -> "0-4"
    # "85세 이상" -> "85+"
    cleaned = age_str.replace("세", "").replace(" ", "").replace("이상", "+")
    return cleaned

def test_join():
    with open('api_samples.json', 'r', encoding='utf-8') as f:
        data = json.load(f)
    
    df_pop = pl.DataFrame(data['api1'])
    df_cancer = pl.DataFrame(data['api2'])
    
    # Preprocessing df_pop (Population)
    df_pop = df_pop.with_columns([
        pl.col("PRD_DE").alias("year"),
        pl.col("C2_NM").alias("gender"),
        pl.col("C3_NM").map_elements(clean_age, return_dtype=pl.String).alias("age_group"),
        pl.col("DT").cast(pl.Float64).alias("population")
    ]).select(["year", "gender", "age_group", "population"])
    
    # Preprocessing df_cancer (Cancer Cases)
    df_cancer = df_cancer.with_columns([
        pl.col("PRD_DE").alias("year"),
        pl.col("C2_NM").alias("gender"),
        pl.col("C3_NM").map_elements(clean_age, return_dtype=pl.String).alias("age_group"),
        pl.col("C1_NM").alias("cancer_type"),
        pl.col("DT").cast(pl.Float64).alias("cases")
    ]).select(["year", "gender", "age_group", "cancer_type", "cases"])
    
    print("--- Polars Schema Population ---")
    print(df_pop.head(2))
    print("\n--- Polars Schema Cancer ---")
    print(df_cancer.head(2))
    
    # Join Test (Note: year 2019 vs 1999 in samples, so we mock one year for join test)
    # Let's mock population to 1999 for test
    df_pop_mock = df_pop.with_columns(pl.lit("1999").alias("year"))
    
    joined = df_cancer.join(df_pop_mock, on=["year", "gender", "age_group"], how="inner")
    
    print("\n--- Joined Data Sample ---")
    print(joined.head(5))
    
    if len(joined) > 0:
        joined = joined.with_columns(
            ((pl.col("cases") / pl.col("population")) * 100000).alias("incidence_rate")
        )
        print("\n--- Final Table with Incidence Rate ---")
        print(joined.select(["year", "gender", "age_group", "cancer_type", "cases", "population", "incidence_rate"]).head(5))
    else:
        print("\n[WARNING] Join result is empty. Check keys again.")

if __name__ == "__main__":
    test_join()
