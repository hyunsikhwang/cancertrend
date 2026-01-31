import polars as pl

# Mock data
data = pl.DataFrame({
    "year": [2000, 2001, 2000, 2001],
    "gender": ["남", "남", "여", "여"],
    "age_group": ["계(전체)", "계(전체)", "계(전체)", "계(전체)"],
    "incidence_rate": [100.5, 110.2, 80.3, 85.1]
})

selected_ages = ["계(전체)"]
years = [2000, 2001]

male_df = data.filter(pl.col("gender") == "남")
if not male_df.is_empty():
    male_pivot = male_df.pivot(values="incidence_rate", index="year", on="age_group").sort("year")
    print("Male Pivot columns:", male_pivot.columns)
    print("Male Pivot data:\n", male_pivot)
    
    for age in selected_ages:
        if age in male_pivot.columns:
            male_dict = dict(zip(male_pivot["year"].to_list(), male_pivot[age].to_list()))
            print(f"Male Dict for {age}:", male_dict)
            y_vals = []
            for y in years:
                val = male_dict.get(y, 0)
                if val is None or (isinstance(val, float) and val != val):
                    val = 0
                y_vals.append(val)
            print(f"Y Vals for {age}:", y_vals)

female_df = data.filter(pl.col("gender") == "여")
if not female_df.is_empty():
    female_pivot = female_df.pivot(values="incidence_rate", index="year", on="age_group").sort("year")
    print("Female Pivot columns:", female_pivot.columns)
    
    for age in selected_ages:
        if age in female_pivot.columns:
            female_dict = dict(zip(female_pivot["year"].to_list(), female_pivot[age].to_list()))
            y_vals = []
            for y in years:
                val = female_dict.get(y, 0)
                y_vals.append(val)
            print(f"Female Y Vals for {age}:", y_vals)
