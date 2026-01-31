import polars as pl
import httpx
import asyncio
import json
import os
import streamlit as st
import plotly.express as px
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

# Page config
st.set_page_config(page_title="Cancer Incidence Trend Analysis", layout="wide")

# 환경 변수 로드 (로컬용)
load_dotenv()

# API 키 가져오기 (Streamlit Secrets 우선, 없으면 환경 변수)
API_KEY = st.secrets.get("KOSIS_API_KEY") or os.getenv("KOSIS_API_KEY")

if not API_KEY:
    st.error("KOSIS_API_KEY not found. Please set it in Streamlit Secrets or .env file.")
    st.stop()

def update_url_params(url, start_year, end_year):
    """URL의 startPrdDe와 endPrdDe 파라미터를 안전하게 업데이트하고 apiKey를 삽입합니다."""
    u = urlparse(url)
    query = parse_qs(u.query)
    query['startPrdDe'] = [str(start_year)]
    query['endPrdDe'] = [str(end_year)]
    query['apiKey'] = [API_KEY]
    new_query = urlencode(query, doseq=True)
    return urlunparse(u._replace(query=new_query))

async def fetch_api_batch(client, url_template, start_year, end_year):
    """비동기적으로 API 데이터를 수집합니다."""
    tasks = []
    for year in range(start_year, end_year + 1, 5):
        s_y = year
        e_y = min(year + 4, end_year)
        url = update_url_params(url_template, s_y, e_y)
        tasks.append(client.get(url, timeout=60.0))
    
    responses = await asyncio.gather(*tasks)
    
    all_data = []
    for i, resp in enumerate(responses):
        if resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list):
                    all_data.extend(data)
            except Exception as e:
                pass
    return all_data

def normalize_age(age_str):
    """연령대 명칭을 정규화합니다. 85세 이상 세분화 항목을 '85+'로 통합합니다."""
    if not age_str: return ""
    cleaned = age_str.replace("세", "").replace(" ", "").replace("이상", "+")
    if cleaned in ["85-89", "90-94", "95-99", "100+"]:
        return "85+"
    return cleaned

@st.cache_data(show_spinner="Fetching data from API...")
def get_processed_data():
    """데이터 수집 및 정제 과정을 수행하고 캐싱합니다."""
    return asyncio.run(_get_processed_data_async())

async def _get_processed_data_async():
    url_pop = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={API_KEY}&itmId=T10+&objL1=1+&objL2=1+2+&objL3=040+050+070+100+120+130+150+160+180+190+210+230+260+280+310+330+340+360+380+410+430+440+&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=1999&endPrdDe=2023&orgId=101&tblId=DT_1BPA001"
    url_cancer = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={API_KEY}&itmId=16117ac000101+&objL1=ALL&objL2=11101SSB21+11101SSB22+&objL3=15117AC001102+15117AC001103+15117AC001104+15117AC001105+15117AC001106+15117AC001107+15117AC001108+15117AC001109+15117AC001110+15117AC001111+15117AC001112+15117AC001113+15117AC001114+15117AC001115+15117AC001116+15117AC001117+15117AC001118+15117AC001119+15117AC001120+&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=1999&endPrdDe=2023&orgId=117&tblId=DT_117N_A0024"

    async with httpx.AsyncClient() as client:
        pop_raw = await fetch_api_batch(client, url_pop, 1999, 2023)
        cancer_raw = await fetch_api_batch(client, url_cancer, 1999, 2023)

    if not pop_raw or not cancer_raw:
        return None

    df_pop = pl.DataFrame(pop_raw)
    df_cancer = pl.DataFrame(cancer_raw)

    # 인구 데이터 정제
    df_pop = df_pop.with_columns([
        pl.col("PRD_DE").cast(pl.Int32).alias("year"),
        pl.col("C2_NM").alias("gender"),
        pl.col("C3_NM").map_elements(normalize_age, return_dtype=pl.String).alias("age_group"),
        pl.col("DT").cast(pl.Float64).alias("population")
    ]).select(["year", "gender", "age_group", "population"])

    # 1999년 80+ 데이터 추산 로직
    pop_2000 = df_pop.filter(pl.col("year") == 2000)
    dist_2000 = pop_2000.filter(pl.col("age_group").is_in(["80-84", "85+"])).group_by(["gender"]).agg([
        pl.col("population").filter(pl.col("age_group") == "80-84").sum().alias("pop_80_84"),
        pl.col("population").filter(pl.col("age_group") == "85+").sum().alias("pop_85_up"),
        pl.col("population").sum().alias("total_80_plus")
    ]).with_columns([
        (pl.col("pop_80_84") / pl.col("total_80_plus")).alias("ratio_80_84"),
        (pl.col("pop_85_up") / pl.col("total_80_plus")).alias("ratio_85_up")
    ])

    pop_1999_80_plus = df_pop.filter((pl.col("year") == 1999) & (pl.col("age_group") == "80+"))
    if len(pop_1999_80_plus) > 0:
        estimated_1999 = pop_1999_80_plus.join(dist_2000.select(["gender", "ratio_80_84", "ratio_85_up"]), on="gender")
        estimated_80_84 = estimated_1999.with_columns([
            pl.lit("80-84").alias("age_group"),
            (pl.col("population") * pl.col("ratio_80_84")).alias("population")
        ]).select(["year", "gender", "age_group", "population"])
        estimated_85_up = estimated_1999.with_columns([
            pl.lit("85+").alias("age_group"),
            (pl.col("population") * pl.col("ratio_85_up")).alias("population")
        ]).select(["year", "gender", "age_group", "population"])
        df_pop = df_pop.filter(~((pl.col("year") == 1999) & (pl.col("age_group") == "80+")))
        df_pop = pl.concat([df_pop, estimated_80_84, estimated_85_up])

    df_pop = df_pop.group_by(["year", "gender", "age_group"]).agg(pl.col("population").sum())

    # 암 데이터 정제
    df_cancer = df_cancer.with_columns([
        pl.col("PRD_DE").cast(pl.Int32).alias("year"),
        pl.col("C2_NM").alias("gender"),
        pl.col("C3_NM").map_elements(normalize_age, return_dtype=pl.String).alias("age_group"),
        pl.col("C1_NM").alias("cancer_type"),
        pl.col("DT").cast(pl.Float64).alias("cases")
    ]).select(["year", "gender", "age_group", "cancer_type", "cases"])
    df_cancer = df_cancer.unique()

    # 조인
    joined = df_cancer.join(df_pop, on=["year", "gender", "age_group"], how="left")
    
    # 최종 산출
    final_df = joined.filter(pl.col("population").is_not_null()).with_columns(
        ((pl.col("cases") / pl.col("population")) * 100000).round(2).alias("incidence_rate")
    ).sort(["year", "gender", "age_group", "cancer_type"])
    
    return final_df

def main():
    st.title("📊 Cancer Incidence Trend Analysis (1999-2023)")
    st.markdown("KOSIS API 데이터를 활용하여 암 발생률 추이를 분석합니다.")

    data = get_processed_data()

    if data is None:
        st.error("Failed to fetch or process data.")
        return

    # Sidebar Filters
    st.sidebar.header("Filters")
    
    genders = data["gender"].unique().to_list()
    selected_gender = st.sidebar.selectbox("Select Gender", genders)
    
    cancer_types = data["cancer_type"].unique().sort().to_list()
    selected_cancer = st.sidebar.selectbox("Select Cancer Type", cancer_types, index=cancer_types.index("모든 암(C00-C96)") if "모든 암(C00-C96)" in cancer_types else 0)
    
    age_groups = data["age_group"].unique().sort().to_list()
    selected_ages = st.sidebar.multiselect("Select Age Groups", age_groups, default=age_groups)

    # Filter Data
    filtered_df = data.filter(
        (pl.col("gender") == selected_gender) &
        (pl.col("cancer_type") == selected_cancer) &
        (pl.col("age_group").is_in(selected_ages))
    )

    # Visualizations
    st.subheader(f"📈 Trend: {selected_cancer} ({selected_gender})")
    
    if len(filtered_df) > 0:
        # Chart 1: Incidence Rate over Years by Age Group
        fig = px.line(
            filtered_df.to_pandas(), 
            x="year", 
            y="incidence_rate", 
            color="age_group",
            title=f"Incidence Rate per 100,000 population",
            labels={"incidence_rate": "Incidence Rate", "year": "Year", "age_group": "Age Group"}
        )
        st.plotly_chart(fig, use_container_width=True)

        # Tabs for more details
        tab1, tab2 = st.tabs(["Data Table", "Summary Stats"])
        
        with tab1:
            st.dataframe(filtered_df.to_pandas(), use_container_width=True)
            
        with tab2:
            summary = filtered_df.group_by("age_group").agg([
                pl.col("incidence_rate").mean().alias("avg_rate"),
                pl.col("incidence_rate").max().alias("max_rate"),
                pl.col("cases").sum().alias("total_cases")
            ]).sort("age_group")
            st.dataframe(summary.to_pandas(), use_container_width=True)
    else:
        st.warning("No data matching the selected filters.")

if __name__ == "__main__":
    main()
