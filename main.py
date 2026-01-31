import polars as pl
import httpx
import asyncio
import json
import os
import streamlit as st
from pyecharts import options as opts
from pyecharts.charts import Line
from streamlit_echarts import st_pyecharts
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

# Page config
st.set_page_config(
    page_title="Cancer Incidence Trend",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# 환경 변수 로드 (로컬용)
load_dotenv()

# API 키 가져오기 (Streamlit Secrets 우선, 없으면 환경 변수)
API_KEY = st.secrets.get("KOSIS_API_KEY") or os.getenv("KOSIS_API_KEY")

# Custom CSS for Value Horizon Look & Feel
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600;700&display=swap');

    .block-container {
        padding-top: 1.5rem !important;
        padding-bottom: 2rem !important;
        max-width: 1200px !important;
    }
    
    /* [data-testid="stHeader"] {
        display: none;
    } */

    .stApp {
        background-color: #ffffff;
        color: #1a1a1a;
        font-family: 'Inter', sans-serif;
    }

    /* Hero Section */
    .hero-container {
        padding: 2rem 0;
        text-align: center;
        border-bottom: 1px solid #f0f0f0;
        margin-bottom: 1.5rem;
    }

    .hero-title {
        font-size: 2.5rem;
        font-weight: 700;
        color: #111111;
        margin-bottom: 0.5rem;
        letter-spacing: -1px;
    }

    .hero-subtitle {
        font-size: 1.1rem;
        font-weight: 400;
        color: #666666;
    }

    /* Filter Sidebar/Section */
    .filter-section {
        background-color: #f8f9fa;
        padding: 1.5rem;
        border-radius: 12px;
        margin-bottom: 2rem;
        border: 1px solid #eaeaea;
    }

    [data-testid="stSidebar"] {
        background-color: #f8f9fa;
        border-right: 1px solid #eaeaea;
    }

    .stSelectbox label, .stMultiSelect label {
        font-weight: 600 !important;
        color: #111111 !important;
        font-size: 0.9rem !important;
    }

    /* Hide unnecessary Streamlit components but keep the header for sidebar toggle */
    #MainMenu, footer, .stDeployButton {
        display: none !important;
    }
</style>
""", unsafe_allow_html=True)

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
    
    # 연령별 발생률 계산
    age_seg_df = joined.filter(pl.col("population").is_not_null()).with_columns(
        ((pl.col("cases") / pl.col("population")) * 100000).round(2).alias("incidence_rate")
    )
    
    # 전체 연령(Total) 합계 계산
    total_df = joined.filter(pl.col("population").is_not_null()).group_by(["year", "gender", "cancer_type"]).agg([
        pl.col("cases").sum(),
        pl.col("population").sum()
    ]).with_columns([
        pl.lit("계(전체)").alias("age_group"),
        ((pl.col("cases") / pl.col("population")) * 100000).round(2).alias("incidence_rate")
    ])
    
    # 최종 결합
    final_df = pl.concat([
        age_seg_df.select(["year", "gender", "age_group", "cancer_type", "cases", "incidence_rate"]),
        total_df.select(["year", "gender", "age_group", "cancer_type", "cases", "incidence_rate"])
    ]).sort(["year", "gender", "age_group", "cancer_type"])
    
    return final_df

def main():
    # Hero Section
    st.markdown("""
    <div class="hero-container">
        <div class="hero-title">📊 Cancer Incidence Trend</div>
        <div class="hero-subtitle">KOSIS API 기반 암 발생률 추이 분석 (1999-2023)</div>
    </div>
    """, unsafe_allow_html=True)

    if not API_KEY:
        st.error("🔑 **KOSIS_API_KEY not found.**")
        st.info("Streamlit Cloud의 App Settings > Secrets에 `KOSIS_API_KEY = 'your_key_here'`를 추가해주세요.")
        return

    try:
        data = get_processed_data()
    except Exception as e:
        st.error(f"❌ **Data Processing Error:** {e}")
        return

    if data is None or len(data) == 0:
        st.error("📡 **Failed to fetch data from KOSIS API.**")
        st.warning("API 키가 유효한지 또는 KOSIS 서버가 정상인지 확인해주세요.")
        return

    # Filter Section - Move to main flow for visibility
    st.markdown("### 🔍 Filter Configuration")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        cancer_types = data["cancer_type"].unique().sort().to_list()
        selected_cancer = st.selectbox(
            "Cancer Type", 
            cancer_types, 
            index=cancer_types.index("모든 암(C00-C96)") if "모든 암(C00-C96)" in cancer_types else 0
        )
    
    with col2:
        age_groups = data["age_group"].unique().sort().to_list()
        if "계(전체)" in age_groups:
            age_groups.remove("계(전체)")
            age_groups = ["계(전체)"] + age_groups
            
        selected_ages = st.multiselect(
            "Age Groups", 
            age_groups, 
            default=["계(전체)"] if "계(전체)" in age_groups else age_groups[:1]
        )

    # Sidebar Filter as Fallback/Additional Info
    st.sidebar.markdown("### Search Info")
    st.sidebar.info("선택된 암 종류와 연령대에 대해 남/여 발생률 추이를 함께 비교합니다.")

    # Filter Data for both Genders
    filtered_df = data.filter(
        (pl.col("cancer_type") == selected_cancer) &
        (pl.col("age_group").is_in(selected_ages))
    )

    if len(filtered_df) > 0:
        st.subheader(f"📈 {selected_cancer} Trend (Male vs Female)")
        
        # Ranges for Y-axis decision
        max_male = filtered_df.filter(pl.col("gender") == "남")["incidence_rate"].max() or 0
        max_female = filtered_df.filter(pl.col("gender") == "여")["incidence_rate"].max() or 0
        
        # Use dual axis if ranges differ significantly (more than 2.5x)
        range_diff = max(max_male, max_female) / min(max_male, max_female) if min(max_male, max_female) > 0 else 0
        use_dual_axis = range_diff > 2.5
        
        # Prepare Data for Pyecharts
        x_data = sorted([str(y) for y in filtered_df["year"].unique().to_list()])
        
        line_chart = Line(init_opts=opts.InitOpts(width="100%", height="650px"))
        line_chart.add_xaxis(xaxis_data=x_data)
        
        # Y-Axis options
        yaxis_male = opts.AxisOpts(name="남 발생률", type_="value", axislabel_opts=opts.LabelOpts(formatter="{value}"))
        yaxis_female = opts.AxisOpts(name="여 발생률", type_="value", axislabel_opts=opts.LabelOpts(formatter="{value}")) if use_dual_axis else None

        if use_dual_axis:
            line_chart.extend_axis(yaxis=yaxis_female)
            st.info("💡 남/여 발생률 수치 차이가 커서 보조축(오른쪽)을 사용하여 표시합니다.")

        # Color Palette
        colors_male = ['#5470c6', '#73c0de', '#3ba272', '#516b91', '#002c53']
        colors_female = ['#ee6666', '#fac858', '#fc8452', '#ea7ccc', '#9a60b4']
        
        # Add Male Series
        male_data_df = filtered_df.filter(pl.col("gender") == "남")
        if not male_data_df.is_empty():
            male_pivot = male_data_df.pivot(values="incidence_rate", index="year", on="age_group").sort("year")
            for i, age in enumerate(selected_ages):
                if age in male_pivot.columns:
                    line_chart.add_yaxis(
                        series_name=f"남 ({age})",
                        y_axis=male_pivot[age].fill_null(0).to_list(),
                        is_smooth=True,
                        symbol_size=8,
                        label_opts=opts.LabelOpts(is_show=False),
                        linestyle_opts=opts.LineStyleOpts(width=3, color=colors_male[i % len(colors_male)]),
                        itemstyle_opts=opts.ItemStyleOpts(color=colors_male[i % len(colors_male)])
                    )
        
        # Add Female Series
        female_data_df = filtered_df.filter(pl.col("gender") == "여")
        if not female_data_df.is_empty():
            female_pivot = female_data_df.pivot(values="incidence_rate", index="year", on="age_group").sort("year")
            for i, age in enumerate(selected_ages):
                if age in female_pivot.columns:
                    line_chart.add_yaxis(
                        series_name=f"여 ({age})",
                        y_axis=female_pivot[age].fill_null(0).to_list(),
                        is_smooth=True,
                        symbol_size=8,
                        yaxis_index=1 if use_dual_axis else 0,
                        label_opts=opts.LabelOpts(is_show=False),
                        linestyle_opts=opts.LineStyleOpts(width=3, color=colors_female[i % len(colors_female)], type_="dashed"),
                        itemstyle_opts=opts.ItemStyleOpts(color=colors_female[i % len(colors_female)])
                    )
        
        line_chart.set_global_opts(
            title_opts=opts.TitleOpts(title="남/여 암 발생률 추이 비교", subtitle="실선: 남성, 점선: 여성"),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            legend_opts=opts.LegendOpts(pos_top="10%", orient="horizontal"),
            xaxis_opts=opts.AxisOpts(name="연도", type_="category", boundary_gap=False),
            yaxis_opts=yaxis_male,
            datazoom_opts=[opts.DataZoomOpts(type_="inside")],
        )
        
        st_pyecharts(line_chart, height="650px", key="cancer_trend_dual_v4")

        st.markdown("<br>", unsafe_allow_html=True)

        # Tabs for more details
        tab1, tab2 = st.tabs(["📊 Data Table", "📋 Summary Stats"])
        
        with tab1:
            st.dataframe(filtered_df.to_pandas(), use_container_width=True)
            
        with tab2:
            summary = filtered_df.group_by(["gender", "age_group"]).agg([
                pl.col("incidence_rate").mean().alias("Mean Rate"),
                pl.col("incidence_rate").max().alias("Max Rate"),
                pl.col("cases").sum().alias("Total Cases")
            ]).sort(["gender", "age_group"])
            st.dataframe(summary.to_pandas(), use_container_width=True)
    else:
        st.warning("No data matching the selected filters.")

if __name__ == "__main__":
    main()
