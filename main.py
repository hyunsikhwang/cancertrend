import polars as pl
import httpx
import asyncio
import json
import os
import streamlit as st
from pyecharts import options as opts
from pyecharts.charts import Line, Bar
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

    /* Filter Section */
    .stSelectbox label, .stMultiSelect label {
        font-weight: 600 !important;
        color: #111111 !important;
        font-size: 0.9rem !important;
    }

    /* Hide unnecessary Streamlit components */
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
    
    # 연령별 발생률 계산 (Population이 0인 경우 처리)
    age_seg_df = joined.filter(pl.col("population").is_not_null()).with_columns(
        (pl.when(pl.col("population") > 0)
         .then((pl.col("cases") / pl.col("population")) * 100000)
         .otherwise(0.0))
        .round(2).alias("incidence_rate")
    )
    
    # 전체 연령(Total) 합계 계산
    total_df = joined.filter(pl.col("population").is_not_null()).group_by(["year", "gender", "cancer_type"]).agg([
        pl.col("cases").sum(),
        pl.col("population").sum()
    ]).with_columns([
        pl.lit("계(전체)").alias("age_group"),
        (pl.when(pl.col("population") > 0)
         .then((pl.col("cases") / pl.col("population")) * 100000)
         .otherwise(0.0))
        .round(2).alias("incidence_rate")
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

    # Filter Section
    st.markdown("### 🔍 Search Filters")
    col1, col2 = st.columns([1, 2])
    
    with col1:
        cancer_types = data["cancer_type"].unique().sort().to_list()
        # Find default index for "모든 암"
        default_idx = 0
        for i, ct in enumerate(cancer_types):
            if "모든" in ct and "암" in ct and "C00-C96" in ct:
                default_idx = i
                break
        
        selected_cancer = st.selectbox(
            "Cancer Type", 
            cancer_types, 
            index=default_idx
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

    # Sidebar Fallback
    st.sidebar.markdown("### Search Info")
    st.sidebar.info("차트 하단의 슬라이더를 통해 분석 기간을 자유롭게 조정할 수 있습니다.")

    # Apply Filters (excluding year range as it's handled by Pyecharts slider)
    filtered_df = data.filter(
        (pl.col("cancer_type") == selected_cancer) &
        (pl.col("age_group").is_in(selected_ages))
    )

    if not filtered_df.is_empty():
        st.subheader(f"📈 {selected_cancer} Trend Analysis")
        
        # Determine if Dual Axis is needed
        max_male = filtered_df.filter(pl.col("gender") == "남자")["incidence_rate"].max() or 0
        max_female = filtered_df.filter(pl.col("gender") == "여자")["incidence_rate"].max() or 0
        
        use_dual_axis = False
        if max_male > 0 and max_female > 0:
            ratio = max(max_male, max_female) / min(max_male, max_female)
            use_dual_axis = ratio > 2.5

        # Colors
        colors_male = ['#5470c6', '#73c0de', '#3ba272', '#516b91', '#002c53']
        colors_female = ['#ee6666', '#fac858', '#fc8452', '#ea7ccc', '#9a60b4']
        
        years = sorted(filtered_df["year"].unique().to_list())
        x_data = [str(y) for y in years]
        
        line_chart = Line(init_opts=opts.InitOpts(width="100%", height="650px"))
        line_chart.add_xaxis(xaxis_data=x_data)
        
        # Add Male Series
        male_df = filtered_df.filter(pl.col("gender") == "남자")
        if not male_df.is_empty():
            male_pivot = male_df.pivot(values="incidence_rate", index="year", on="age_group").sort("year")
            for i, age in enumerate(selected_ages):
                if age in male_pivot.columns:
                    y_vals = []
                    male_dict = dict(zip(male_pivot["year"].to_list(), male_pivot[age].to_list()))
                    for y in years:
                        val = male_dict.get(y, 0)
                        if val is None or (isinstance(val, float) and val != val):
                            val = 0
                        y_vals.append(float(val))
                    
                    line_chart.add_yaxis(
                        series_name=f"남 ({age})",
                        y_axis=y_vals,
                        is_smooth=True,
                        symbol_size=8,
                        yaxis_index=0,
                        label_opts=opts.LabelOpts(is_show=False),
                        linestyle_opts=opts.LineStyleOpts(width=3, color=colors_male[i % len(colors_male)]),
                        itemstyle_opts=opts.ItemStyleOpts(color=colors_male[i % len(colors_male)])
                    )
        
        # Add Female Series
        female_df = filtered_df.filter(pl.col("gender") == "여자")
        if not female_df.is_empty():
            female_pivot = female_df.pivot(values="incidence_rate", index="year", on="age_group").sort("year")
            for i, age in enumerate(selected_ages):
                if age in female_pivot.columns:
                    y_vals = []
                    female_dict = dict(zip(female_pivot["year"].to_list(), female_pivot[age].to_list()))
                    for y in years:
                        val = female_dict.get(y, 0)
                        if val is None or (isinstance(val, float) and val != val):
                            val = 0
                        y_vals.append(float(val))
                        
                    line_chart.add_yaxis(
                        series_name=f"여 ({age})",
                        y_axis=y_vals,
                        is_smooth=True,
                        symbol_size=8,
                        yaxis_index=1 if use_dual_axis else 0,
                        label_opts=opts.LabelOpts(is_show=False),
                        linestyle_opts=opts.LineStyleOpts(width=3, color=colors_female[i % len(colors_female)], type_="dashed"),
                        itemstyle_opts=opts.ItemStyleOpts(color=colors_female[i % len(colors_female)])
                    )

        # Axis Setup
        yaxis_primary = opts.AxisOpts(
            name="남성 발생률", 
            type_="value", 
            is_show=True,
            axislabel_opts=opts.LabelOpts(formatter="{value}"),
            splitline_opts=opts.SplitLineOpts(is_show=True),
            is_scale=True
        )
        
        if use_dual_axis:
            yaxis_secondary = opts.AxisOpts(
                name="여성 발생률", 
                type_="value", 
                is_show=True,
                axislabel_opts=opts.LabelOpts(formatter="{value}"),
                splitline_opts=opts.SplitLineOpts(is_show=False),
                is_scale=True
            )
            line_chart.extend_axis(yaxis=yaxis_secondary)
            st.info("💡 남/여 발생률 차이가 커서 우측 보조축을 사용합니다.")

        line_chart.set_global_opts(
            title_opts=opts.TitleOpts(title="Annual Incidence per 100k", subtitle="Solid: Male, Dashed: Female"),
            tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="cross"),
            legend_opts=opts.LegendOpts(pos_top="10%", orient="horizontal"),
            xaxis_opts=opts.AxisOpts(name="연도", type_="category", boundary_gap=False),
            yaxis_opts=yaxis_primary,
            datazoom_opts=[
                opts.DataZoomOpts(type_="slider", range_start=0, range_end=100),
                opts.DataZoomOpts(type_="inside", range_start=0, range_end=100)
            ],
        )
        
        st_pyecharts(line_chart, height="680px", key="chart_merged_v_final")

        # New Section: Top 10 Cancers by Gender
        st.markdown("<br><hr>", unsafe_allow_html=True)
        st.subheader("🏆 Top 10 Cancers by Incidence Rate")
        
        # Year selection for ranking
        all_years = sorted(data["year"].unique().to_list())
        ranking_year = st.select_slider(
            "분석 연도 선택",
            options=all_years,
            value=max(all_years),
            key="ranking_year_slider"
        )
        
        # Prepare ranking data
        ranking_df = data.filter(
            (pl.col("year") == ranking_year) & 
            (pl.col("age_group") == "계(전체)") &
            (~pl.col("cancer_type").str.contains("모든 ?암"))
        )

        def create_ranking_chart(df, gender_label, color):
            # Sort Top 10
            top10 = df.filter(pl.col("gender") == gender_label).sort("incidence_rate", descending=True).head(10)
            # For Pyecharts horizontal bar, reverse to show rank 1 at the top
            top10 = top10.reverse()
            
            c_names = top10["cancer_type"].to_list()
            c_rates = [round(float(x), 1) for x in top10["incidence_rate"].to_list()]
            
            bar = Bar(init_opts=opts.InitOpts(width="100%", height="450px"))
            bar.add_xaxis(c_names)
            bar.add_yaxis(
                "발생률", 
                c_rates, 
                label_opts=opts.LabelOpts(position="right"),
                itemstyle_opts=opts.ItemStyleOpts(color=color)
            )
            bar.reversal_axis()
            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=f"{gender_label} 암 발생 순위"),
                xaxis_opts=opts.AxisOpts(name="발생률", is_show=True),
                yaxis_opts=opts.AxisOpts(name="", axislabel_opts=opts.LabelOpts(font_size=11)),
                tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="shadow")
            )
            return bar

        col_rank_m, col_rank_f = st.columns(2)
        with col_rank_m:
            bar_m = create_ranking_chart(ranking_df, "남자", "#5470c6")
            st_pyecharts(bar_m, height="480px", key=f"rank_male_{ranking_year}")
        
        with col_rank_f:
            bar_f = create_ranking_chart(ranking_df, "여자", "#ee6666")
            st_pyecharts(bar_f, height="480px", key=f"rank_female_{ranking_year}")

        st.markdown("<br>", unsafe_allow_html=True)

        # Bottom Tabs
        tab1, tab2 = st.tabs(["📊 Data Table", "📋 Summary Stats"])
        with tab1:
            st.dataframe(filtered_df.to_pandas(), use_container_width=True)
        with tab2:
            summary = filtered_df.group_by(["gender", "age_group"]).agg([
                pl.col("incidence_rate").mean().alias("Avg Rate"),
                pl.col("incidence_rate").max().alias("Max Rate"),
                pl.col("cases").sum().alias("Total Cases")
            ]).sort(["gender", "age_group"])
            st.dataframe(summary.to_pandas(), use_container_width=True)
    else:
        st.warning("No data found.")

if __name__ == "__main__":
    main()
