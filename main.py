import polars as pl
import httpx
import asyncio
import json
import os
import streamlit as st
from pyecharts import options as opts
from pyecharts.charts import Line, Bar, Grid, Timeline
from streamlit_echarts import st_pyecharts
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

# Define stable colors for cancer types
CANCER_COLORS = {
    "위(C16)": "#5470c6", "간(C22)": "#91cc75", "폐(C33-C34)": "#fac858",
    "대장(C18-C20)": "#ee6666", "유방(C50)": "#73c0de", "갑상선(C73)": "#3ba272",
    "전립선(C61)": "#fc8452", "췌장(C25)": "#9a60b4", "담낭 및 기타 담도(C23-C24)": "#ea7ccc",
    "신장(C64)": "#5470c6", "방광(C67)": "#91cc75", "백혈병(C91-C95)": "#fac858",
    "비호지킨 림프종(C82-C86 C96)": "#ee6666", "식도(C15)": "#73c0de",
    "결장(C18)": "#3ba272", "직장(C19-C20)": "#fc8452", "자궁경부(C53)": "#9a60b4"
}

def get_cancer_color(name):
    """Returns a stable color for a given cancer name."""
    if name in CANCER_COLORS:
        return CANCER_COLORS[name]
    # Fallback to a hash-based color if not predefined
    import hashlib
    hash_object = hashlib.md5(name.encode())
    return f"#{hash_object.hexdigest()[:6]}"

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
    # Standardize separator and remove noise
    cleaned = age_str.replace("세", "").replace(" ", "").replace("이상", "+").replace("~", "-")
    if cleaned in ["85-89", "90-94", "95-99", "100+"]:
        return "85+"
    return cleaned

@st.cache_data(show_spinner="Fetching data from API...")
def get_processed_data_v2():
    """데이터 수집 및 정제 과정을 수행하고 캐싱합니다. (v2: 인구수 데이터 포함)"""
    return asyncio.run(_get_processed_data_async())

def map_to_custom_age_group(age):
    """정규화된 연령대를 요청된 5개 그룹으로 매핑합니다."""
    if age in ["0-4", "5-9", "10-14", "15-19"]:
        return "0-19세"
    elif age in ["20-24", "25-29", "30-34", "35-39"]:
        return "20-39세"
    elif age in ["40-44", "45-49"]:
        return "40-49세"
    elif age in ["50-54", "55-59"]:
        return "50-59세"
    elif age in ["60-64", "65-69", "70-74", "75-79", "80-84", "85+"]:
        return "60세+"
    return None

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
    
    # 1. 연령별 데이터 계산
    # API의 DT는 암발생자수(cases)로 간주하며, 발생률은 (발생자수 / 인구수) * 100,000으로 수동 계산합니다.
    age_seg_df = joined.filter(pl.col("population").is_not_null()).with_columns(
        (pl.when(pl.col("population") > 0)
         .then((pl.col("cases") / pl.col("population")) * 100000)
         .otherwise(0.0))
        .round(2).alias("incidence_rate")
    )
    
    # 2. 전체 연령(Total) 합계 및 발생률 재계산
    total_df = joined.filter(pl.col("population").is_not_null()).group_by(["year", "gender", "cancer_type"]).agg([
        pl.col("cases").sum().alias("cases"),
        pl.col("population").sum().alias("population")
    ]).with_columns([
        pl.lit("계(전체)").alias("age_group"),
        (pl.when(pl.col("population") > 0)
         .then((pl.col("cases") / pl.col("population")) * 100000)
         .otherwise(0.0))
        .round(2).alias("incidence_rate")
    ])
    
    # 최종 결합
    final_df = pl.concat([
        age_seg_df.select(["year", "gender", "age_group", "cancer_type", "cases", "incidence_rate", "population"]),
        total_df.select(["year", "gender", "age_group", "cancer_type", "cases", "incidence_rate", "population"])
    ]).sort(["year", "gender", "age_group", "cancer_type"])
    
    return final_df

def main():
    # Hero Section
    h_col1, h_col2 = st.columns([1, 6])
    with h_col1:
        st.image("app_logo.png", width=120)
    with h_col2:
        st.markdown("""
        <div class="hero-container" style="text-align: left; padding: 0.5rem 0;">
            <div class="hero-title" style="font-size: 2.2rem; margin-top: 10px;">Cancer Incidence Trend</div>
            <div class="hero-subtitle">KOSIS API 기반 암 발생률 추이 분석 (1999-2023)</div>
        </div>
        """, unsafe_allow_html=True)

    if not API_KEY:
        st.error("🔑 **KOSIS_API_KEY not found.**")
        st.info("Streamlit Cloud의 App Settings > Secrets에 `KOSIS_API_KEY = 'your_key_here'`를 추가해주세요.")
        return

    try:
        data = get_processed_data_v2()
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
        
        selected_cancers = st.multiselect(
            "Cancer Type(s)", 
            cancer_types, 
            default=[cancer_types[default_idx]]
        )
        
        # 제외할 암종 선택 (목록에 "모든암" 성격의 항목이 포함된 경우에만)
        excluded_cancers = []
        is_all_cancer_selected = any("모든" in ct and "암" in ct for ct in selected_cancers)
        
        if is_all_cancer_selected:
            other_cancer_types = [ct for ct in cancer_types if not ("모든" in ct and "암" in ct)]
            excluded_cancers = st.multiselect(
                "제외할 암종 선택 (발생률 차감)",
                other_cancer_types,
                help="일부 암종을 제외한 전체 발생률을 보려면 여기서 선택하세요."
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
    if not selected_cancers:
        filtered_df = pl.DataFrame()
    elif is_all_cancer_selected:
        # 1. 모든암 모드 (리스트에 모든암이 포함된 경우, 첫 번째 모든암 항목 기준)
        primary_all_cancer = [ct for ct in selected_cancers if "모든" in ct and "암" in ct][0]
        all_cancer_df = data.filter(
            (pl.col("cancer_type") == primary_all_cancer) &
            (pl.col("age_group").is_in(selected_ages))
        )
        
        if excluded_cancers:
            exclude_df = data.filter(
                (pl.col("cancer_type").is_in(excluded_cancers)) &
                (pl.col("age_group").is_in(selected_ages))
            )
            exclude_sum = exclude_df.group_by(["year", "gender", "age_group"]).agg(
                pl.col("cases").sum().alias("exclude_cases")
            )
            filtered_df = all_cancer_df.join(exclude_sum, on=["year", "gender", "age_group"], how="left").with_columns(
                pl.col("exclude_cases").fill_null(0)
            ).with_columns(
                (pl.col("cases") - pl.col("exclude_cases")).alias("cases")
            ).with_columns(
                (pl.when(pl.col("population") > 0)
                 .then((pl.col("cases") / pl.col("population")) * 100000)
                 .otherwise(0.0))
                .round(2).alias("incidence_rate")
            ).drop("exclude_cases")
        else:
            filtered_df = all_cancer_df
    else:
        # 2. 개별 암종 복수 선택 및 합산 모드
        base_filtered = data.filter(
            (pl.col("cancer_type").is_in(selected_cancers)) &
            (pl.col("age_group").is_in(selected_ages))
        )
        
        if len(selected_cancers) > 1:
            # 여러 개 선택 시 합산
            filtered_df = base_filtered.group_by(["year", "gender", "age_group"]).agg([
                pl.col("cases").sum(),
                pl.col("population").first() # 동일 그룹이면 인구는 같음
            ]).with_columns(
                (pl.when(pl.col("population") > 0)
                 .then((pl.col("cases") / pl.col("population")) * 100000)
                 .otherwise(0.0))
                .round(2).alias("incidence_rate")
            ).with_columns(
                pl.lit(", ".join(selected_cancers)).alias("cancer_type")
            )
        else:
            # 단일 선택 시 그대로 사용
            filtered_df = base_filtered
    # Section: Trends
    st.markdown("<br>", unsafe_allow_html=True)
    col_icon1, col_text1 = st.columns([1, 15])
    with col_icon1:
        st.image("trend_icon.png", width=40)
    with col_text1:
        st.subheader("Annual Incidence Trends")

    # Main Visualization
    if not filtered_df.is_empty():
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
        col_icon2, col_text2 = st.columns([1, 15])
        with col_icon2:
            st.image("ranking_icon.png", width=40)
        with col_text2:
            st.subheader("Top 10 Cancers Ranking")
        
        mode = st.radio("보기 모드 선택", ["정적 분석 (연도 선택)", "애니메이션 분석 (Bar Chart Race)"], horizontal=True)
        
        all_years = sorted(data["year"].unique().to_list())
        
        if mode == "정적 분석 (연도 선택)":
            ranking_year = st.select_slider(
                "분석 연도 선택",
                options=all_years,
                value=max(all_years),
                key="ranking_year_slider"
            )
            
            ranking_df = data.filter(
                (pl.col("year") == ranking_year) & 
                (pl.col("age_group") == "계(전체)") &
                (~pl.col("cancer_type").str.contains("모든 ?암"))
            )

            def create_ranking_chart(df, gender_label, color_hint):
                # Sort Top 10
                top10 = df.filter(pl.col("gender") == gender_label).sort("incidence_rate", descending=True).head(10)
                top10 = top10.reverse()
                
                c_names = top10["cancer_type"].to_list()
                c_rates = [round(float(x), 1) for x in top10["incidence_rate"].to_list()]
                
                # Create per-item color list
                bar_colors = [get_cancer_color(name) for name in c_names]
                
                bar = Bar(init_opts=opts.InitOpts(width="100%", height="450px"))
                bar.add_xaxis(c_names)
                
                # use data pairs to apply individual colors
                data_points = []
                for name, rate, color in zip(c_names, c_rates, bar_colors):
                    data_points.append(
                        opts.BarItem(name=name, value=rate, itemstyle_opts=opts.ItemStyleOpts(color=color))
                    )
                
                bar.add_yaxis(
                    "발생률", 
                    data_points, 
                    label_opts=opts.LabelOpts(position="right")
                )
                bar.reversal_axis()
                bar.set_global_opts(
                    title_opts=opts.TitleOpts(title=f"{ranking_year}년 {gender_label} 암 발생 순위"),
                    xaxis_opts=opts.AxisOpts(name="발생률", is_show=True),
                    yaxis_opts=opts.AxisOpts(
                        name="", 
                        axislabel_opts=opts.LabelOpts(font_size=11, margin=15)
                    ),
                    tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="shadow")
                )
                
                grid = Grid(init_opts=opts.InitOpts(width="100%", height="480px"))
                grid.add(bar, grid_opts=opts.GridOpts(pos_left="35%", pos_right="10%"))
                return grid

            col_rank_m, col_rank_f = st.columns(2)
            with col_rank_m:
                st_pyecharts(create_ranking_chart(ranking_df, "남자", "#5470c6"), height="480px", key=f"rank_m_{ranking_year}")
            with col_rank_f:
                st_pyecharts(create_ranking_chart(ranking_df, "여자", "#ee6666"), height="480px", key=f"rank_f_{ranking_year}")

        else:
            # Bar Chart Race using Timeline
            st.info("💡 하단의 플레이 버튼(▶)을 누르면 1999년부터 2023년까지의 변화를 보실 수 있습니다.")
            
            def create_race_chart(df, gender_label):
                tl = Timeline(init_opts=opts.InitOpts(width="100%", height="520px"))
                tl.add_schema(is_auto_play=False, play_interval=800, is_loop_play=False, pos_bottom="-5px")
                
                for year in all_years:
                    year_df = df.filter(
                        (pl.col("year") == year) & 
                        (pl.col("age_group") == "계(전체)") &
                        (~pl.col("cancer_type").str.contains("모든 ?암")) &
                        (pl.col("gender") == gender_label)
                    ).sort("incidence_rate", descending=True).head(10).reverse()
                    
                    if not year_df.is_empty():
                        c_names = year_df["cancer_type"].to_list()
                        c_rates = [round(float(x), 1) for x in year_df["incidence_rate"].to_list()]
                        
                        # Create per-item color list
                        data_points = []
                        for name, rate in zip(c_names, c_rates):
                            color = get_cancer_color(name)
                            data_points.append(
                                opts.BarItem(name=name, value=rate, itemstyle_opts=opts.ItemStyleOpts(color=color))
                            )

                        bar = (
                            Bar()
                            .add_xaxis(c_names)
                            .add_yaxis(
                                "발생률", 
                                data_points, 
                                label_opts=opts.LabelOpts(position="right")
                            )
                            .reversal_axis()
                            .set_global_opts(
                                title_opts=opts.TitleOpts(title=f"{year}년 {gender_label} 암 발생 순위"),
                                xaxis_opts=opts.AxisOpts(name="발생률", is_show=True),
                                yaxis_opts=opts.AxisOpts(
                                    name="", 
                                    axislabel_opts=opts.LabelOpts(font_size=11, margin=15)
                                ),
                                tooltip_opts=opts.TooltipOpts(trigger="axis", axis_pointer_type="shadow")
                            )
                        )
                        # Wrap each year's bar in grid for margin
                        grid = Grid()
                        grid.add(bar, grid_opts=opts.GridOpts(pos_left="35%", pos_right="10%"))
                        tl.add(grid, f"{year}년")
                return tl

            col_race_m, col_race_f = st.columns(2)
            with col_race_m:
                st_pyecharts(create_race_chart(data, "남자"), height="550px", key="race_male")
            with col_race_f:
                st_pyecharts(create_race_chart(data, "여자"), height="550px", key="race_female")

        # New Section: Incidence Proportion by Age Group
        st.markdown("<br><hr>", unsafe_allow_html=True)
        col_icon3, col_text3 = st.columns([1, 15])
        with col_icon3:
            st.image("chart_icon.png", width=40)
        with col_text3:
            st.subheader("Cancer Incidence Proportion by Age Group")
        
        prop_year = st.select_slider(
            "분석 연도 선택 (비중 차트)",
            options=all_years,
            value=max(all_years),
            key="prop_year_slider"
        )
        
        # Data transformation for Proportion Chart
        df_prop = data.filter(
            (pl.col("year") == prop_year) & 
            (pl.col("age_group") != "계(전체)") &
            (~pl.col("cancer_type").str.contains("모든 ?암"))
        ).with_columns(
            pl.col("age_group").map_elements(map_to_custom_age_group, return_dtype=pl.String).alias("custom_age_group")
        ).filter(pl.col("custom_age_group").is_not_null())
        
        # Ensure population column exists (defensive)
        if "population" not in df_prop.columns:
            st.error("데이터에 'population' 컬럼이 누락되었습니다. 캐시를 새로고침합니다.")
            st.cache_data.clear()
            st.rerun()
        
        # Aggregate by custom age group (First sum cases and population, then calculate rate)
        df_prop_agg = df_prop.group_by(["gender", "custom_age_group", "cancer_type"]).agg([
            pl.col("cases").sum().alias("cases_sum"),
            pl.col("population").sum().alias("pop_sum")
        ]).with_columns(
            ((pl.col("cases_sum") / pl.col("pop_sum")) * 100000).round(2).alias("custom_incidence_rate")
        )
        
        # Calculate proportion (%) based on custom incidence rates within each (gender, custom_age_group)
        df_prop_agg = df_prop_agg.with_columns(
            (pl.col("custom_incidence_rate") / pl.col("custom_incidence_rate").sum().over(["gender", "custom_age_group"]) * 100).round(1).alias("proportion")
        )
        
        custom_age_order = ["0-19세", "20-39세", "40-49세", "50-59세", "60세+"]
        
        from pyecharts.charts import Pie
        
        def create_stacked_bar_chart(df, gender_label):
            gender_df = df.filter(pl.col("gender") == gender_label)
            if gender_df.is_empty():
                return None
            
            # 1. Identify Top 5 per age group
            age_top5_map = {}
            all_top_cancers_union = set()
            
            for age in custom_age_order:
                age_subset = gender_df.filter(pl.col("custom_age_group") == age)
                if age_subset.is_empty():
                    age_top5_map[age] = []
                    continue
                
                # Get top 5 cancers for THIS age group
                top5 = (
                    age_subset.sort("proportion", descending=True)
                    .head(5)["cancer_type"].to_list()
                )
                age_top5_map[age] = top5
                for c in top5:
                    all_top_cancers_union.add(c)
            
            # Sort the union set by the MAX proportion it achieves in any age group
            # This helps ensure that the 'most dominant' cancer in any group is likely to be at the bottom
            cancer_ranks = []
            for c in all_top_cancers_union:
                max_prop = gender_df.filter(pl.col("cancer_type") == c)["proportion"].max()
                cancer_ranks.append({"name": c, "max_prop": max_prop or 0})
            
            union_list = [item["name"] for item in sorted(cancer_ranks, key=lambda x: x["max_prop"], reverse=True)]
            
            # 2. Prepare data for each series
            series_data = {c: [0.0] * len(custom_age_order) for c in union_list}
            series_data["기타(Others)"] = [0.0] * len(custom_age_order)
            
            for idx, age in enumerate(custom_age_order):
                age_subset = gender_df.filter(pl.col("custom_age_group") == age)
                top5_for_this_age = age_top5_map.get(age, [])
                
                # Proportions for Top 5
                age_top5_data = age_subset.filter(pl.col("cancer_type").is_in(top5_for_this_age))
                age_top5_dict = dict(zip(age_top5_data["cancer_type"].to_list(), age_top5_data["proportion"].to_list()))
                
                # Update series_data for Top 5
                for c in top5_for_this_age:
                    series_data[c][idx] = float(age_top5_dict.get(c, 0))
                
                # Proportions for Others (all cancers NOT in Top 5 for this specific bar)
                age_others_data = age_subset.filter(~pl.col("cancer_type").is_in(top5_for_this_age))
                series_data["기타(Others)"][idx] = round(float(age_others_data["proportion"].sum()), 1)
            
            # 3. Build Chart
            # Stack order: Largest in union on bottom, Others on top
            # add_yaxis calls are stacked bottom to top.
            bar = Bar(init_opts=opts.InitOpts(width="100%", height="550px"))
            bar.add_xaxis(custom_age_order)
            
            for s_name in union_list:
                # Filter for this series
                y_vals = series_data[s_name]
                # Replace 0 with None to hide labels in ECharts
                y_vals = [v if v > 0 else None for v in y_vals]
                
                # Format name for display (newline before KCD code)
                display_name = s_name.replace('(', '\n(') if '(' in s_name else s_name
                
                bar.add_yaxis(
                    display_name,
                    y_vals,
                    stack="stack1",
                    label_opts=opts.LabelOpts(
                        is_show=True, 
                        position="inside",
                        formatter="{a}",
                        font_size=10,
                        color="#fff"
                    ),
                    itemstyle_opts=opts.ItemStyleOpts(color=get_cancer_color(s_name))
                )
            
            # Finally add Others at the top
            y_vals_others = [v if v > 0 else None for v in series_data["기타(Others)"]]
            bar.add_yaxis(
                "기타\n(Others)",
                y_vals_others,
                stack="stack1",
                label_opts=opts.LabelOpts(
                    is_show=True, 
                    position="inside",
                    formatter="{a}",
                    font_size=10,
                    color="#fff"
                ),
                itemstyle_opts=opts.ItemStyleOpts(color="#d3d3d3")
            )
            
            bar.set_global_opts(
                title_opts=opts.TitleOpts(title=f"{prop_year}년 {gender_label} 연령별 암종 비중 (%)"),
                tooltip_opts=opts.TooltipOpts(trigger="item", formatter="{a}<br/>{b}: {c}%"),
                legend_opts=opts.LegendOpts(is_show=False),
                xaxis_opts=opts.AxisOpts(name="연령그룹"),
                yaxis_opts=opts.AxisOpts(name="비중 (%)", min_=0, max_=100)
            )
            return bar

        col_prop_m, col_prop_f = st.columns(2)
        with col_prop_m:
            chart_m = create_stacked_bar_chart(df_prop_agg, "남자")
            if chart_m:
                st_pyecharts(chart_m, height="600px", key=f"stack_m_{prop_year}")
            else:
                st.warning("데이터가 없습니다.")
                
        with col_prop_f:
            chart_f = create_stacked_bar_chart(df_prop_agg, "여자")
            if chart_f:
                st_pyecharts(chart_f, height="600px", key=f"stack_f_{prop_year}")
            else:
                st.warning("데이터가 없습니다.")
        
        st.info("💡 가장 비중이 큰 암종부터 아래에서 위로 쌓이며, 기타(Others) 항목은 항상 맨 위에 표시됩니다.")

        with st.expander("📝 연령별 암 발생 비중 상세 데이터 보기", expanded=False):
            # Create a pivot table for the user to see the actual proportions
            df_table = df_prop_agg.pivot(
                values="proportion", 
                index=["gender", "custom_age_group"], 
                on="cancer_type"
            ).sort(["gender", "custom_age_group"])
            
            # Format custom_age_group as categorical for correct sorting in the table
            gender_order = ["남자", "여자"]
            df_table = df_table.with_columns(
                pl.col("custom_age_group").cast(pl.Categorical)
            )
            
            st.dataframe(df_table.to_pandas(), use_container_width=True)

        st.markdown("<br>", unsafe_allow_html=True)

        # Bottom Data View (Collapsed by default)
        with st.expander("📊 상세 데이터 및 요약 통계 보기 (Detailed Data & Stats)", expanded=False):
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
