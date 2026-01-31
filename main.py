import polars as pl
import httpx
import asyncio
import json
import os
from urllib.parse import urlparse, parse_qs, urlencode, urlunparse
from dotenv import load_dotenv

# 환경 변수 로드
load_dotenv()
API_KEY = os.getenv("KOSIS_API_KEY")

if not API_KEY:
    print("[ERROR] KOSIS_API_KEY not found in environment variables or .env file.")

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
    
    print(f"Sending {len(tasks)} async requests...")
    responses = await asyncio.gather(*tasks)
    
    all_data = []
    for i, resp in enumerate(responses):
        if resp.status_code == 200:
            try:
                data = resp.json()
                if isinstance(data, list):
                    all_data.extend(data)
                else:
                    print(f"[WARNING] Batch {i} returned non-list data.")
            except Exception as e:
                print(f"[ERROR] Failed to parse JSON for batch {i}: {e}")
        else:
            print(f"[ERROR] Batch {i} failed with status {resp.status_code}")
    
    return all_data

def normalize_age(age_str):
    """연령대 명칭을 정규화합니다. 85세 이상 세분화 항목을 '85+'로 통합합니다."""
    if not age_str: return ""
    # 기본 정제
    cleaned = age_str.replace("세", "").replace(" ", "").replace("이상", "+")
    # 85세 이상 통합 처리 (85-89, 90-94, 95-99, 100+ -> 85+)
    if cleaned in ["85-89", "90-94", "95-99", "100+"]:
        return "85+"
    return cleaned

async def get_data_with_cache(cache_file, url_template, start_year, end_year, label):
    """캐시된 파일이 있으면 로드하고, 없으면 API 호출 후 저장합니다."""
    if os.path.exists(cache_file):
        print(f"Loading {label} from cache: {cache_file}")
        with open(cache_file, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    print(f"No cache found for {label}. Fetching from API...")
    async with httpx.AsyncClient() as client:
        data = await fetch_api_batch(client, url_template, start_year, end_year)
        
    if data:
        print(f"Saving {label} to cache: {cache_file}")
        with open(cache_file, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False)
            
    return data

async def main():
    # URL 정의
    url_pop = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={API_KEY}&itmId=T10+&objL1=1+&objL2=1+2+&objL3=040+050+070+100+120+130+150+160+180+190+210+230+260+280+310+330+340+360+380+410+430+440+&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=1999&endPrdDe=2023&orgId=101&tblId=DT_1BPA001"
    url_cancer = f"https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey={API_KEY}&itmId=16117ac000101+&objL1=ALL&objL2=11101SSB21+11101SSB22+&objL3=15117AC001102+15117AC001103+15117AC001104+15117AC001105+15117AC001106+15117AC001107+15117AC001108+15117AC001109+15117AC001110+15117AC001111+15117AC001112+15117AC001113+15117AC001114+15117AC001115+15117AC001116+15117AC001117+15117AC001118+15117AC001119+15117AC001120+&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=1999&endPrdDe=2023&orgId=117&tblId=DT_117N_A0024"

    # 1. 데이터 수집 (캐싱 적용)
    # URL이 변경되었으므로 캐시를 무시하고 새로 가져오도록 처리하거나 캐시 파일을 삭제해야 함
    # 여기서는 URL이 변경되었음을 명시적으로 알리기 위해 캐시 파일명을 변경하거나 삭제 권장
    if os.path.exists("raw_pop_cache.json"):
        pop_cached = await get_data_with_cache("raw_pop_cache.json", url_pop, 1999, 2023, "Population")
        # 340 코드가 있는지 확인 (없으면 다시 가져오기)
        if not any(d.get('C3') == '340' for d in pop_cached):
            print("[INFO] 340 code missing in cache. Refreshing population data...")
            os.remove("raw_pop_cache.json")
    
    pop_raw = await get_data_with_cache("raw_pop_cache.json", url_pop, 1999, 2023, "Population")
    cancer_raw = await get_data_with_cache("raw_cancer_cache.json", url_cancer, 1999, 2023, "Cancer")

    if not pop_raw or not cancer_raw:
        return

    # 2. Polars 데이터 로드 및 정제
    print("Processing data with Polars...")
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
    # 2000년의 80-84, 85+ 비율 계산
    pop_2000 = df_pop.filter(pl.col("year") == 2000)
    
    dist_2000 = pop_2000.filter(pl.col("age_group").is_in(["80-84", "85+"])).group_by(["gender"]).agg([
        pl.col("population").filter(pl.col("age_group") == "80-84").sum().alias("pop_80_84"),
        pl.col("population").filter(pl.col("age_group") == "85+").sum().alias("pop_85_up"),
        pl.col("population").sum().alias("total_80_plus")
    ]).with_columns([
        (pl.col("pop_80_84") / pl.col("total_80_plus")).alias("ratio_80_84"),
        (pl.col("pop_85_up") / pl.col("total_80_plus")).alias("ratio_85_up")
    ])

    # 1999년 80+ 데이터 가져오기
    pop_1999_80_plus = df_pop.filter((pl.col("year") == 1999) & (pl.col("age_group") == "80+"))
    
    if len(pop_1999_80_plus) > 0:
        print("Estimating 1999 population for 80-84 and 85+...")
        # 1999년 80-84, 85+ 행 생성
        estimated_1999 = pop_1999_80_plus.join(dist_2000.select(["gender", "ratio_80_84", "ratio_85_up"]), on="gender")
        
        estimated_80_84 = estimated_1999.with_columns([
            pl.lit("80-84").alias("age_group"),
            (pl.col("population") * pl.col("ratio_80_84")).alias("population")
        ]).select(["year", "gender", "age_group", "population"])
        
        estimated_85_up = estimated_1999.with_columns([
            pl.lit("85+").alias("age_group"),
            (pl.col("population") * pl.col("ratio_85_up")).alias("population")
        ]).select(["year", "gender", "age_group", "population"])
        
        # 기존 1999년 80+ 행 제거 및 추산된 행 추가
        df_pop = df_pop.filter(~((pl.col("year") == 1999) & (pl.col("age_group") == "80+")))
        df_pop = pl.concat([df_pop, estimated_80_84, estimated_85_up])

    # 85+ 통합을 위해 GroupBy Sum 수행 (이미 85+ 인 것들도 있으므로 보장)
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

    # 3. 조인
    joined = df_cancer.join(df_pop, on=["year", "gender", "age_group"], how="left")

    # 4. 미매칭 데이터 분석
    missing_mask = pl.col("population").is_null()
    missing_data = joined.filter(missing_mask).sort(["year", "gender", "age_group"])
    
    if len(missing_data) > 0:
        print(f"\n[WARNING] Found {len(missing_data)} records missing population data.")
        missing_data.write_csv("missing_joins.csv")
        print("Missing join details saved to 'missing_joins.csv'")
        
        # 원인 분석용 유니크 카테고리
        summary = missing_data.select(["year", "age_group"]).unique().sort(["year", "age_group"])
        print("\nSummary of missing age/year combinations:")
        print(summary)

    # 5. 최종 산출 (인구수가 있는 항목만)
    final_df = joined.filter(~missing_mask).with_columns(
        ((pl.col("cases") / pl.col("population")) * 100000).round(2).alias("incidence_rate")
    ).sort(["year", "gender", "age_group", "cancer_type"])

    print(f"\nFinal record count: {len(final_df)}")
    final_df.write_csv("cancer_incidence_final.csv")
    print("Final results saved to 'cancer_incidence_final.csv'")

if __name__ == "__main__":
    asyncio.run(main())
