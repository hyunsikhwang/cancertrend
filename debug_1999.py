import httpx
import asyncio

async def fetch_1999_all_ages():
    url = "https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=MDI3YTQ5OThmNzc2NGJlZjg4ZDVkMDIxMTEzNDBhYTA=&itmId=T10+&objL1=1+&objL2=1+&objL3=ALL&format=json&jsonVD=Y&prdSe=Y&startPrdDe=1999&endPrdDe=1999&orgId=101&tblId=DT_1BPA001"
    async with httpx.AsyncClient() as client:
        resp = await client.get(url, timeout=30.0)
        data = resp.json()
        # 정제 전 명칭
        raw_ages = sorted(list(set(i['C3_NM'] for i in data)))
        print("Raw Age Names for 1999:")
        for a in raw_ages:
            print(f"- {a}")
            
        # 코드와 명칭 쌍
        code_map = sorted(list(set((i['C3'], i['C3_NM']) for i in data)))
        print("\nCode-Name pairs:")
        for c, n in code_map:
            print(f"{c}: {n}")

if __name__ == "__main__":
    asyncio.run(fetch_1999_all_ages())
