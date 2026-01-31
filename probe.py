import requests
import json

url1 = "https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=MDI3YTQ5OThmNzc2NGJlZjg4ZDVkMDIxMTEzNDBhYTA=&itmId=T10+&objL1=1+&objL2=1+2+&objL3=040+050+070+100+120+130+150+160+180+190+210+230+260+280+310+330+360+380+410+430+440+&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=2019&endPrdDe=2019&orgId=101&tblId=DT_1BPA001"
url2 = "https://kosis.kr/openapi/Param/statisticsParameterData.do?method=getList&apiKey=MDI3YTQ5OThmNzc2NGJlZjg4ZDVkMDIxMTEzNDBhYTA=&itmId=16117ac000101+&objL1=ALL&objL2=11101SSB21+11101SSB22+&objL3=15117AC001102+15117AC001103+15117AC001104+15117AC001105+15117AC001106+15117AC001107+15117AC001108+15117AC001109+15117AC001110+15117AC001111+15117AC001112+15117AC001113+15117AC001114+15117AC001115+15117AC001116+15117AC001117+15117AC001118+15117AC001119+15117AC001120+&objL4=&objL5=&objL6=&objL7=&objL8=&format=json&jsonVD=Y&prdSe=Y&startPrdDe=1999&endPrdDe=1999&orgId=117&tblId=DT_117N_A0024"

try:
    print("Fetching API 1...")
    r1 = requests.get(url1)
    d1 = r1.json()
    print(f"API 1 Keys: {list(d1[0].keys())}")
    print(f"API 1 Sample: {d1[0]}")
    
    print("\nFetching API 2...")
    r2 = requests.get(url2)
    d2 = r2.json()
    print(f"API 2 Keys: {list(d2[0].keys())}")
    print(f"API 2 Sample: {d2[0]}")
    
    with open('api_samples.json', 'w', encoding='utf-8') as f:
        json.dump({'api1': d1[:5], 'api2': d2[:5]}, f, ensure_ascii=False, indent=2)
except Exception as e:
    print(f"Error: {e}")
