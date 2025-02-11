from pymongo import MongoClient
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv

load_dotenv()
# 데이터베이스 연결
load_dotenv()
client = MongoClient(os.getenv('DB_ADR'),
          username=os.getenv('DB_USER'),
          password=os.getenv('DB_PASSWORD'),
          authSource=os.getenv('DB_AuthSource'),
          authMechanism=os.getenv('DB_AuthMechanism'))

db = client.get_database(os.getenv('DB_Collection'))

# 🔹 1. MongoDB에서 필요한 데이터만 로컬로 가져오기
print("📥 Fetching data from MongoDB...")

# (1) raw_test_1 컬렉션 가져오기
timefiltered_data = list(db["raw_test_1"].find({}, {
    "_id": 0,
    "uuid": 1,
    "time_end": 1,
    "destination_lat": 1,
    "destination_lng": 1
}))
df_time = pd.DataFrame(timefiltered_data)

# (2) public_open_04 컬렉션 가져오기
public_open_data = list(db["public_open_04"].find({}, {
    "_id": 0,
    "좌표정보x(epsg4326)": 1,
    "좌표정보y(epsg4326)": 1,
    "소재지전체주소": 1,
    "도로명전체주소": 1,
    "사업장명": 1
}))
df_public = pd.DataFrame(public_open_data)

print(f"Loaded {len(df_time)} rows from 'raw_test_1'")
print(f"Loaded {len(df_public)} rows from 'public_open_04'")

# 2. NaN 데이터 필터링
df_public = df_public[
    (df_public["좌표정보x(epsg4326)"].notna()) & (df_public["좌표정보x(epsg4326)"] != float('nan'))
]

# 3. 좌표를 기준으로 병합 (거리가 0.0001 이하인 것)
print("Performing local join...")

# 공차(거리) 설정
tolerance = 0.0001

def find_data(lat, lng):
    mask = (np.abs(df_public["좌표정보x(epsg4326)"] - lat) < tolerance) & \
           (np.abs(df_public["좌표정보y(epsg4326)"] - lng) < tolerance)
    matches = df_public[mask]
    if not matches.empty:
        return {
            "소재지전체주소":matches["소재지전체주소"].tolist(),
            "도로명전체주소":matches["도로명전체주소"].tolist(),
            "사업장명":matches["사업장명"].tolist()
        }
    return None

# 병합 실행
matched_data = []
for _, row in df_time.iterrows():
    match = find_data(row["destination_lat"], row["destination_lng"])
    if match is not None:
        matched_data.append({
            "uuid": row["uuid"],
            "time_end": row["time_end"],
            "destination_lat": row["destination_lat"],
            "destination_lng": row["destination_lng"],
            "소재지전체주소": match["소재지전체주소"],
            "도로명전체주소": match["도로명전체주소"],
            "사업장명": match["사업장명"]
        })

# 결과 데이터프레임 변환
df_result = pd.DataFrame(matched_data)
print(f"Matched {len(df_result)} rows")

# 4. 결과를 다시 MongoDB에 저장
print("Saving results to MongoDB...")
db["user_destination_restaurant_timefiltering_20~23"].insert_many(df_result.to_dict(orient="records"))

print("Data saved successfully!")