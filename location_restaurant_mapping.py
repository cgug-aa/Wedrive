from pymongo import MongoClient
import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from scipy.spatial import cKDTree  # 공간 인덱스 라이브러리


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

# (1) timefiltered_2023 컬렉션 가져오기
timefiltered_data = list(db["timefiltered_2023"].find({}, {
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

print(f"Loaded {len(df_time)} rows from 'timefiltered_2023'")
print(f"Loaded {len(df_public)} rows from 'public_open_04'")

# 2. NaN 데이터 필터링
df_public = df_public[
    (df_public["좌표정보x(epsg4326)"].notna()) & (df_public["좌표정보x(epsg4326)"] != float('nan'))
]

# 3. 좌표를 기준으로 병합 (거리가 0.0001 이하인 것)
print("Performing local join...")

# 공차(거리) 설정
tolerance = 0.0001
public_coords = df_public[["좌표정보x(epsg4326)", "좌표정보y(epsg4326)"]].values
tree = cKDTree(public_coords)

# 5. df_time의 각 좌표에 대해 tolerance 내의 인덱스 찾기 (Chebyshev 거리, p=inf)
time_coords = df_time[["destination_lat", "destination_lng"]].values
indices_list = tree.query_ball_point(time_coords, r=tolerance, p=np.inf)

# 6. 결과 데이터 생성 (매칭되는 경우에만)
matched_data = []
for idx, indices in enumerate(indices_list):
    if indices:  # 하나 이상의 매칭이 있는 경우
        match = df_public.iloc[indices]
        row = df_time.iloc[idx]
        matched_data.append({
            "uuid": row["uuid"],
            "time_end": row["time_end"],
            "destination_lat": row["destination_lat"],
            "destination_lng": row["destination_lng"],
            "소재지전체주소": match["소재지전체주소"].tolist(),
            "도로명전체주소": match["도로명전체주소"].tolist(),
            "사업장명": match["사업장명"].tolist()
        })

df_result = pd.DataFrame(matched_data)
print(f"Matched {len(df_result)} rows")

# 7. 결과를 다시 MongoDB에 저장
print("Saving results to MongoDB...")
db["user_destination_restaurant_timefiltering"].insert_many(df_result.to_dict(orient="records"))
print("Data saved successfully!")