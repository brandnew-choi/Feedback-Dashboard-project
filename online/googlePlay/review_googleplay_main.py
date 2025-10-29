import schedule
import time
import requests
import hashlib
import json
import review_googleplay_scrap
import online.common.review_redis_common_insert as review_redis_common_insert


URL = "https://play.google.com/store/apps/details?id=world.mnetplus"
API_ENDPOINT = "https://api.myserver.com/insert"

# 이전 응답의 해시 저장
prev_hash = None

def check_for_update():
    global prev_hash
    resp = requests.get(URL, stream=True)
    print("✅headers : ",resp.headers)
    print("✅content tyepe : ",type(resp.content))
    new_hash = hashlib.sha256(resp.content).hexdigest()

    # if new_hash != prev_hash:
    #     prev_hash = new_hash
    #     print("✅ New data detected, API called and stored.")
        
    #     # requests 또는 google_play_scraper를 이용해 새글을 data로 받아 dto에 mapping한다.
    #     review_redis_common_insert_dto = review_googleplay_scrap
        
    #     # data = resp.json()  # 새 데이터
    #     # requests.post(API_ENDPOINT, json=data)
        
    #     print("🚀---start insert :", review_redis_common_insert_dto)
        
    #     # 위에서 mapping한 dto를 redis에 insert한다.
    #     review_redis_common_insert.insert_review(review_redis_common_insert_dto)
    # else:
    #     print("No change detected.")

    
def main() :    
    print("start")
    schedule.every(10).seconds.do(check_for_update)

    while True:
        schedule.run_pending()
        time.sleep(1)
        
if __name__ == "__main__":
    main()
