from time import sleep
from datetime import datetime
import online.common.review_redis_common_insert as review_redis_common_insert
from online.common.review_redis_common_insert_dto import review_redis_common_insert_dto
from google_play_scraper import app, reviews, Sort

APP_ID = "world.mnetplus"  # 앱 패키지명

if __name__ == "__main__":
#def get_newest_review():

    # 1) 앱 메타(제목, 업데이트 시각 등)
    meta = app(APP_ID, lang="ko", country="kr")

    # 2) 최신 리뷰 100개 가져오기 (+ 다음 페이지 토큰)
    items, _ = reviews(APP_ID,
                       lang="ko",
                       country="kr",
                       sort=Sort.NEWEST,
                       count=1)
    
    item = items[0]
    
    #필요한 항목 
    # reviewId : 785f0b33-a54c-4501-8691-a1f8fe8ecae6
    # userName : 도경
    # content : 애초에 업뎃도 잘안되고 업뎃이랑 깔았다 다시깔아도 잘 안되네요ㅠㅠㅠ 콘텐츠는 많지만 광고가 더 많습니다ㅠㅠㅠ
    # score : 1
    # reviewCreatedVersion : 3.29.1
    # at : datetime.datetime(2025, 10, 20, 21, 48, 55) <-이건 자르건 합치건 해야할듯...
    
    for item in items:
        print(item, "\n")
        
        review_data = review_redis_common_insert_dto(
            row_id              = datetime.now().strftime("%Y%m%d%H%M%S%f"),  
            channel_name        = "google_play",
            original_id         = "",
            original_created_at = "",
            original_content    = item.get("reviewCreatedVersion", ""),
            reviewer_id         = item.get("reviewId", ""), 
            reviewer_name       = item.get("userName", ""),      
            rating              = int(item.get("score", 0)),          
            review_content      = (item.get("content") or "").strip(),  
            review_created_at   = item.get("at","").strftime("%Y-%m-%dT%H:%M:%S"),
            inserted_at         = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        )
        
        # review_redis_common_insert 모듈로 insert
        review_redis_common_insert.insert_review(review_data)
    
    