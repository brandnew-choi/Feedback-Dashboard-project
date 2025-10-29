from datetime import datetime
import online.common.review_redis_common_insert as review_redis_common_insert
from online.common.review_redis_common_insert_dto import review_redis_common_insert_dto
from google_play_scraper import app, reviews, Sort
import online.const as const

DB = 0

def get_review() : 

    # 1) 앱 메타(제목, 업데이트 시각 등)
    meta = app(const.MNT_APP_ID, lang="ko", country="kr")

    # 2) 최신 리뷰 100개 가져오기 (+ 다음 페이지 토큰)
    items, _ = reviews(const.MNT_APP_ID,
                       lang="ko",
                       country="kr",
                       sort=Sort.NEWEST,
                       count=1)
    
    item = items[0]
        
    for item in items:
        #✅forDebug
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
        review_redis_common_insert.insert_review(review_data, DB)
    
    