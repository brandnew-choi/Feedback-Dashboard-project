from time import sleep
from datetime import datetime
import online.const as const
import online.common.review_redis_common_flush as review_redis_common_flush
import online.common.review_redis_common_insert as review_redis_common_insert
from online.common.review_redis_common_insert_dto import review_redis_common_insert_dto
from google_play_scraper import app, reviews, Sort

DB = 0

if __name__ == "__main__":
    
    print(f"start flush db={DB}")
    review_redis_common_flush.flush_db(DB)
    print(f"flush success db={DB}")
    
    token, total, out = None, 0, review_redis_common_insert_dto
            
    for _ in range(const.MAX_PAGES):
        items, token = reviews(const.MNT_APP_ID, 
                               lang="ko", 
                               country="kr",
                               sort=Sort.NEWEST, 
                               count=const.REVIEW_CNT,
                               continuation_token=token)
        if not items:
            break
      
        for item in items:
            #✅forDebug
            print(item, "\n")
            
            review_data = review_redis_common_insert_dto(
                channel_name        = "google_play",
                original_id         = "",
                original_created_at = "",
                original_content    = item.get("reviewCreatedVersion", ""),
                review_id         = item.get("reviewId", ""), 
                reviewer_name       = item.get("userName", ""),      
                rating              = int(item.get("score", 0)),          
                review_content      = (item.get("content") or "").strip(),
                views               = "",
                like                = item.get("thumbsUpCount", 0),   
                review_created_at   = item.get("at","").strftime("%Y%m%d%H%M%S"),
                inserted_at         = datetime.now().strftime("%Y%m%d%H%M%S%f")
            )
            
            # review_redis_common_insert 모듈로 insert
            review_redis_common_insert.insert_review(review_data, DB)
            
        if not token:  # 다음 페이지 없으면 종료
            break
        
        sleep(300/1000.0)
    