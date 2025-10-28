import redis
import online.common.review_redis_common_insert_dto as review_redis_common_insert_dto

def init_redis(host, port, db):
   client = redis.StrictRedis(host=host, port=port, db=db, decode_responses=True)
   return client

def insert_review(review_data: review_redis_common_insert_dto):
    """
    ReviewData 객체를 받아 Redis에 저장하는 메서드
    """
    client = init_redis(host='localhost', port=6379, db=0)
    
    # 리뷰 내용 1000자 제한
    review_data.original_content = (review_data.original_content or "")[:1000]
    review_data.review_content = (review_data.review_content or "")[:1000]

    #print(review_data)

    # Redis key 생성
    key = f"review:{review_data.channel_name}:{review_data.review_created_at}"
   
    # JSON mapping
    result = client.hmset(key, mapping = {
        "channel_name": review_data.channel_name,
        "original_id": review_data.original_id,
        "original_created_at": review_data.original_created_at,
        "original_content": review_data.original_content,
        "review_id": review_data.review_id,
        "reviewer_name": review_data.reviewer_name,
        "rating": review_data.rating,
        "review_content": review_data.review_content,
        "views": review_data.views,
        "like": review_data.like,
        "review_created_at": review_data.review_created_at,
        "inserted_at": review_data.inserted_at,
    })
    
    print("저장 결과:", result)
    return key