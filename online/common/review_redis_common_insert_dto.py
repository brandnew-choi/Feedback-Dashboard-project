from dataclasses import dataclass

@dataclass
class review_redis_common_insert_dto:
    channel_name: str
    original_id: str
    original_created_at: str
    original_content: str
    review_id: str
    reviewer_name: str
    rating: int
    review_content: str
    views: str
    like : str
    review_created_at: str
    inserted_at: str