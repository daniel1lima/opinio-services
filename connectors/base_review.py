from pydantic import BaseModel


class ReviewEntry(BaseModel, extra="allow"):  # Allow extra fields
    business_id: str
    company_id: str
    review_id: str
    review_date: str = None
    review_text: str
    review_url: str = "No Url"
    rating: float
    total_reviews: int
    platform_id: str = "Yelp"
