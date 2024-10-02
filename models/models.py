import datetime
import json
import pynamodb
from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute,
    ListAttribute,
    MapAttribute,
    NumberAttribute,
    BooleanAttribute,
    JSONAttribute,
)
import os
from dotenv import load_dotenv
from enum import Enum

load_dotenv(override=True)


DYNAMODB_URL = os.getenv("DYNAMODB_URL", "http://localhost:8000")
AWS_REGION = os.getenv("AWS_REGION", "us-east-2")

print(DYNAMODB_URL)
print(AWS_REGION)


class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class JobModel(Model):
    class Meta:
        table_name = "Jobs"
        region = AWS_REGION
        host = DYNAMODB_URL

    job_id = UnicodeAttribute()
    company_id = UnicodeAttribute(hash_key=True)
    connector_type = UnicodeAttribute()
    status = UnicodeAttribute()
    total_reviews_fetched = NumberAttribute(default=0)
    last_sync = UnicodeAttribute(null=True)
    error_message = UnicodeAttribute(null=True)
    created_at = UnicodeAttribute()
    updated_at = UnicodeAttribute()

    @classmethod
    def create_job(cls, job_id, company_id, connector_type):
        now = datetime.datetime.now().isoformat()
        job = cls(
            job_id=job_id,
            company_id=company_id,
            connector_type=connector_type,
            status=JobStatus.IN_PROGRESS.value,
            created_at=now,
            updated_at=now,
        )
        job.save()
        return job

    def update_status(
        self, status, total_reviews_fetched=None, last_sync=None, error_message=None
    ):
        if isinstance(status, JobStatus):
            self.status = status.value
        else:
            self.status = status
        if total_reviews_fetched is not None:
            self.total_reviews_fetched = total_reviews_fetched
        if last_sync is not None:
            self.last_sync = last_sync
        if error_message is not None:
            self.error_message = error_message
        self.updated_at = datetime.datetime.now().isoformat()
        self.save()

    @classmethod
    def fetch_all_jobs(cls):
        return cls.scan()  # Fetch all companies from the table

    @classmethod
    def wipe_jobs(cls):
        """
        Deletes all jobs from the Jobs table.
        """
        try:
            for job in cls.scan():
                job.delete()
            return {
                "status": "success",
                "message": "All jobs have been wiped successfully.",
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to wipe jobs: {e}"}

    @classmethod
    def get_most_recent_job(cls, company_id):
        try:
            return next(iter(cls.query(company_id, scan_index_forward=False, limit=1)))
        except StopIteration:
            return None

    @classmethod
    def update_last_sync(cls, company_id, last_sync):
        job = cls.get_most_recent_job(company_id)
        if job:
            # Ensure last_sync is a string in ISO format
            job.last_sync = (
                last_sync.isoformat()
                if isinstance(last_sync, datetime.datetime)
                else last_sync
            )
            job.save()


class ConnectorModel(MapAttribute):
    type = UnicodeAttribute()
    config = MapAttribute()  # Use MapAttribute for nested objects
    last_sync = UnicodeAttribute(null=True)  # Allow null for no date by default


class UserModel(Model):
    class Meta:
        table_name = "Users"
        region = AWS_REGION
        host = DYNAMODB_URL

    user_id = UnicodeAttribute(hash_key=True)
    first_name = UnicodeAttribute()
    last_name = UnicodeAttribute()
    role = UnicodeAttribute()
    company_id = UnicodeAttribute()

    @classmethod
    def fetch_user(cls, user_id):
        try:
            return cls.get(user_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_user(cls, user_id, first_name, last_name, role, company_id, connections):
        user = cls(
            user_id=user_id,
            first_name=first_name,
            last_name=last_name,
            role=role,
            company_id=company_id,
            connections=connections,
        )
        user.save()
        return user

    @classmethod
    def get_all_users(cls):
        return cls.scan()  # Fetch all users from the table


class ConnectionModel(Model):
    class Meta:
        table_name = "Connections"
        region = AWS_REGION
        host = DYNAMODB_URL

    user_id = UnicodeAttribute(hash_key=True)
    connection_id = UnicodeAttribute(range_key=True)
    config = UnicodeAttribute()
    last_sync = UnicodeAttribute(null=True)  # Allow null for no date by default


class CompanyModel(Model):
    class Meta:
        table_name = "Companies"
        region = AWS_REGION
        host = DYNAMODB_URL

    company_id = UnicodeAttribute(hash_key=True)
    company_name = UnicodeAttribute()  # New field for company name
    industry_id = UnicodeAttribute()  # New field for industry ID
    country = UnicodeAttribute()  # New field for country
    city = UnicodeAttribute()  # New field for city
    connectors = ListAttribute(of=ConnectorModel)  # New field for connectors
    insights = JSONAttribute(null=True)  # Changed to JSONAttribute

    @classmethod
    def fetch_all_companies(cls):
        return cls.scan()  # Fetch all companies from the table

    @classmethod
    def get_company_by_id(cls, company_id):
        try:
            return cls.get(company_id)
        except cls.DoesNotExist:
            return None

    def add_connector(self, connector):
        # Ensure connectors is initialized
        if self.connectors is None:
            self.connectors = []  # Initialize as an empty list if None

        # Check if the connector already exists based on business_id and type
        if not any(c.type == connector["type"] for c in self.connectors):
            self.connectors.append(connector)  # Add the new connector
            self.save()  # Save the updated company model
            return {
                "status": "success",
                "message": "Connector added successfully.",
            }  # Return success status
        else:
            return {
                "status": "error",
                "message": "Connector already exists.",
            }  # Return error status

    def remove_connector(self, connector_type, user_id):
        if self.connectors is None:
            return {
                "status": "error",
                "message": "No connectors to remove.",
            }

        connector_to_remove = next(
            (c for c in self.connectors if c.type == connector_type), None
        )

        if connector_to_remove:
            self.connectors.remove(connector_to_remove)
            self.save()

            # Remove associated reviews and inbox items
            ReviewModel.remove_reviews_by_company_and_platform(
                self.company_id, connector_type
            )
            InboxModel.remove_inbox_items_by_company_and_platform(
                user_id, connector_type
            )

            return {
                "status": "success",
                "message": f"Connector, associated reviews, and inbox items for {connector_type} removed successfully.",
            }
        else:
            return {
                "status": "error",
                "message": "Connector not found.",
            }

    @classmethod
    def update_insights(cls, company_id, insights):
        company = cls.get_company_by_id(company_id)
        if company:
            company.insights = insights
            company.save()
            return {
                "status": "success",
                "message": "Insights updated successfully.",
            }
        else:
            return {
                "status": "error",
                "message": "Company not found.",
            }

    @classmethod
    def get_insights(cls, company_id):
        company = cls.get_company_by_id(company_id)
        if company:
            return company.insights
        else:
            return None

    @classmethod
    def update_connector_last_sync(cls, company_id, connector_type, last_sync):
        company = cls.get_company_by_id(company_id)
        last_sync = last_sync.isoformat()
        if company:
            for connector in company.connectors:
                if connector.type == connector_type:
                    connector.last_sync = last_sync
                    company.save()
                    break

    @classmethod
    def migrate_insights_to_json(cls):
        """
        Migrate existing insights to JSONAttribute format.
        """
        for company in cls.scan():
            try:
                # If insights is already a dict or list, it's likely already in the correct format
                if isinstance(company.insights, (dict, list)):
                    continue

                # If insights is a string, try to parse it as JSON
                if isinstance(company.insights, str):
                    try:
                        parsed_insights = json.loads(company.insights)
                    except json.JSONDecodeError:
                        # If it's not valid JSON, wrap it in a list
                        parsed_insights = [company.insights]
                else:
                    # For any other type, wrap it in a list
                    parsed_insights = (
                        [company.insights] if company.insights is not None else []
                    )

                # Update the insights attribute
                company.insights = {"insights": parsed_insights}
                company.save()
            except Exception as e:
                print(
                    f"Error migrating insights for company {company.company_id}: {str(e)}"
                )

        return {"status": "success", "message": "Insights migration completed."}


class ReviewModel(Model):
    class Meta:
        table_name = "Reviews"
        region = AWS_REGION
        host = DYNAMODB_URL
        read_capacity_units = 10  # Increased read capacity
        write_capacity_units = 10  # Increased write capacity

    review_id = UnicodeAttribute(range_key=True)
    business_id = UnicodeAttribute()
    company_id = UnicodeAttribute(hash_key=True)
    review_date = UnicodeAttribute()  # Store as string in ISO format
    review_text = UnicodeAttribute()
    review_url = UnicodeAttribute(default="No Url")
    rating = UnicodeAttribute()  # Store as string to accommodate float
    total_reviews = UnicodeAttribute()  # Store as string to accommodate int
    platform_id = UnicodeAttribute(default="Yelp")
    assigned_label = ListAttribute(
        of=UnicodeAttribute
    )  # Assuming this is a list of strings
    named_labels = ListAttribute(
        of=UnicodeAttribute
    )  # Assuming this is a list of strings
    sentiment = NumberAttribute(null=True)  # Allow null for no sentiment
    polarity = NumberAttribute(null=True)  # Allow null for no polarity
    author_name = UnicodeAttribute(default="Anonymous")
    author_image_url = UnicodeAttribute(default="No Url")
    ai_response = UnicodeAttribute(null=True)

    @classmethod
    def fetch_review_by_comp_id_review_id(cls, company_id, review_id):
        try:
            return cls.get(company_id, review_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def create_review(cls, review_data):
        review = cls(**review_data)  # Unpack the review_data dictionary
        review.save()
        return review

    @classmethod
    def fetch_all_reviews(cls):
        return cls.scan()  # Fetch all companies from the table

    @classmethod
    def wipe_reviews(cls):
        """
        Deletes all reviews from the Reviews table.
        """
        try:
            for review in cls.scan():
                review.delete()
            return {
                "status": "success",
                "message": "All reviews have been wiped successfully.",
            }
        except Exception as e:
            return {"status": "error", "message": f"Failed to wipe reviews: {e}"}

    @classmethod
    def recreate_table(cls):
        if cls.exists():
            cls.delete_table()
        cls.create_table(read_capacity_units=10, write_capacity_units=10)

    @classmethod
    def fetch_reviews_by_company_id(cls, company_id):
        try:
            return cls.query(company_id)
        except Exception as e:
            return {"status": "error", "message": f"Failed to fetch reviews: {e}"}

    @classmethod
    def update_review_urls(cls):
        reviews = cls.fetch_all_reviews()
        for review in reviews:
            new_url = f"https://www.yelp.com/biz/{review.business_id}?hrid={review.review_id}&utm_campaign=www_review_share_popup&utm_medium=copy_link&utm_source=(direct)"
            review.update(actions=[ReviewModel.review_url.set(new_url)])
        return {"status": "success", "message": "Review URLs updated successfully."}

    @classmethod
    def update_review_ids(cls):
        reviews = cls.fetch_all_reviews()
        for review in reviews:
            if review.review_id.startswith("google_"):
                new_review_id = review.review_id[len("google_") :]
                review.update(actions=[ReviewModel.review_id.set(new_review_id)])
        return {"status": "success", "message": "Review IDs updated successfully."}

    @classmethod
    def remove_reviews_by_company_and_platform(cls, company_id, platform_id):
        try:
            reviews_to_delete = cls.query(
                hash_key=company_id,
                filter_condition=ReviewModel.platform_id == platform_id,
            )

            for review in reviews_to_delete:
                review.delete()

            return {
                "status": "success",
                "message": f"All reviews for company {company_id} and platform {platform_id} have been removed.",
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to remove reviews: {str(e)}",
            }


class InboxModel(Model):
    class Meta:
        table_name = "Inbox"
        region = AWS_REGION
        host = DYNAMODB_URL

    user_id = UnicodeAttribute(hash_key=True)
    review_id = UnicodeAttribute(range_key=True)
    created_at = UnicodeAttribute()
    is_read = BooleanAttribute(default=False)
    is_starred = BooleanAttribute(default=False)
    labels = ListAttribute(of=UnicodeAttribute)
    folder_id = UnicodeAttribute(default="None")
    company_id = UnicodeAttribute()
    review_date = UnicodeAttribute()  # Store as string in ISO format
    review_text = UnicodeAttribute()
    review_url = UnicodeAttribute(default="No Url")
    rating = UnicodeAttribute()  # Store as string to accommodate float
    total_reviews = UnicodeAttribute()  # Store as string to accommodate int
    platform_id = UnicodeAttribute(default="Yelp")
    assigned_label = ListAttribute(
        of=UnicodeAttribute
    )  # Assuming this is a list of strings
    named_labels = ListAttribute(
        of=UnicodeAttribute
    )  # Assuming this is a list of strings
    author_name = UnicodeAttribute(default="Anonymous")
    author_image_url = UnicodeAttribute(default="No Url")
    ai_response = UnicodeAttribute(null=True)

    @classmethod
    def create_inbox_item(cls, user_id, review):
        inbox_item = cls(
            user_id=user_id,
            review_id=review.review_id,
            created_at=datetime.datetime.now().isoformat(),
            is_read=False,
            is_starred=False,
            labels=review.labels if hasattr(review, "labels") else [],
            company_id=review.company_id,
            review_date=review.review_date,
            review_text=review.review_text,
            review_url=review.review_url if hasattr(review, "review_url") else "No Url",
            rating=review.rating,
            total_reviews=review.total_reviews,
            platform_id=review.platform_id
            if hasattr(review, "platform_id")
            else "Yelp",
            assigned_label=review.assigned_label
            if hasattr(review, "assigned_label")
            else [],
            named_labels=review.named_labels if hasattr(review, "named_labels") else [],
            author_name=review.author_name
            if hasattr(review, "author_name")
            else "Anonymous",
            author_image_url=review.author_image_url
            if hasattr(review, "author_image_url")
            else "No Url",
            ai_response=review.ai_response if hasattr(review, "ai_response") else None,
        )
        inbox_item.save()
        return inbox_item

    @classmethod
    def fetch_inbox_item_by_id(cls, inbox_id):
        try:
            return cls.get(inbox_id)
        except cls.DoesNotExist:
            return None

    @classmethod
    def delete_inbox_item(cls, inbox_id):
        try:
            inbox_item = cls.get(inbox_id)
            inbox_item.delete()
            return {"status": "success", "message": "Inbox item deleted successfully."}
        except cls.DoesNotExist:
            return {"status": "error", "message": "Inbox item not found."}

    @classmethod
    def fetch_inbox_items_by_user_id(cls, user_id):
        try:
            return cls.query(user_id)
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @classmethod
    def wipe_inbox_items(cls):
        try:
            for item in cls.scan():
                item.delete()
            return {
                "status": "success",
                "message": "All inbox items wiped successfully.",
            }
        except Exception as e:
            return {"status": "error", "message": str(e)}

    @classmethod
    def ensure_table_exists(cls):
        if not cls.exists():
            cls.create_table(read_capacity_units=10, write_capacity_units=10)

    @classmethod
    def fetch_inbox_item_by_user_id_and_review_id(cls, user_id, review_id):
        try:
            return cls.get(user_id, review_id)  # Fetch the inbox item using both keys
        except cls.DoesNotExist:
            return None  # Return None if the item does not exist

    @classmethod
    def remove_inbox_items_by_company_and_platform(cls, user_id, platform_id):
        try:
            # Query all inbox items for the given company_id
            items_to_delete = cls.query(
                user_id, filter_condition=(InboxModel.platform_id == platform_id)
            )

            # Delete each matching item
            delete_count = 0
            for item in items_to_delete:
                item.delete()
                delete_count += 1

            return {
                "status": "success",
                "message": f"Removed {delete_count} inbox items for user {user_id} and platform {platform_id}.",
            }
        except Exception as e:
            return {
                "status": "error",
                "message": f"Failed to remove inbox items: {str(e)}",
            }


class InboxEditorModel(Model):
    class Meta:
        table_name = "InboxEditor"
        region = AWS_REGION
        host = DYNAMODB_URL

    user_id = UnicodeAttribute(hash_key=True)
    review_id = UnicodeAttribute(range_key=True)
    content = UnicodeAttribute()
    updated_at = UnicodeAttribute()

    @classmethod
    def save_editor_content(cls, user_id, review_id, content):
        now = datetime.datetime.now().isoformat()
        editor_item = cls(
            user_id=user_id,
            review_id=review_id,
            content=json.dumps(content),
            updated_at=now,
        )
        editor_item.save()
        return editor_item

    @classmethod
    def get_editor_content(cls, user_id, review_id):
        try:
            item = cls.get(user_id, review_id)
            item.content = json.loads(item.content)
            return item
        except cls.DoesNotExist:
            return None

    @classmethod
    def update_editor_content(cls, user_id, review_id, content):
        try:
            item = cls.get(user_id, review_id)
            item.content = json.dumps(content)
            item.updated_at = datetime.datetime.now().isoformat()
            item.save()
            return item
        except cls.DoesNotExist:
            return None

    @classmethod
    def delete_editor_content(cls, user_id, review_id):
        try:
            item = cls.get(user_id, review_id)
            item.delete()
            return True
        except cls.DoesNotExist:
            return False

    @classmethod
    def get_all_editor_content_for_user(cls, user_id):
        return cls.query(user_id)


def export_reviews():
    """
    Export all reviews from the ReviewModel table.
    """
    all_reviews = []
    for review in ReviewModel.scan():
        review_data = {
            "review_id": review.review_id,
            "business_id": review.business_id,
            "company_id": review.company_id,
            "review_date": review.review_date,
            "review_text": review.review_text,
            "review_url": review.review_url,
            "rating": review.rating,
            "total_reviews": review.total_reviews,
            "platform_id": review.platform_id,
            "assigned_label": review.assigned_label,
            "named_labels": review.named_labels,
            "sentiment": review.sentiment,
            "polarity": review.polarity,
            "author_name": review.author_name,
            "author_image_url": review.author_image_url,
            "ai_response": review.ai_response,
        }
        all_reviews.append(review_data)

    return all_reviews


def save_reviews_to_file(reviews, filename="exported_reviews.json"):
    """
    Save the exported reviews to a JSON file.
    """
    with open(filename, "w") as f:
        json.dump(reviews, f, indent=2)
    print(f"Reviews exported to {filename}")


if __name__ == "__main__":
    # Migrate insights to JSON format
    # Delete the Companies table and recreate it

    exported_reviews = export_reviews()
    save_reviews_to_file(exported_reviews)
