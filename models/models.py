import datetime
import pynamodb
from pynamodb.models import Model
from pynamodb.attributes import (
    UnicodeAttribute,
    ListAttribute,
    MapAttribute,
    NumberAttribute,
)
import os
from dotenv import load_dotenv


load_dotenv()

from enum import Enum


class JobStatus(Enum):
    PENDING = "pending"
    IN_PROGRESS = "in_progress"
    COMPLETED = "completed"
    FAILED = "failed"


class JobModel(Model):
    class Meta:
        table_name = "Jobs"
        region = "us-west-2"
        host = "http://localhost:8000"

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
            status=JobStatus.PENDING.value,
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


class ConnectorModel(MapAttribute):
    type = UnicodeAttribute()
    config = MapAttribute()  # Use MapAttribute for nested objects
    last_sync = UnicodeAttribute(null=True)  # Allow null for no date by default


class UserModel(Model):
    class Meta:
        table_name = "Users"  # Ensure this is set in your .env
        region = "us-west-2"  # Change to your desired region
        host = "http://localhost:8000"  # Point to local DynamoDB

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


class ConnectionModel(Model):
    class Meta:
        table_name = "Connections"  # Ensure this is set in your .env
        region = "us-west-2"  # Change to your desired region
        host = "http://localhost:8000"  # Point to local DynamoDB

    user_id = UnicodeAttribute(hash_key=True)
    connection_id = UnicodeAttribute(range_key=True)
    config = UnicodeAttribute()
    last_sync = UnicodeAttribute(null=True)  # Allow null for no date by default


class CompanyModel(Model):
    class Meta:
        table_name = "Companies"  # Ensure this is set in your .env
        region = "us-west-2"  # Change to your desired region
        host = "http://localhost:8000"  # Point to local DynamoDB

    company_id = UnicodeAttribute(hash_key=True)
    company_name = UnicodeAttribute()  # New field for company name
    industry_id = UnicodeAttribute()  # New field for industry ID
    country = UnicodeAttribute()  # New field for country
    city = UnicodeAttribute()  # New field for city
    connectors = ListAttribute(of=ConnectorModel)  # New field for connectors

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
        # print(connector['type'])
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

    def remove_connector(self, connector_type):
        # Ensure connectors is initialized
        if self.connectors is None:
            return {
                "status": "error",
                "message": "No connectors to remove.",
            }  # Return error if no connectors

        # Find the connector to remove
        connector_to_remove = next(
            (c for c in self.connectors if c.type == connector_type), None
        )

        if connector_to_remove:
            self.connectors.remove(connector_to_remove)  # Remove the connector
            self.save()  # Save the updated company model
            return {
                "status": "success",
                "message": "Connector removed successfully.",
            }  # Return success status
        else:
            return {
                "status": "error",
                "message": "Connector not found.",
            }  # Return error status


class ReviewModel(Model):
    class Meta:
        table_name = "Reviews"  # Ensure this is set in your .env
        region = "us-west-2"  # Change to your desired region
        host = "http://localhost:8000"  # Point to local DynamoDB

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

    @classmethod
    def fetch_review_by_id(cls, review_id):
        try:
            return cls.get(review_id)
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


if __name__ == "__main__":
    # print(len(list(ReviewModel.fetch_all_reviews())))
    # print(list(CompanyModel.fetch_all_companies()))
    print(list(JobModel.fetch_all_jobs()))
    # JobModel.create_table(read_capacity_units=1, write_capacity_units=1)
    # JobModel.create_table(read_capacity_units=1, write_capacity_units=1)
    # print(ReviewModel.wipe_reviews())
    # JobModel.create_table(read_capacity_units=1, write_capacity_units=1)
    # print("JobModel table created successfully.")
