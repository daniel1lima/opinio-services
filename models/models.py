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


if __name__ == "__main__":
    print(list(ReviewModel.fetch_all_reviews()))
