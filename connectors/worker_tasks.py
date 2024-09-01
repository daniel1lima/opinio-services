from connectors.factory import ConnectorFactory
from connectors.publish import publish_job_status
from modules.logger_setup import setup_logger
from connectors.analyze import Analyzer
from models.models import CompanyModel, JobModel, JobStatus
import uuid
import redis
import json
import datetime

logger = setup_logger("worker_task.log")


def initial_onboarding(connector_config, company_id, user_id):
    job_id = str(uuid.uuid4())
    job = JobModel.create_job(job_id, company_id, connector_config.type)
    job_data = {
        "job_id": job.job_id,
        "company_id": job.company_id,
        "connector_type": job.connector_type,
        "status": job.status,
        "total_reviews_fetched": job.total_reviews_fetched,
        "last_sync": job.last_sync,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    publish_job_status(company_id, job_data)
    connector = ConnectorFactory(
        connector_config, company_id, job_id
    ).create_connector_instance()
    analyzer = Analyzer(connector)
    result = analyzer.initial_onboarding(
        connector_config.config, user_id, n_reviews=200
    )
    logger.info(f"Initial onboarding completed for {connector.__class__.__name__}")
    job = JobModel.get_most_recent_job(company_id)
    job_data = {
        "job_id": job.job_id,
        "company_id": job.company_id,
        "connector_type": job.connector_type,
        "status": job.status,
        "total_reviews_fetched": job.total_reviews_fetched,
        "last_sync": job.last_sync,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    publish_job_status(company_id, job_data)

    # Update last_sync date
    current_time = datetime.datetime.utcnow()  # Convert to string
    JobModel.update_last_sync(company_id, current_time)
    CompanyModel.update_connector_last_sync(
        company_id, connector_config.type, current_time
    )

    return result


def poll_new_reviews(connector_config, company_id, user_id):
    job_id = str(uuid.uuid4())
    JobModel.create_job(job_id, company_id, connector_config.type)
    job = JobModel.get_most_recent_job(company_id)
    job_data = {
        "job_id": job.job_id,
        "company_id": job.company_id,
        "connector_type": job.connector_type,
        "status": job.status,
        "total_reviews_fetched": job.total_reviews_fetched,
        "last_sync": job.last_sync,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    publish_job_status(company_id, job_data)
    connector = ConnectorFactory(
        connector_config, company_id, job_id
    ).create_connector_instance()
    analyzer = Analyzer(connector)
    result = analyzer.poll_new_reviews(
        connector_config.config, user_id, connector_config.last_sync
    )
    logger.info(f"Polled new reviews for {connector.__class__.__name__}")
    job = JobModel.get_most_recent_job(company_id)
    job_data = {
        "job_id": job.job_id,
        "company_id": job.company_id,
        "connector_type": job.connector_type,
        "status": job.status,
        "total_reviews_fetched": job.total_reviews_fetched,
        "last_sync": job.last_sync,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    publish_job_status(company_id, job_data)

    # Update last_sync date
    current_time = datetime.datetime.utcnow()  # Convert to string
    JobModel.update_last_sync(company_id, current_time)
    CompanyModel.update_connector_last_sync(
        company_id, connector_config.type, current_time
    )

    return result


def resume_fetch(connector_config, company_id, user_id):
    job_id = str(uuid.uuid4())
    JobModel.create_job(job_id, company_id, connector_config.type)
    job = JobModel.get_most_recent_job(company_id)
    job_data = {
        "job_id": job.job_id,
        "company_id": job.company_id,
        "connector_type": job.connector_type,
        "status": job.status,
        "total_reviews_fetched": job.total_reviews_fetched,
        "last_sync": job.last_sync,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    publish_job_status(company_id, job_data)
    connector = ConnectorFactory(
        connector_config, company_id, job_id
    ).create_connector_instance()
    analyzer = Analyzer(connector)
    result = analyzer.resume_fetch(connector_config.config, user_id)
    logger.info(f"Resumed fetch for {connector.__class__.__name__}")
    job = JobModel.get_most_recent_job(company_id)
    job_data = {
        "job_id": job.job_id,
        "company_id": job.company_id,
        "connector_type": job.connector_type,
        "status": job.status,
        "total_reviews_fetched": job.total_reviews_fetched,
        "last_sync": job.last_sync,
        "error_message": job.error_message,
        "created_at": job.created_at,
        "updated_at": job.updated_at,
    }
    publish_job_status(company_id, job_data)

    # Update last_sync date
    current_time = datetime.datetime.utcnow()  # Convert to string
    JobModel.update_last_sync(company_id, current_time)
    CompanyModel.update_connector_last_sync(
        company_id, connector_config.type, current_time
    )

    return result
