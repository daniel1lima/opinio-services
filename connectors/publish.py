import json
import redis

redis_conn = redis.Redis()


def publish_job_status(company_id, status):
    """
    Publish job status updates to a Redis pubsub channel.

    :param company_id: The ID of the company associated with the job
    :param status: A dictionary containing the job status information
    """
    channel = f"job_status:{company_id}"
    message = json.dumps(status)
    redis_conn.publish(channel, message)
