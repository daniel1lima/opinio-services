import redis
from rq import Worker, Queue, Connection

# Define the Redis connection
redis_conn = redis.Redis()

# List of queues to listen to
listen = ["default"]

if __name__ == "__main__":
    with Connection(redis_conn):
        worker = Worker(map(Queue, listen))
        worker.work()
