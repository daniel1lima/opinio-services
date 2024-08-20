from connectors.factory import ConnectorFactory
from modules.logger_setup import setup_logger
from connectors.analyze import Analyzer


def start_fetch(connectors):
    for c in connectors:
        logger = setup_logger("worker_task.log")
        print(c)
        connector = ConnectorFactory(c)
        connector = (
            connector.create_connector_instance()
        )  # Initialize connector with factory

        # Store job_id in jobs list

        # q.enqueue(connector.fetch_new_reviews, job_id=job_id)
        analyze = Analyzer(connector)

        analyze.analyze_reviews(c.config, c.last_sync)

        logger.info(f"Enqueued {connector.__class__.__name__}")
