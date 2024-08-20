from connectors.factory import ConnectorFactory
from modules.logger_setup import setup_logger
from connectors.analyze import Analyzer

logger = setup_logger("worker_task.log")


def initial_onboarding(connector_config):
    connector = ConnectorFactory(connector_config).create_connector_instance()
    analyzer = Analyzer(connector)
    result = analyzer.initial_onboarding(connector_config.config, n_reviews=10)
    logger.info(f"Initial onboarding completed for {connector.__class__.__name__}")
    return result


def poll_new_reviews(connector_config):
    connector = ConnectorFactory(connector_config).create_connector_instance()
    analyzer = Analyzer(connector)
    result = analyzer.poll_new_reviews(
        connector_config.config, connector_config.last_sync
    )
    logger.info(f"Polled new reviews for {connector.__class__.__name__}")
    return result


def resume_fetch(connector_config):
    connector = ConnectorFactory(connector_config).create_connector_instance()
    analyzer = Analyzer(connector)
    result = analyzer.resume_fetch(connector_config.config)
    logger.info(f"Resumed fetch for {connector.__class__.__name__}")
    return result
