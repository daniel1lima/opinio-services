from connectors.yelp import YelpConnector


class ConnectorFactory:
    def __init__(self, connector, company_id, job_id):
        self.connector = connector
        self.type = connector.type
        self.company_id = company_id
        self.job_id = job_id

        print(self.connector)
        print(self.type)

        # Initialize the connector class based on the type
        self.connector_instance = self.create_connector_instance()

    def create_connector_instance(self):
        # Assuming connectors are defined in a dictionary or module
        connector_classes = {"Yelp": YelpConnector}
        config = {
            "business_id": self.connector.config.business_id,
            "company_id": self.company_id,
            "job_id": self.job_id,
        }
        return connector_classes.get(self.type)(config)

    pass
