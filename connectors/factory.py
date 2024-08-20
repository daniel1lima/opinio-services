from connectors.yelp import YelpConnector


class ConnectorFactory:
    def __init__(self, connector):
        self.connector = connector
        self.type = connector.type

        print(self.connector)
        print(self.type)

        # Initialize the connector class based on the type
        self.connector_instance = self.create_connector_instance()

    def create_connector_instance(self):
        # Assuming connectors are defined in a dictionary or module
        connector_classes = {"Yelp": YelpConnector}
        return connector_classes.get(self.type)(self.connector.config)

    pass
