# Opinio - Business Review Analytics Platform

![Opinio Logo](./docs/Opinio_Logo_White.png)

## Overview

Opinio is a sophisticated Flask-based application that provides comprehensive business review analytics using advanced natural language processing and machine learning techniques. The platform helps businesses gather, analyze, and gain insights from customer reviews across multiple platforms.

## Key Features

- **Multi-Platform Review Collection**: Automated collection of reviews from various platforms
- **Sentiment Analysis**: Advanced sentiment and polarity analysis of customer reviews
- **Topic Categorization**: AI-powered categorization of review content
- **Real-time Processing**: Asynchronous job processing using Redis Queue
- **Dynamic Insights Generation**: AI-generated insights using Azure OpenAI
- **DynamoDB Integration**: Scalable data storage using Amazon DynamoDB

## Technology Stack

- **Backend**: Python/Flask
- **Database**: Amazon DynamoDB
- **Queue System**: Redis Queue
- **AI/ML**:
  - Azure OpenAI
  - NLTK
  - Scikit-learn
  - HDBSCAN
- **Infrastructure**: Docker
- **Task Runner**: Task (go-task)

## Prerequisites

- Python (version 3.11+)
- Poetry (version 1.x)
- Docker
- Redis
- go-task

## Installation

1. **Install Poetry**
```sh
curl -sSL https://install.python-poetry.org | python3 -
```

2. **Install go-task**
```sh
brew install go-task/tap/go-task
```

3. **Install Dependencies**
```sh
task install
```

## Environment Setup

Create a `.env` file in the root directory with the following variables:
```
DYNAMODB_URL=http://localhost:8000
AWS_REGION=us-east-2
AZURE_OPENAI_API_KEY=your_api_key
```

## Running the Application

1. **Start DynamoDB Local**
```sh
docker-compose up -d
```

2. **Start Redis Worker**
```python
import redis
from rq import Worker, Queue, Connection

redis_conn = redis.Redis()
listen = ["default"]

if __name__ == "__main__":
    with Connection(redis_conn):
        worker = Worker(map(Queue, listen))
        worker.work()
```

3. **Run the Application**
```sh
task run
```

## Development Commands

The project uses Taskfile for common development tasks:

```sh
task install    # Install dependencies
task run       # Run the application
task stop      # Stop the application
task test      # Run tests
task lint      # Run linter
task format    # Format code
```

## Project Structure

```
.
├── application.py        # Main Flask application
├── modules/             # Core functionality modules
├── connectors/          # Platform-specific connectors
├── models/             # Data models and database schemas
├── docker/             # Docker configuration
└── logs/              # Application logs
```

## Key Components

1. **Review Analysis Engine**: Advanced NLP pipeline for processing customer reviews
2. **DynamoDB Models**: Scalable data models for storing application data
3. **API Endpoints**: Comprehensive REST API for platform interaction

## Contributing

1. Fork the repository
2. Create a feature branch
3. Commit your changes
4. Push to the branch
5. Create a Pull Request

## License

This project is proprietary and confidential. All rights reserved.

## Documentation

For more detailed information:
- [Poetry Documentation](https://python-poetry.org/docs/)
- [Task Documentation](https://taskfile.dev/)

## Support

For support, please open an issue in the project repository or contact damorosolima@gmail.com

---
