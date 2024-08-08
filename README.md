# Project Name

## Overview
This project is a Python application managed with Poetry. The `Taskfile.yml` is used to simplify common development tasks.

## Prerequisites
- Python (version 3.x)
- Poetry (version 1.x)

## Setup

### 1. Install Poetry
If you don't have Poetry installed, you can install it using the following command:
```sh
curl -sSL https://install.python-poetry.org | python3 -
```

brew install go-task/tap/go-task

## Taskfile Commands
The `Taskfile.yml` defines several tasks to help with development:

### Install Dependencies
To install the project dependencies:
```sh
task install
```

### Run the Application
To run the Flask application:
```sh
task run
```

### Run Tests
To run the tests:
```sh
task test
```

### Lint the Code
To run the linter:
```sh
task lint
```

### Format the Code
To format the code:
```sh
task format
```

## Additional Information
For more information on how to use Poetry, visit the [Poetry documentation](https://python-poetry.org/docs/).

For more information on Taskfile, visit the [Task documentation](https://taskfile.dev/).
