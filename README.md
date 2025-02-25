# AWS Glue ETL Project

ETL pipeline for processing customer revenue data using AWS Glue and Apache Spark.

## Features

- Customer revenue analysis
- Glossy paper purchase tracking
- Data transformation from Aurora MySQL to Redshift
- Automated testing suite

## Setup

1. Clone the repository:
```bash
git clone https://github.com/your-username/glue-etl-project.git
cd glue-etl-project
```

2. Create and activate virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # Linux/Mac
# or
venv\Scripts\activate     # Windows
```

3. Install dependencies:
```bash
pip install -e ".[dev]"
```

## Running Tests

Run all tests:
```bash
pytest
```

Run with coverage:
```bash
pytest --cov=src/glue_etl tests/
```

## Project Structure

- `src/glue_etl/`: Source code
- `tests/`: Test suite
- `docs/`: Documentation

## Development

1. Format code:
```bash
black src/ tests/
```

2. Sort imports:
```bash
isort src/ tests/
```

3. Lint code:
```bash
flake8 src/ tests/
```
