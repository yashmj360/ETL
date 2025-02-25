from setuptools import setup, find_packages

setup(
    name="glue-etl",
    version="0.1.0",
    packages=find_packages(where="src"),
    package_dir={"": "src"},
    install_requires=[
        # Runtime dependencies
        "pyspark>=3.4.1",
    ],
    extras_require={
        "dev": [
            "pytest>=7.4.3",
            "pytest-mock>=3.12.0",
            "pytest-cov>=4.1.0",
            "black>=23.11.0",
            "flake8>=6.1.0",
            "isort>=5.12.0",
        ],
    },
    python_requires=">=3.8",
    author="Yash Vedant",
    author_email="yvedant18@outlook.com",
    description="AWS Glue ETL",
    keywords="aws,glue,etl,spark",
)
