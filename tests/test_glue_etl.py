import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import pandas as pd

# Import the module to test (assuming it's named glue_etl.py)
from glue_etl import (
    read_source_table,
    write_to_redshift,
    create_customer_revenue_report,
    create_glossy_customers_report,
    main
)

@pytest.fixture
def mock_glue_context():
    """Create a mock GlueContext with necessary methods."""
    mock_context = Mock()
    mock_context.create_dynamic_frame.from_options = Mock()
    mock_context.write_dynamic_frame.from_jdbc_conf = Mock()
    return mock_context

@pytest.fixture
def mock_spark_session():
    """Create a real SparkSession for testing."""
    return SparkSession.builder \
        .appName("unit-test") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def sample_orders_df(mock_spark_session):
    """Create a sample orders DataFrame for testing."""
    data = [
        (1, 1, 1000.0, 200.0),
        (2, 1, 2000.0, 400.0),
        (3, 2, 1500.0, 300.0)
    ]
    schema = StructType([
        StructField("id", LongType()),
        StructField("account_id", LongType()),
        StructField("total_amt_usd", DoubleType()),
        StructField("gloss_amt_usd", DoubleType())
    ])
    return mock_spark_session.createDataFrame(data, schema)

@pytest.fixture
def sample_accounts_df(mock_spark_session):
    """Create a sample accounts DataFrame for testing."""
    data = [
        (1, "Account A"),
        (2, "Account B")
    ]
    schema = StructType([
        StructField("id", LongType()),
        StructField("name", StringType())
    ])
    return mock_spark_session.createDataFrame(data, schema)

@pytest.fixture
def mock_dynamic_frame():
    """Create a mock DynamicFrame."""
    mock_df = Mock()
    mock_df.toDF = Mock()
    return mock_df

def test_read_source_table(mock_glue_context):
    """Test reading a source table from MySQL."""
    table_name = "test_table"
    read_source_table(mock_glue_context, table_name)
    
    mock_glue_context.create_dynamic_frame.from_options.assert_called_once_with(
        connection_type="mysql",
        connection_options={
            "useConnectionProperties": True,
            "dbtable": table_name,
            "connectionName": "Aurora connection"
        }
    )

def test_write_to_redshift(mock_glue_context, mock_dynamic_frame):
    """Test writing to Redshift."""
    table_name = "test_table"
    temp_dir = "/tmp"
    
    write_to_redshift(mock_glue_context, mock_dynamic_frame, table_name, temp_dir)
    
    mock_glue_context.write_dynamic_frame.from_jdbc_conf.assert_called_once_with(
        frame=mock_dynamic_frame,
        catalog_connection="Redshift connection",
        connection_options={
            "dbtable": table_name,
            "database": "dev",
            "preactions": f"DROP TABLE IF EXISTS {table_name}",
            "redshiftTmpDir": temp_dir
        }
    )

def test_create_customer_revenue_report(mock_glue_context, 
                                      sample_orders_df, 
                                      sample_accounts_df):
    """Test customer revenue report creation."""
    # Create mock DynamicFrames
    mock_orders_df = Mock()
    mock_orders_df.toDF.return_value = sample_orders_df
    
    mock_accounts_df = Mock()
    mock_accounts_df.toDF.return_value = sample_accounts_df
    
    # Execute the function
    result = create_customer_revenue_report(
        mock_glue_context, mock_orders_df, mock_accounts_df)
    
    # Convert result to pandas for easier assertion
    result_df = result.toDF().toPandas()
    
    # Verify the results
    assert len(result_df) == 2  # Should have 2 accounts
    assert result_df.iloc[0]['total_revenue'] == 3000.0  # Account A total
    assert result_df.iloc[1]['total_revenue'] == 1500.0  # Account B total

def test_create_glossy_customers_report(mock_glue_context, 
                                      sample_orders_df, 
                                      sample_accounts_df):
    """Test glossy customers report creation."""
    # Create mock DynamicFrames
    mock_orders_df = Mock()
    mock_orders_df.toDF.return_value = sample_orders_df
    
    mock_accounts_df = Mock()
    mock_accounts_df.toDF.return_value = sample_accounts_df
    
    # Execute the function
    result = create_glossy_customers_report(
        mock_glue_context, mock_orders_df, mock_accounts_df)
    
    # Convert result to pandas for easier assertion
    result_df = result.toDF().toPandas()
    
    # Verify the results
    assert len(result_df) == 2  # Should have 2 accounts
    assert result_df.iloc[0]['glossy_spending'] == 600.0  # Account A total
    assert result_df.iloc[1]['glossy_spending'] == 300.0  # Account B total

@patch('sys.argv', ['script.py', '--JOB_NAME', 'test_job', '--TempDir', '/tmp'])
@patch('glue_etl.SparkContext')
@patch('glue_etl.GlueContext')
@patch('glue_etl.Job')
def test_main(mock_job_class, mock_glue_context_class, 
              mock_spark_context_class):
    """Test the main function execution."""
    # Setup mocks
    mock_job = Mock()
    mock_job_class.return_value = mock_job
    
    mock_glue_context = Mock()
    mock_glue_context_class.return_value = mock_glue_context
    
    # Execute main function
    main()
    
    # Verify job initialization and commit
    mock_job.init.assert_called_once()
    mock_job.commit.assert_called_once()

if __name__ == '__main__':
    pytest.main(['-v'])import pytest
from unittest.mock import Mock, patch
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType
import pandas as pd

# Import the module to test (assuming it's named glue_etl.py)
from glue_etl import (
    read_source_table,
    write_to_redshift,
    create_customer_revenue_report,
    create_glossy_customers_report,
    main
)

@pytest.fixture
def mock_glue_context():
    """Create a mock GlueContext with necessary methods."""
    mock_context = Mock()
    mock_context.create_dynamic_frame.from_options = Mock()
    mock_context.write_dynamic_frame.from_jdbc_conf = Mock()
    return mock_context

@pytest.fixture
def mock_spark_session():
    """Create a real SparkSession for testing."""
    return SparkSession.builder \
        .appName("unit-test") \
        .master("local[1]") \
        .getOrCreate()

@pytest.fixture
def sample_orders_df(mock_spark_session):
    """Create a sample orders DataFrame for testing."""
    data = [
        (1, 1, 1000.0, 200.0),
        (2, 1, 2000.0, 400.0),
        (3, 2, 1500.0, 300.0)
    ]
    schema = StructType([
        StructField("id", LongType()),
        StructField("account_id", LongType()),
        StructField("total_amt_usd", DoubleType()),
        StructField("gloss_amt_usd", DoubleType())
    ])
    return mock_spark_session.createDataFrame(data, schema)

@pytest.fixture
def sample_accounts_df(mock_spark_session):
    """Create a sample accounts DataFrame for testing."""
    data = [
        (1, "Account A"),
        (2, "Account B")
    ]
    schema = StructType([
        StructField("id", LongType()),
        StructField("name", StringType())
    ])
    return mock_spark_session.createDataFrame(data, schema)

@pytest.fixture
def mock_dynamic_frame():
    """Create a mock DynamicFrame."""
    mock_df = Mock()
    mock_df.toDF = Mock()
    return mock_df

def test_read_source_table(mock_glue_context):
    """Test reading a source table from MySQL."""
    table_name = "test_table"
    read_source_table(mock_glue_context, table_name)
    
    mock_glue_context.create_dynamic_frame.from_options.assert_called_once_with(
        connection_type="mysql",
        connection_options={
            "useConnectionProperties": True,
            "dbtable": table_name,
            "connectionName": "Aurora connection"
        }
    )

def test_write_to_redshift(mock_glue_context, mock_dynamic_frame):
    """Test writing to Redshift."""
    table_name = "test_table"
    temp_dir = "/tmp"
    
    write_to_redshift(mock_glue_context, mock_dynamic_frame, table_name, temp_dir)
    
    mock_glue_context.write_dynamic_frame.from_jdbc_conf.assert_called_once_with(
        frame=mock_dynamic_frame,
        catalog_connection="Redshift connection",
        connection_options={
            "dbtable": table_name,
            "database": "dev",
            "preactions": f"DROP TABLE IF EXISTS {table_name}",
            "redshiftTmpDir": temp_dir
        }
    )

def test_create_customer_revenue_report(mock_glue_context, 
                                      sample_orders_df, 
                                      sample_accounts_df):
    """Test customer revenue report creation."""
    # Create mock DynamicFrames
    mock_orders_df = Mock()
    mock_orders_df.toDF.return_value = sample_orders_df
    
    mock_accounts_df = Mock()
    mock_accounts_df.toDF.return_value = sample_accounts_df
    
    # Execute the function
    result = create_customer_revenue_report(
        mock_glue_context, mock_orders_df, mock_accounts_df)
    
    # Convert result to pandas for easier assertion
    result_df = result.toDF().toPandas()
    
    # Verify the results
    assert len(result_df) == 2  # Should have 2 accounts
    assert result_df.iloc[0]['total_revenue'] == 3000.0  # Account A total
    assert result_df.iloc[1]['total_revenue'] == 1500.0  # Account B total

def test_create_glossy_customers_report(mock_glue_context, 
                                      sample_orders_df, 
                                      sample_accounts_df):
    """Test glossy customers report creation."""
    # Create mock DynamicFrames
    mock_orders_df = Mock()
    mock_orders_df.toDF.return_value = sample_orders_df
    
    mock_accounts_df = Mock()
    mock_accounts_df.toDF.return_value = sample_accounts_df
    
    # Execute the function
    result = create_glossy_customers_report(
        mock_glue_context, mock_orders_df, mock_accounts_df)
    
    # Convert result to pandas for easier assertion
    result_df = result.toDF().toPandas()
    
    # Verify the results
    assert len(result_df) == 2  # Should have 2 accounts
    assert result_df.iloc[0]['glossy_spending'] == 600.0  # Account A total
    assert result_df.iloc[1]['glossy_spending'] == 300.0  # Account B total

@patch('sys.argv', ['script.py', '--JOB_NAME', 'test_job', '--TempDir', '/tmp'])
@patch('glue_etl.SparkContext')
@patch('glue_etl.GlueContext')
@patch('glue_etl.Job')
def test_main(mock_job_class, mock_glue_context_class, 
              mock_spark_context_class):
    """Test the main function execution."""
    # Setup mocks
    mock_job = Mock()
    mock_job_class.return_value = mock_job
    
    mock_glue_context = Mock()
    mock_glue_context_class.return_value = mock_glue_context
    
    # Execute main function
    main()
    
    # Verify job initialization and commit
    mock_job.init.assert_called_once()
    mock_job.commit.assert_called_once()

if __name__ == '__main__':
    pytest.main(['-v'])
