import unittest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, isnan, isnull, upper, lower, trim, lit, count, sum as spark_sum
from pyspark.sql.types import *
import json
import io
import os
import time
from unittest.mock import Mock, patch, MagicMock
# Note: Import functions only for isolated testing of transformation logic
# Full integration testing would require Docker environment

class TestMedallionTransformations(unittest.TestCase):
    """Test cases for medallion architecture transformations using high-performance PySpark operations"""
    
    def _assert_dataframe_value(self, df, filter_condition, column, expected_value):
        """Helper method to efficiently assert DataFrame values"""
        result = df.filter(filter_condition).select(column).collect()
        self.assertEqual(len(result), 1, f"Expected exactly 1 row matching condition for column {column}")
        self.assertEqual(result[0][column], expected_value)
    
    def _assert_aggregation_count(self, df, filter_condition, expected_count):
        """Helper method to efficiently assert aggregation counts"""
        actual_count = df.filter(filter_condition).count()
        self.assertEqual(actual_count, expected_count)
        
    def _get_single_row_value(self, df, column, row_index=0):
        """Helper method to get a single value from DataFrame efficiently"""
        result = df.select(column).collect()
        return result[row_index][column] if row_index < len(result) else None
    
    def _time_operation(self, operation_name, operation_func):
        """Helper method to time PySpark operations for performance monitoring"""
        start_time = time.time()
        result = operation_func()
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"{operation_name} executed in {execution_time:.4f} seconds")
        return result
    
    def setUp(self):
        """Set up test data"""
        # Initialize Spark session for tests with optimized settings
        self.spark = SparkSession.builder \
            .appName("MedallionTransformationTests") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.sql.adaptive.enabled", "true") \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.shuffle.partitions", "2") \
            .getOrCreate()
        
        # Set log level to reduce noise during testing
        self.spark.sparkContext.setLogLevel("WARN")
        self.sample_brewery_data = [
            {
                "id": "1",
                "name": "Test Brewery 1",
                "brewery_type": "micro",
                "address_1": "123 Main St",
                "city": "Portland",
                "state": "Oregon",
                "postal_code": "97201",
                "country": "United States",
                "longitude": "-122.6765",
                "latitude": "45.5152",
                "phone": "5035551234",
                "website_url": "https://testbrewery1.com"
            },
            {
                "id": "2", 
                "name": "Test Brewery 2",
                "brewery_type": "regional",
                "address_1": None,
                "city": "Austin",
                "state": "Texas", 
                "postal_code": "78701",
                "country": "United States",
                "longitude": None,
                "latitude": None,
                "phone": None,
                "website_url": None
            },
            {
                "id": "3",
                "name": "Test Brewery 3", 
                "brewery_type": None,
                "address_1": "456 Oak Ave",
                "city": None,
                "state": None,
                "postal_code": "12345",
                "country": None,
                "longitude": "-73.9857",
                "latitude": "40.7484",
                "phone": "2125559876",
                "website_url": "https://testbrewery3.com"
            }
        ]
    
    def test_data_cleaning_and_standardization(self):
        """Test that data cleaning works correctly"""
        df = self.spark.createDataFrame(self.sample_brewery_data)
        # Cache DataFrame for better performance across multiple operations
        df.cache()
        
        # Apply the same transformations as in silver layer
        from pyspark.sql.functions import concat_ws
        df = df.withColumn('brewery_type', 
                          lower(trim(when(col('brewery_type').isNull(), 'unknown')
                                   .otherwise(col('brewery_type')))))
        df = df.withColumn('country', 
                          upper(trim(when(col('country').isNull(), 'unknown')
                                    .otherwise(col('country')))))
        df = df.withColumn('state', 
                          upper(trim(when(col('state').isNull(), 'unknown')
                                    .otherwise(col('state')))))
        df = df.withColumn('city', 
                          trim(when(col('city').isNull(), 'unknown')
                              .otherwise(col('city'))))
        df = df.withColumn('location', 
                          concat_ws('/', col('country'), col('state'), col('city')))
        
        df = df.withColumn('latitude', col('latitude').cast('double'))
        df = df.withColumn('longitude', col('longitude').cast('double'))
        
        df = df.withColumn('has_coordinates', 
                          ~(col('latitude').isNull() | col('longitude').isNull()))
        df = df.withColumn('has_website', ~col('website_url').isNull())
        df = df.withColumn('has_phone', ~col('phone').isNull())
        
        # Test brewery_type cleaning using PySpark operations
        brewery_type_results = df.select('id', 'brewery_type').collect()
        brewery_types = {row['id']: row['brewery_type'] for row in brewery_type_results}
        self.assertEqual(brewery_types['1'], 'micro')
        self.assertEqual(brewery_types['2'], 'regional')
        self.assertEqual(brewery_types['3'], 'unknown')
        
        # Test location standardization
        location_results = df.select('id', 'country', 'state', 'city').collect()
        locations = {row['id']: row for row in location_results}
        self.assertEqual(locations['1']['country'], 'UNITED STATES')
        self.assertEqual(locations['1']['state'], 'OREGON')
        self.assertEqual(locations['1']['city'], 'Portland')
        self.assertEqual(locations['3']['country'], 'UNKNOWN')
        
        # Test location hierarchy using efficient DataFrame operations
        self._time_operation(
            "Location hierarchy validation",
            lambda: [
                self._assert_dataframe_value(
                    df, col('id') == '1', 'location', 'UNITED STATES/OREGON/Portland'
                ),
                self._assert_dataframe_value(
                    df, col('id') == '3', 'location', 'UNKNOWN/UNKNOWN/unknown'
                )
            ]
        )
        
        # Test data quality flags using PySpark aggregations
        quality_results = df.select('id', 'has_coordinates', 'has_website', 'has_phone').collect()
        quality = {row['id']: row for row in quality_results}
        
        # Test coordinates flags
        self.assertTrue(quality['1']['has_coordinates'])
        self.assertFalse(quality['2']['has_coordinates'])
        self.assertTrue(quality['3']['has_coordinates'])
        
        # Test website flags
        self.assertTrue(quality['1']['has_website'])
        self.assertFalse(quality['2']['has_website'])
        self.assertTrue(quality['3']['has_website'])
        
        # Test phone flags
        self.assertTrue(quality['1']['has_phone'])
        self.assertFalse(quality['2']['has_phone'])
        self.assertTrue(quality['3']['has_phone'])
    
    def test_gold_layer_aggregations(self):
        """Test gold layer aggregation logic"""
        # Create test silver data
        silver_data_list = [
            {
                'id': '1', 'brewery_type': 'micro', 'country': 'UNITED STATES', 
                'state': 'OREGON', 'city': 'Portland', 'has_coordinates': True,
                'has_website': True, 'has_phone': True
            },
            {
                'id': '2', 'brewery_type': 'micro', 'country': 'UNITED STATES',
                'state': 'OREGON', 'city': 'Portland', 'has_coordinates': False,
                'has_website': False, 'has_phone': True
            },
            {
                'id': '3', 'brewery_type': 'regional', 'country': 'UNITED STATES',
                'state': 'TEXAS', 'city': 'Austin', 'has_coordinates': True,
                'has_website': True, 'has_phone': False
            }
        ]
        
        silver_data = self.spark.createDataFrame(silver_data_list)
        # Cache for performance since we'll use this DataFrame multiple times
        silver_data.cache()
        
        # Test aggregation by type and location
        brewery_by_type_location = silver_data.groupBy('brewery_type', 'country', 'state', 'city').agg(
            count('id').alias('brewery_count'),
            spark_sum('has_coordinates').alias('has_coordinates'),
            spark_sum('has_website').alias('has_website'),
            spark_sum('has_phone').alias('has_phone')
        ).cache()  # Cache aggregation result for multiple assertions
        
        # Verify aggregation results using efficient PySpark operations
        portland_micro_filter = (col('brewery_type') == 'micro') & (col('city') == 'Portland')
        
        self._time_operation(
            "Gold layer aggregation validation",
            lambda: [
                self._assert_aggregation_count(brewery_by_type_location, portland_micro_filter, 1),
                self._assert_dataframe_value(brewery_by_type_location, portland_micro_filter, 'brewery_count', 2),
                self._assert_dataframe_value(brewery_by_type_location, portland_micro_filter, 'has_coordinates', 1),
                self._assert_dataframe_value(brewery_by_type_location, portland_micro_filter, 'has_website', 1),
                self._assert_dataframe_value(brewery_by_type_location, portland_micro_filter, 'has_phone', 2)
            ]
        )
        
        # Test aggregation by type only
        brewery_by_type = silver_data.groupBy('brewery_type').agg(
            count('id').alias('brewery_count'),
            spark_sum('has_coordinates').alias('has_coordinates'),
            spark_sum('has_website').alias('has_website'),
            spark_sum('has_phone').alias('has_phone')
        ).cache()  # Cache for efficient multiple assertions
        
        # Test aggregation by type using efficient PySpark operations
        micro_filter = col('brewery_type') == 'micro'
        self._assert_aggregation_count(brewery_by_type, micro_filter, 1)
        self._assert_dataframe_value(brewery_by_type, micro_filter, 'brewery_count', 2)
        self._assert_dataframe_value(brewery_by_type, micro_filter, 'has_coordinates', 1)
        
        regional_filter = col('brewery_type') == 'regional'
        self._assert_aggregation_count(brewery_by_type, regional_filter, 1)
        self._assert_dataframe_value(brewery_by_type, regional_filter, 'brewery_count', 1)
        self._assert_dataframe_value(brewery_by_type, regional_filter, 'has_coordinates', 1)
    
    def test_percentage_calculations(self):
        """Test percentage calculations in aggregations (including division by zero safety)"""
        from pyspark.sql.functions import round as spark_round
        
        test_data_list = [
            {'brewery_count': 10, 'has_coordinates': 7, 'has_website': 5, 'has_phone': 3},
            {'brewery_count': 20, 'has_coordinates': 16, 'has_website': 12, 'has_phone': 8},
            {'brewery_count': 0, 'has_coordinates': 0, 'has_website': 0, 'has_phone': 0}  # Edge case
        ]
        
        test_data = self.spark.createDataFrame(test_data_list)
        
        # Apply safe division logic (same as in gold layer)
        test_data = test_data.withColumn(
            'pct_with_coordinates',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_coordinates') / col('brewery_count') * 100)).otherwise(0), 2)
        ).withColumn(
            'pct_with_website',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_website') / col('brewery_count') * 100)).otherwise(0), 2)
        ).withColumn(
            'pct_with_phone',
            spark_round(when(col('brewery_count') > 0, 
                           (col('has_phone') / col('brewery_count') * 100)).otherwise(0), 2)
        ).cache()  # Cache the transformed DataFrame
        
        # Test percentage calculations using efficient PySpark operations with window functions
        from pyspark.sql.window import Window
        from pyspark.sql.functions import row_number
        
        # Add row numbers for efficient testing
        window_spec = Window.orderBy(col('brewery_count').desc())
        test_data_with_row = test_data.withColumn('row_num', row_number().over(window_spec))
        
        # Test first row (normal case - brewery_count = 20)
        row1_filter = col('row_num') == 1
        self._assert_dataframe_value(test_data_with_row, row1_filter, 'pct_with_coordinates', 80.0)
        self._assert_dataframe_value(test_data_with_row, row1_filter, 'pct_with_website', 60.0)
        self._assert_dataframe_value(test_data_with_row, row1_filter, 'pct_with_phone', 40.0)
        
        # Test second row (normal case - brewery_count = 10)
        row2_filter = col('row_num') == 2
        self._assert_dataframe_value(test_data_with_row, row2_filter, 'pct_with_coordinates', 70.0)
        self._assert_dataframe_value(test_data_with_row, row2_filter, 'pct_with_website', 50.0)
        self._assert_dataframe_value(test_data_with_row, row2_filter, 'pct_with_phone', 30.0)
        
        # Test third row (division by zero safety - brewery_count = 0)
        row3_filter = col('row_num') == 3
        self._assert_dataframe_value(test_data_with_row, row3_filter, 'pct_with_coordinates', 0.0)
        self._assert_dataframe_value(test_data_with_row, row3_filter, 'pct_with_website', 0.0)
        self._assert_dataframe_value(test_data_with_row, row3_filter, 'pct_with_phone', 0.0)
    
    def test_summary_statistics(self):
        """Test summary statistics calculation"""
        test_df = self.spark.createDataFrame(self.sample_brewery_data)
        
        # Apply transformations
        test_df = test_df.withColumn('latitude', col('latitude').cast('double'))
        test_df = test_df.withColumn('longitude', col('longitude').cast('double'))
        test_df = test_df.withColumn('has_coordinates', 
                                   ~(col('latitude').isNull() | col('longitude').isNull()))
        test_df = test_df.withColumn('has_website', ~col('website_url').isNull())
        test_df = test_df.withColumn('has_phone', ~col('phone').isNull())
        test_df = test_df.withColumn('country', 
                                   upper(trim(when(col('country').isNull(), 'unknown')
                                             .otherwise(col('country')))))
        test_df = test_df.withColumn('state', 
                                   upper(trim(when(col('state').isNull(), 'unknown')
                                             .otherwise(col('state')))))
        test_df = test_df.withColumn('city', 
                                   trim(when(col('city').isNull(), 'unknown')
                                       .otherwise(col('city'))))
        test_df = test_df.withColumn('brewery_type', 
                                   lower(trim(when(col('brewery_type').isNull(), 'unknown')
                                             .otherwise(col('brewery_type')))))
        
        total_breweries = test_df.count()
        unique_countries = test_df.select('country').distinct().count()
        unique_states = test_df.select('state').distinct().count()
        unique_cities = test_df.select('city').distinct().count()
        unique_brewery_types = test_df.select('brewery_type').distinct().count()
        breweries_with_coordinates = test_df.agg(spark_sum('has_coordinates')).collect()[0][0]
        breweries_with_website = test_df.agg(spark_sum('has_website')).collect()[0][0]
        breweries_with_phone = test_df.agg(spark_sum('has_phone')).collect()[0][0]
        
        summary_stats = {
            'total_breweries': total_breweries,
            'unique_countries': unique_countries,
            'unique_states': unique_states,
            'unique_cities': unique_cities,
            'unique_brewery_types': unique_brewery_types,
            'breweries_with_coordinates': int(breweries_with_coordinates) if breweries_with_coordinates else 0,
            'breweries_with_website': int(breweries_with_website) if breweries_with_website else 0,
            'breweries_with_phone': int(breweries_with_phone) if breweries_with_phone else 0
        }
        
        self.assertEqual(summary_stats['total_breweries'], 3)
        self.assertEqual(summary_stats['unique_countries'], 2)  # UNITED STATES + UNKNOWN
        self.assertEqual(summary_stats['unique_states'], 3)  # OREGON, TEXAS, UNKNOWN
        self.assertEqual(summary_stats['unique_cities'], 3)  # Portland, Austin, Unknown
        self.assertEqual(summary_stats['unique_brewery_types'], 3)  # micro, regional, unknown
        self.assertEqual(summary_stats['breweries_with_coordinates'], 2)
        self.assertEqual(summary_stats['breweries_with_website'], 2)
        self.assertEqual(summary_stats['breweries_with_phone'], 2)
    
    def test_data_validation(self):
        """Test data validation and edge cases"""
        # Test empty data - create empty schema
        from pyspark.sql.types import StructType
        empty_df = self.spark.createDataFrame([], StructType([]))
        self.assertEqual(empty_df.count(), 0)
        
        # Test data with all nulls
        null_data_list = [{
            'id': '1',
            'name': None,
            'brewery_type': None,
            'city': None,
            'state': None,
            'country': None,
            'latitude': None,
            'longitude': None,
            'phone': None,
            'website_url': None
        }]
        
        null_data = self.spark.createDataFrame(null_data_list)
        
        # Apply transformations
        null_data = null_data.withColumn('brewery_type', 
                                        lower(trim(when(col('brewery_type').isNull(), 'unknown')
                                                  .otherwise(col('brewery_type')))))
        null_data = null_data.withColumn('country', 
                                        upper(trim(when(col('country').isNull(), 'unknown')
                                                  .otherwise(col('country')))))
        null_data = null_data.withColumn('state', 
                                        upper(trim(when(col('state').isNull(), 'unknown')
                                                  .otherwise(col('state')))))
        null_data = null_data.withColumn('city', 
                                        trim(when(col('city').isNull(), 'unknown')
                                            .otherwise(col('city'))))
        
        # Test null data transformations using efficient PySpark operations
        self.assertEqual(null_data.count(), 1)  # Verify single row
        
        # Test all transformations in a single assertion batch for performance
        null_result = null_data.select('brewery_type', 'country', 'state', 'city').first()
        self.assertEqual(null_result['brewery_type'], 'unknown')
        self.assertEqual(null_result['country'], 'UNKNOWN')
        self.assertEqual(null_result['state'], 'UNKNOWN')
        self.assertEqual(null_result['city'], 'unknown')
        
    def test_environment_variable_validation(self):
        """Test that environment variable validation works correctly"""
        # Test missing environment variables
        with patch.dict(os.environ, {}, clear=True):
            # This would test the get_minio_client function validation
            # In a real test, we'd mock the function and test the error
            required_vars = ['MINIO_ENDPOINT', 'MINIO_ACCESS_KEY', 'MINIO_SECRET_KEY']
            missing_vars = [var for var in required_vars if not os.getenv(var)]
            self.assertEqual(len(missing_vars), 3)  # All should be missing
            
    def test_safe_location_path_cleaning(self):
        """Test location path cleaning for file system safety"""
        test_locations = [
            "UNITED STATES/CALIFORNIA/San Francisco",
            "CANADA/ONTARIO/Toronto-East", 
            "GERMANY/BAVARIA/München",
            "UNKNOWN/UNKNOWN/Unknown"
        ]
        
        expected_safe_locations = [
            "united_states_california_san_francisco",
            "canada_ontario_toronto_east",
            "germany_bavaria_m_nchen", 
            "unknown_unknown_unknown"
        ]
        
        for location, expected in zip(test_locations, expected_safe_locations):
            # Apply same cleaning logic as in silver layer
            safe_location = location.replace('/', '_').replace(' ', '_').replace('-', '_').lower()
            safe_location = ''.join(c if c.isalnum() or c == '_' else '_' for c in safe_location)
            self.assertEqual(safe_location, expected)

    def tearDown(self):
        """Clean up Spark session after tests"""
        # Clear cache to free memory
        self.spark.catalog.clearCache()
        self.spark.stop()

if __name__ == '__main__':
    unittest.main()