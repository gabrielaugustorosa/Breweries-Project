# Medallion Architecture Data Pipeline Solution

## Overview
This solution implements a complete medallion architecture data pipeline using Apache Airflow for orchestration, MinIO as the data lake storage, and Docker for containerization. The pipeline consumes data from the Open Brewery DB API and processes it through Bronze, Silver, and Gold layers.

## Architecture Components

### 🏗️ Infrastructure
- **Orchestration**: Apache Airflow 2.10.0
- **Data Lake**: MinIO (S3-compatible storage)
- **Database**: PostgreSQL 13 
- **Message Broker**: Redis
- **Containerization**: Docker Compose

### 📊 Data Pipeline Layers

#### 🥉 Bronze Layer (`medallion_bronze_layer`)
- **Purpose**: Raw data ingestion from Open Brewery DB API
- **Storage**: JSON format in MinIO `bronze` bucket
- **Features**:
  - Pagination handling for complete data extraction
  - API rate limiting and error handling
  - 3 retries with 5-minute delays
  - Timestamped file naming

**Transformations Applied**: None - raw data preservation

#### 🥈 Silver Layer (`medallion_silver_layer`)
- **Purpose**: Data cleaning, standardization, and partitioning
- **Storage**: Parquet format in MinIO `silver` bucket
- **Partitioning**: By location hierarchy (country/state/city)

**Transformations Applied**:
1. **Data Cleaning**:
   - Standardize brewery types (lowercase, handle nulls)
   - Normalize location fields (country: UPPERCASE, city: Title Case)
   - Convert coordinates to numeric, handle invalid values
   
2. **Data Enrichment**:
   - Create location hierarchy for partitioning
   - Add data quality flags (`has_coordinates`, `has_website`, `has_phone`)
   - Add processing metadata (`processed_at`, `data_source`)

3. **Partitioning Strategy**:
   - Partition by: `country/state/city`
   - File format: Parquet (columnar, compressed)
   - Enables efficient location-based queries

#### 🥇 Gold Layer (`medallion_gold_layer`)
- **Purpose**: Analytical aggregations and business metrics
- **Storage**: Both Parquet and JSON formats in MinIO `gold` bucket
- **Analytics Created**:

**Aggregations Provided**:
1. **Brewery Count by Type and Location**
   - Grouped by: `brewery_type`, `country`, `state`, `city`
   - Metrics: count, data quality percentages

2. **Brewery Summary by Type**
   - Grouped by: `brewery_type`
   - Metrics: total count, quality metrics

3. **Location-based Analytics**
   - Grouped by: `country`, `state`, `city`
   - Metrics: brewery count, unique types per location

4. **Overall Summary Statistics**
   - Total breweries, unique locations, data quality metrics

### 🎼 Orchestration (`medallion_architecture_pipeline`)
- **Main Pipeline**: Coordinates Bronze → Silver → Gold execution
- **Scheduling**: Daily execution (`@daily`)
- **Dependencies**: Sequential execution with proper task sensors
- **Monitoring**: Comprehensive logging and XCom data passing

## Prerequisites
- Git
- Docker
- Docker Compose
- Python 3.7+ (recommended)

## 🐳 Docker Configuration

### Services
- **Airflow Components**: Webserver, Scheduler, Worker, Triggerer
- **MinIO**: S3-compatible object storage
- **PostgreSQL**: Metadata database
- **Redis**: Message broker

### Dependencies
Dependencies are managed through `requirements.txt`:
```
apache-airflow>=2.5.0
requests>=2.25.1
pandas>=1.3.0
minio>=7.1.0
pyarrow>=5.0.0
```

## 📁 Data Lake Structure

```
MinIO Buckets:
├── bronze/
│   └── open_brewery_db_YYYYMMDD_HHMMSS.json
├── silver/
│   └── breweries_by_location/
│       ├── united_states_oregon_portland/
│       ├── united_states_california_san_francisco/
│       └── .../*.parquet
└── gold/
    └── analytics/
        ├── brewery_by_type_location/
        ├── brewery_by_type/
        ├── brewery_by_location/
        └── summary/
```

## 🧪 Testing

### Test Coverage (`test_medallion_transformations.py`)
1. **Data Cleaning Tests**: Verify standardization logic
2. **Aggregation Tests**: Validate gold layer calculations
3. **Percentage Calculations**: Test metric accuracy
4. **Summary Statistics**: Verify overall metrics
5. **Edge Cases**: Handle null data, empty datasets

### Running Tests
```bash
cd applications/airflow/dags
python -m pytest test_medallion_transformations.py -v
```

## 🚀 Deployment & Execution

### 1. Start Infrastructure
```bash
cd applications/airflow
docker-compose up -d
```

### 2. Access Interfaces
- **Airflow UI**: http://localhost:8081
- **MinIO Console**: http://localhost:9001


### 3. Execute Pipeline
**Option A**: Run main orchestrator (recommended)
- Trigger `medallion_architecture_pipeline` DAG

**Option B**: Run individual layers
1. `medallion_bronze_layer` → ingests raw data
2. `medallion_silver_layer` → transforms and partitions  
3. `medallion_gold_layer` → creates analytics

## 🛡️ Error Handling & Reliability

### Retry Strategy
- **Bronze Layer**: 3 retries, 5-minute delays
- **Silver Layer**: 3 retries, 5-minute delays  
- **Gold Layer**: 3 retries, 5-minute delays
- **Orchestrator**: 2 retries, 10-minute delays

### Error Scenarios Handled
1. **API Failures**: Network timeouts, rate limiting
2. **Data Quality Issues**: Null values, invalid formats
3. **Storage Failures**: MinIO connection issues
4. **Processing Errors**: Transformation failures

### Monitoring
- Comprehensive logging at each layer
- XCom data passing for debugging
- Task sensors for dependency management

## 📈 Business Value

### Analytics Delivered
1. **Geographic Distribution**: Breweries by location
2. **Type Analysis**: Distribution by brewery type
3. **Data Quality Metrics**: Completeness tracking
4. **Trend Analysis**: Ready for time-series analysis

### Performance Benefits
- **Partitioned Storage**: Fast location-based queries
- **Columnar Format**: Efficient analytical queries
- **Dual Format Storage**: Parquet for analytics, JSON for APIs

## 🔧 Configuration

### Environment Variables
```yaml
MINIO_ENDPOINT: localhost:9000
```

### Customization Options
- **API Endpoints**: Modify base URL in bronze layer
- **Partitioning Strategy**: Adjust grouping in silver layer
- **Aggregation Logic**: Customize metrics in gold layer
- **Scheduling**: Modify `schedule_interval` parameters

## 🎯 Challenge Requirements Met

✅ **API Consumption**: Open Brewery DB API with pagination  
✅ **Orchestration**: Apache Airflow with scheduling & retries  
✅ **Language**: Python with pandas for transformations  
✅ **Containerization**: Complete Docker Compose setup  
✅ **Bronze Layer**: Raw JSON data preservation  
✅ **Silver Layer**: Parquet format, location partitioning  
✅ **Gold Layer**: Aggregated analytics by type & location  
✅ **Testing**: Comprehensive unit tests  
✅ **Error Handling**: Retries, logging, validation  

## 🚀 Next Steps

### Potential Enhancements
1. **Data Quality Dashboard**: Monitoring data completeness
2. **Delta Lake Integration**: ACID transactions, time travel
3. **Spark Integration**: Large-scale processing
4. **API Gateway**: Serve gold layer analytics
5. **CI/CD Pipeline**: Automated testing and deployment
6. **Alerting**: Email/Slack notifications for failures

## 📚 References & Technical Resources

### Official Documentation
- [Apache Airflow Documentation](https://airflow.apache.org/docs/) - Comprehensive orchestration framework guide
- [MinIO Documentation](https://min.io/docs/) - S3-compatible object storage implementation
- [AWS S3 API Reference](https://docs.aws.amazon.com/s3/index.html) - Standard object storage protocols

### Architecture & Implementation References
- [Medallion Architecture Best Practices](https://github.com/nuriyeakin/DE-6-Airflow-Minio-s3-Bucket) - Enterprise data lakehouse patterns with Airflow and MinIO integration
- [Data Lake Implementation with MinIO and DuckDB](https://www.youtube.com/watch?v=XXE-QPoY4es) - Modern analytics storage architecture using containerized object storage
- [REST API Integration in Airflow](https://www.youtube.com/watch?v=Ai1ajkuZREE) - Production patterns for external API consumption in data pipelines
- [Streamlit-Airflow-MinIO Integration](https://github.com/wlcamargo/setup_streamlit_airflow_s3minio) - Full-stack data platform configuration and orchestration setup

### Development Tools
- [Claude Code](https://claude.ai/code) - AI-assisted development environment used for comprehensive test coverage development, architectural decision validation, and requirements compliance verification

## 🧑🏼‍🚀 Developer
| Developer      | LinkedIn   | Email               | Portfolio   |  
| -------------- | ---------- | ------------------- | ----------- |  
| Gabriel Augusto Rosa | LinkedIn | gabrielrosa.apoio@gmail.com | Portfolio |  

This solution provides a production-ready, scalable data pipeline following modern data engineering best practices with the medallion architecture pattern.