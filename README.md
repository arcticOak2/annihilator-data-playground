# Data Phantom Platform

A data processing and analytics platform for managing SQL workflows across multiple engines (Hive, Presto, Spark SQL, MySQL) with AWS EMR integration.

## Features

- Multi-engine SQL support (Hive, Presto, Spark SQL, MySQL)
- Visual workflow management with dependency tracking
- Cron-based scheduling
- Data reconciliation and validation
- UDF support with JAR upload
- AWS EMR and S3 integration
- React-based frontend dashboard ([data-phantom-dashboard](https://github.com/arcticOak2/data-phantom-dashboard))

## Tech Stack

- **Backend**: Java 11, Dropwizard 4.0.2
- **Frontend**: React 18.3.1
- **Database**: MariaDB/MySQL
- **Cloud**: AWS EMR, S3
- **Build**: Maven

## How It Works

### 1. Create Playground
A playground is a container for your data processing workflows. It defines the environment and settings for your tasks.

### 2. Create Tasks
Tasks are individual data processing units within a playground. Each task supports different types:

- **Hive**: Apache Hive queries for data warehousing
- **Presto**: Distributed SQL queries across multiple data sources
- **Spark SQL**: Apache Spark SQL for big data processing
- **PySpark**: Python scripts for Spark processing
- **MySQL**: Direct MySQL database connections

### 3. Task Dependencies (DAG)
Tasks can have parent-child relationships to create a Directed Acyclic Graph (DAG):

- **Parent Tasks**: Must complete successfully before child tasks can run
- **Dependency Chain**: Tasks execute in the correct order based on dependencies
- **Visual DAG**: View task relationships in the React dashboard
- **Parallel Execution**: Independent tasks can run simultaneously

### 4. Data Reconciliation
Compare and validate data between different tasks or sources:

- **Reconciliation Mapping**: Define which tasks to compare
- **Adaptive Comparison Algorithm**: 
  - **Small files**: Exact matching for precise results
  - **Large files**: Bloom filter with 1% error rate for up to 1 million records
- **CSV Comparison**: Intelligent data comparison with adaptive algorithms
- **Data Validation**: Built-in data quality checks
- **Reconciliation Results**: View comparison results and differences

### 5. Execution & History
Run your workflows and track execution history:

- **Scheduled Runs**: Use cron expressions for automated execution
- **Ad-hoc Runs**: Manual execution on demand
- **Limited Runs**: Execute only selected tasks
- **Run History**: Track all execution attempts, status, and results
- **Status Tracking**: Monitor task status (PENDING, RUNNING, SUCCESS, FAILED, etc.)

### 6. UDF Support
Upload and use custom User-Defined Functions:

- **JAR Upload**: Upload custom Java JARs containing UDFs
- **Task-specific UDFs**: Select which UDFs to use for each task
- **Runtime Registration**: Automatic UDF registration during execution

## Quick Start

1. **Install MariaDB:**
   ```bash
# macOS
brew install mariadb
brew services start mariadb

# Ubuntu/Debian
sudo apt-get install mariadb-server
sudo systemctl start mariadb

# CentOS/RHEL
sudo yum install mariadb-server
sudo systemctl start mariadb
```

2. **Setup database:**
```bash
# Create database
mysql -u root -p
CREATE DATABASE data_phantom;
EXIT;

# Run DDL script to create tables
mysql -u root -p data_phantom < src/main/resources/database.ddl
```

3. **Set environment variables:**
```bash
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export MYSQL_URL=jdbc:mariadb://localhost:3306/data_phantom
export MYSQL_USER=root
export MYSQL_PASSWORD=your_root_password
```

4. **Build and run:**
```bash
mvn clean install
java -jar target/annihilator-data-phantom-1.0-SNAPSHOT.jar server src/main/resources/config-dev.yml
```

5. **Access:**
- Backend API: http://localhost:8080
- Health Check: http://localhost:8081/health

## Configuration

The Data Phantom platform uses a comprehensive YAML-based configuration system that allows you to customize all aspects of the application without code changes.

### Configuration File Structure

```yaml
# Server Configuration
server:
  applicationConnectors:
    - type: http
      port: 9092
  adminConnectors:
    - type: http
      port: 9093

# Database Configuration
meta_store:
  driverClass: org.mariadb.jdbc.Driver
  url: jdbc:mariadb://localhost:3306/data_phantom
  user: ${MYSQL_USER:root}
  password: ${MYSQL_PASSWORD:your-password}
  maxSize: 50
  minSize: 10
  maxWaitForConnection: 30s
  maxConnectionAge: 30m
  minIdleTime: 10m
  validationQuery: "SELECT 1"
  validationQueryTimeout: 3s
  checkConnectionOnBorrow: true
  checkConnectionOnReturn: true

# JWT Authentication
jwt:
  secretKey: "${JWT_SECRET_KEY:your-super-secret-jwt-key-change-this-in-production}"
  tokenExpirationMinutes: 60
  refreshTokenExpirationDays: 7

# Concurrency and Performance
concurrency_config:
  adhoc_threadpool_size: 200
  scheduled_threadpool_size: 200
  scheduler_sleep_time: 300000
  playground_execution_grace_period: 300000
  playground_max_execution_frequency: 360000

# Data Reconciliation
reconciliation_settings:
  exact_match_threshold: 1048576
  false_positive_rate: 0.1
  estimated_rows: 1000000

# External Connectors
connector:
  aws_emr:
    access_key: ${AWS_ACCESS_KEY_ID:your-access-key}
    secret_key: ${AWS_SECRET_ACCESS_KEY:your-secret-key}
    s3_bucket: ${AWS_S3_BUCKET:your-s3-bucket}
    s3_path_prefix: ${AWS_S3_PATH_PREFIX:data-phantom}
    region: ${AWS_REGION:us-east-1}
    stack_name: ${AWS_STACK_NAME:DataPhantomClusterStack}
    cluster_logical_id: ${AWS_CLUSTER_LOGICAL_ID:DataPhantomCluster}
    step_polling_interval: 30000
    stack_update_polling_interval: 30000
    stack_update_check_max_attempt: 60
    s3_output_preview_line_count: 100
    s3_max_keys_per_request: 20
    max_step_retries: 3

  mysql:
    driverClass: org.mariadb.jdbc.Driver
    url: ${MYSQL_URL:jdbc:mariadb://localhost:3306/your_database}
    user: ${MYSQL_USER:your_username}
    password: ${MYSQL_PASSWORD:your_password}
    maxSize: 20
    minSize: 5
    maxWaitForConnection: 30s
    maxConnectionAge: 30m
    minIdleTime: 10m
    validationQuery: "SELECT 1"
    validationQueryTimeout: 3s
    checkConnectionOnBorrow: true
    checkConnectionOnReturn: true
    outputDirectory: ${SQL_OUTPUT_DIRECTORY:/tmp/sql-output}
```

### Configuration Sections

#### Server Configuration
- **applicationConnectors**: Main application server settings
- **adminConnectors**: Administrative interface settings
- **port**: Server port (default: 9092 for app, 9093 for admin)

#### Meta Store (Database)
- **driverClass**: Database driver (MariaDB/MySQL)
- **url**: Database connection URL
- **user/password**: Database credentials
- **maxSize/minSize**: Connection pool settings
- **maxWaitForConnection**: Maximum wait time for connections
- **maxConnectionAge**: Maximum connection lifetime
- **minIdleTime**: Minimum idle time before connection cleanup
- **validationQuery**: Health check query
- **validationQueryTimeout**: Timeout for validation queries

#### JWT Authentication
- **secretKey**: Secret key for JWT token signing (⚠️ **CHANGE IN PRODUCTION**)
- **tokenExpirationMinutes**: Access token expiration time
- **refreshTokenExpirationDays**: Refresh token expiration time

#### Concurrency Configuration
- **adhoc_threadpool_size**: Thread pool size for adhoc task execution (1-1000)
- **scheduled_threadpool_size**: Thread pool size for scheduled tasks (1-1000)
- **scheduler_sleep_time**: Scheduler polling interval in milliseconds (min: 1000ms)
- **playground_execution_grace_period**: Grace period for playground execution (5 minutes)
- **playground_max_execution_frequency**: Minimum time between playground executions (6 minutes)

#### Reconciliation Settings
- **exact_match_threshold**: File size threshold for exact matching vs bloom filter (1MB)
- **false_positive_rate**: Bloom filter false positive rate (0.1 = 10%)
- **estimated_rows**: Estimated number of rows for bloom filter sizing

#### AWS EMR Configuration
- **access_key/secret_key**: AWS credentials
- **s3_bucket**: S3 bucket for data storage
- **s3_path_prefix**: S3 path prefix for organization
- **region**: AWS region
- **stack_name**: CloudFormation stack name
- **cluster_logical_id**: EMR cluster logical ID
- **step_polling_interval**: EMR step status polling interval (30 seconds)
- **stack_update_polling_interval**: CloudFormation update polling interval (30 seconds)
- **stack_update_check_max_attempt**: Maximum attempts for stack update checks (60)
- **s3_output_preview_line_count**: Number of lines to preview in S3 output (100)
- **s3_max_keys_per_request**: Maximum S3 objects per list request (20)
- **max_step_retries**: Maximum retry attempts for failed EMR steps (3)

#### MySQL Connector Configuration
- **driverClass**: MySQL driver class
- **url**: MySQL connection URL
- **user/password**: MySQL credentials
- **maxSize/minSize**: Connection pool settings
- **maxWaitForConnection**: Maximum wait time for connections
- **maxConnectionAge**: Maximum connection lifetime
- **minIdleTime**: Minimum idle time before connection cleanup
- **validationQuery**: Health check query
- **validationQueryTimeout**: Timeout for validation queries
- **outputDirectory**: Directory for MySQL query output files

### Environment Variables

The configuration supports environment variables for sensitive data:

```bash
# AWS Configuration
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
export AWS_S3_BUCKET=your-s3-bucket
export AWS_S3_PATH_PREFIX=data-phantom
export AWS_REGION=us-east-1
export AWS_STACK_NAME=DataPhantomClusterStack
export AWS_CLUSTER_LOGICAL_ID=DataPhantomCluster

# Database Configuration
export MYSQL_URL=jdbc:mariadb://localhost:3306/data_phantom
export MYSQL_USER=your_username
export MYSQL_PASSWORD=your_password

# Output Configuration
export SQL_OUTPUT_DIRECTORY=/tmp/sql-output

# JWT Configuration
export JWT_SECRET_KEY=your-super-secret-jwt-key-change-this-in-production
```

### Configuration Best Practices

1. **Security**: Never commit real credentials to version control
2. **Environment Variables**: Use environment variables for sensitive data
3. **Validation**: Test configuration changes in development first
4. **Documentation**: Document any custom configuration changes
5. **Backup**: Keep backup copies of working configurations

### Performance Tuning

#### Thread Pool Sizing
- **adhoc_threadpool_size**: Set based on expected concurrent adhoc runs
- **scheduled_threadpool_size**: Set based on number of scheduled playgrounds
- **Recommendation**: Start with 200, adjust based on load

#### Database Connection Pool
- **maxSize**: Maximum connections (recommended: 50 for meta store, 20 for MySQL)
- **minSize**: Minimum connections (recommended: 10 for meta store, 5 for MySQL)
- **maxConnectionAge**: Connection lifetime (recommended: 30 minutes)

#### Scheduler Performance
- **scheduler_sleep_time**: Lower values = more responsive, higher CPU usage
- **playground_execution_grace_period**: Time to wait before considering a playground stuck
- **playground_max_execution_frequency**: Minimum time between executions to prevent overload

#### AWS EMR Optimization
- **step_polling_interval**: Lower values = faster status updates, more API calls
- **s3_max_keys_per_request**: Higher values = fewer API calls, more memory usage
- **max_step_retries**: Higher values = more resilience, longer failure recovery time

## API Endpoints

### Authentication
- `POST /auth/login` - User login
- `POST /auth/register` - User registration
- `POST /auth/refresh` - Refresh token
- `POST /auth/change-password` - Change password
- `POST /auth/forgot-password` - Forgot password
- `POST /auth/reset-password` - Reset password

### Playground Management
- `GET /data-phantom/playground/{user_id}` - Get user playgrounds
- `POST /data-phantom/playground` - Create playground
- `PUT /data-phantom/playground/update` - Update playground
- `DELETE /data-phantom/playground/{id}` - Delete playground
- `POST /data-phantom/playground/cancel/{id}` - Cancel playground execution

### Task Management
- `GET /data-phantom/playground/{playgroundId}/tasks` - Get playground tasks
- `POST /data-phantom/task` - Create task
- `PUT /data-phantom/task/query` - Update task query
- `DELETE /data-phantom/task/{taskId}` - Delete task
- `GET /data-phantom/task/fields/{task_id}` - Get task fields

### Execution
- `POST /data-phantom/adhoc-run/{playground_id}` - Run playground
- `POST /data-phantom/limited-adhoc-run` - Limited run
- `GET /data-phantom/playground/{playgroundId}/run-history` - Get run history
- `GET /data-phantom/ping` - Health check

### Reconciliation
- `POST /data-phantom/reconciliation-mapping` - Create reconciliation mapping
- `GET /data-phantom/reconciliation-mapping/playground/{playgroundId}` - Get playground mappings
- `GET /data-phantom/reconciliation-mapping/{reconciliationId}` - Get mapping
- `PUT /data-phantom/reconciliation-mapping/{reconciliationId}` - Update mapping
- `DELETE /data-phantom/reconciliation-mapping/{reconciliationId}` - Delete mapping
- `POST /data-phantom/reconciliation-run/{reconciliation_id}` - Run reconciliation
- `GET /data-phantom/reconciliation-status/{reconciliationId}` - Get reconciliation status
- `GET /data-phantom/reconciliation-result/{reconciliationId}` - Get reconciliation result
- `POST /data-phantom/reconciliation-cancel/{reconciliation_id}` - Cancel reconciliation

### UDF Management
- `POST /data-phantom/udf` - Upload UDF
- `GET /data-phantom/udf/user/{userId}` - Get user UDFs
- `DELETE /data-phantom/udf/{udfId}` - Delete UDF

### User Management
- `POST /data-phantom/user` - Create user
- `DELETE /data-phantom/user/{user_id}` - Delete user

### Data Preview
- `GET /data-phantom/preview/{s3Location}` - Preview S3 data

## Author

**Abhijeet Kumar**
- Email: [searchabhijeet@gmail.com](mailto:searchabhijeet@gmail.com)
- LinkedIn: [@abhijeet-kumar-983b57a4](https://www.linkedin.com/in/abhijeet-kumar-983b57a4/)

## License

Apache License 2.0