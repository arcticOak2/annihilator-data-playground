# Data Phantom Platform

A data processing and analytics platform for managing SQL workflows across multiple engines (Hive, Presto, Spark SQL, MySQL) with AWS EMR integration.

## Features

- Multi-engine SQL support (Hive, Presto, Spark SQL, MySQL)
- Visual workflow management with dependency tracking
- Cron-based scheduling
- Data reconciliation and validation
- UDF support with JAR upload
- AWS EMR and S3 integration
- React-based frontend dashboard

## Tech Stack

- **Backend**: Java 11, Dropwizard 4.0.2
- **Frontend**: React 18.3.1
- **Database**: MariaDB/MySQL
- **Cloud**: AWS EMR, S3
- **Build**: Maven

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

Update `src/main/resources/config-dev.yml` with your settings:

```yaml
aws_emr:
  access_key: ${AWS_ACCESS_KEY_ID:your-access-key}
  secret_key: ${AWS_SECRET_ACCESS_KEY:your-secret-key}
  s3_bucket: ${AWS_S3_BUCKET:your-s3-bucket}
  s3_path_prefix: ${AWS_S3_PATH_PREFIX:data-phantom}
  region: ${AWS_REGION:us-east-1}

database:
  driverClass: org.mariadb.jdbc.Driver
  url: ${MYSQL_URL:jdbc:mariadb://localhost:3306/your_database}
  user: ${MYSQL_USER:your_username}
  password: ${MYSQL_PASSWORD:your_password}
```

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