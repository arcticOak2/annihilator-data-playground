# Data Phantom Platform

[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](LICENSE)
[![Java](https://img.shields.io/badge/java-11-orange.svg)](https://openjdk.java.net/)
[![Dropwizard](https://img.shields.io/badge/dropwizard-4.0.2-green.svg)](https://www.dropwizard.io/)
[![React](https://img.shields.io/badge/react-18.3.1-blue.svg)](https://reactjs.org/)

**Data Phantom** is a comprehensive data processing and analytics platform that provides a robust framework for managing data processing workflows, executing SQL queries across multiple engines, and performing data reconciliation tasks. The platform is designed to handle complex data pipelines with dependency management, scheduling, and cloud-based execution.

## 🚀 Key Features

### Multi-Engine SQL Support
- **Hive**: Apache Hive queries for data warehousing
- **Presto**: Distributed SQL queries across multiple data sources
- **Spark SQL**: Apache Spark SQL for big data processing
- **PySpark**: Python scripts for Spark processing
- **MySQL**: Direct MySQL database connections

### Visual Workflow Management
- **Interactive DAG Visualization**: Visual representation of task dependencies using ReactFlow
- **Real-time Status Updates**: Live monitoring of task execution status
- **Dependency Management**: Parent-child task relationships with automatic execution ordering
- **Multiple View Modes**: List, Graph, History, and Reconciliation views

### Advanced Scheduling
- **Cron-based Scheduling**: Flexible scheduling with standard cron expressions
- **Ad-hoc Execution**: Manual task execution on demand
- **Recovery Execution**: Automatic recovery from system interruptions
- **Limited Execution**: Run only selected tasks from a playground

### Data Quality & Reconciliation
- **Built-in Reconciliation**: Automated data comparison and validation
- **CSV Comparison**: Intelligent data comparison with adaptive algorithms
- **Data Validation**: Built-in data quality checks and validation

### User-Defined Functions (UDFs)
- **JAR Upload**: Upload custom Java JARs containing UDFs
- **Runtime Registration**: Automatic UDF registration during task execution
- **Task-specific UDFs**: Select which UDFs to use for each task
- **S3 Integration**: Secure UDF storage and distribution

### Cloud-Native Architecture
- **AWS EMR Integration**: Scalable distributed processing on AWS
- **S3 Storage**: Cloud-based data storage and result storage
- **Auto-scaling**: Dynamic cluster management based on workload
- **Cost Optimization**: Pay-per-use model with efficient resource utilization

## 🏗️ Architecture

### Backend Components
- **Application Layer**: Dropwizard-based REST API with JWT authentication
- **Execution Engine**: AWS EMR integration for distributed data processing
- **Scheduling System**: Cron-based task scheduling with dependency management
- **Data Storage**: MariaDB for metadata and S3 for data storage
- **Reconciliation Engine**: Automated data comparison and validation
- **UDF Support**: Custom function management for extended SQL capabilities

### Frontend Components
- **React Dashboard**: Modern React 18.3.1 with professional UI/UX
- **Monaco Editor**: Advanced SQL editor with syntax highlighting and auto-complete
- **Real-time Updates**: Auto-refresh functionality with configurable intervals
- **Responsive Design**: Collapsible sidebar and tab-based playground management
- **Visual Components**: Interactive graphs, charts, and status indicators

### Technology Stack
- **Backend**: Java 11, Dropwizard 4.0.2
- **Frontend**: React 18.3.1, React Router DOM, ReactFlow
- **Database**: MariaDB with connection pooling
- **Cloud**: AWS EMR, S3, CloudFormation
- **Authentication**: JWT with bcrypt password hashing
- **Build Tools**: Maven, npm
- **Logging**: SLF4J with Log4j2

## 📋 Core Concepts

### Playgrounds
A **Playground** is the top-level organizational unit that contains a collection of related data processing tasks. Each playground has:
- **ID**: Unique identifier (UUID)
- **Name**: Human-readable name
- **Cron Expression**: Schedule for automatic execution
- **Status**: Current execution state (IDLE, RUNNING, SUCCESS, FAILED, etc.)
- **User ID**: Owner of the playground
- **Execution History**: Track record of runs with success/failure counts

### Tasks
**Tasks** are individual data processing units within a playground. Each task represents a single SQL query or script execution with:
- **Task Types**: HIVE, PRESTO, SPARK_SQL, PY_SPARK, SQL
- **Query**: The actual SQL or script content
- **Dependencies**: Parent-child relationships for execution order
- **UDF Support**: Custom functions that can be attached
- **Output Location**: S3 path where results are stored

### Status Management
The platform uses a comprehensive status system:
- **PENDING**: Task queued for execution
- **RUNNING**: Currently executing
- **SUCCESS**: Completed successfully
- **FAILED**: Execution failed
- **CANCELLED**: User-initiated cancellation
- **SKIPPED**: Skipped due to dependencies or user selection
- **UPSTREAM_FAILED**: Failed due to dependency failure
- **PARTIAL_SUCCESS**: Some tasks succeeded, others failed

## 🚀 Quick Start

**TL;DR - Get running in 5 minutes:**
```bash
# 1. Set environment variables
export AWS_ACCESS_KEY_ID=your_key
export AWS_SECRET_ACCESS_KEY=your_secret
export MYSQL_URL=jdbc:mariadb://localhost:3306/your_db
export MYSQL_USER=your_user
export MYSQL_PASSWORD=your_pass

# 2. Build and run
mvn clean install
java -jar target/annihilator-data-phantom-1.0-SNAPSHOT.jar server src/main/resources/config-dev.yml

# 3. Access the application
# Backend: http://localhost:8080
# Health: http://localhost:8081/health
```

### Prerequisites
- Java 11 or higher
- Node.js 18 or higher
- MariaDB or MySQL
- AWS Account (for EMR integration)
- Maven 3.6+

### Backend Setup

1. **Clone the repository**
   ```bash
   git clone https://github.com/your-username/data-phantom-platform.git
   cd data-phantom-platform
   ```

2. **Configure database**
   ```bash
   # Update src/main/resources/config-dev.yml with your database settings
   database:
     driverClass: org.mariadb.jdbc.Driver
     url: jdbc:mariadb://localhost:3306/data_phantom
     user: your_username
     password: your_password
   ```

3. **Configure AWS credentials**
   ```bash
   # Set AWS credentials
   export AWS_ACCESS_KEY_ID=your_access_key
   export AWS_SECRET_ACCESS_KEY=your_secret_key
   export AWS_DEFAULT_REGION=us-east-1
   ```

4. **Build and run**
   
   **Option 1: Quick Start (Development)**
   ```bash
   # Build the project
   mvn clean install
   
   # Run with development configuration
   java -jar target/annihilator-data-phantom-1.0-SNAPSHOT.jar server src/main/resources/config-dev.yml
   ```
   
   **Option 2: Production Build**
   ```bash
   # Create a fat JAR with all dependencies
   mvn clean package
   
   # Run with custom configuration
   java -jar target/annihilator-data-phantom-1.0-SNAPSHOT.jar server /path/to/your/config.yml
   ```
   
   **Option 3: Run Tests**
   ```bash
   # Run all tests
   mvn test
   
   # Run specific test class
   mvn test -Dtest=DataPhantomResourceTest
   ```
   
   **Application will be available at:**
   - **Backend API**: http://localhost:8080
   - **Health Check**: http://localhost:8081/health
   - **Admin Interface**: http://localhost:8081

### Configuration

The application uses environment variables for sensitive configuration. Update `src/main/resources/config-dev.yml` or create environment-specific configs:

**Environment Variables:**
```bash
# AWS Configuration
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
export AWS_REGION=us-east-1
export AWS_S3_BUCKET=your-s3-bucket
export AWS_S3_PATH_PREFIX=data-phantom
export AWS_STACK_NAME=DataPhantomClusterStack
export AWS_CLUSTER_LOGICAL_ID=DataPhantomCluster

# Database Configuration
export MYSQL_URL=jdbc:mariadb://localhost:3306/your_database
export MYSQL_USER=your_username
export MYSQL_PASSWORD=your_password

# Application Configuration
export SQL_OUTPUT_DIRECTORY=/tmp/sql-output
```

**Configuration File Structure:**
```yaml
# config-dev.yml
server:
  applicationConnectors:
    - type: http
      port: 8080
  adminConnectors:
    - type: http
      port: 8081

aws_emr:
  access_key: ${AWS_ACCESS_KEY_ID:your-access-key}
  secret_key: ${AWS_SECRET_ACCESS_KEY:your-secret-key}
  s3_bucket: ${AWS_S3_BUCKET:your-s3-bucket}
  s3_path_prefix: ${AWS_S3_PATH_PREFIX:data-phantom}
  region: ${AWS_REGION:us-east-1}
  stack_name: ${AWS_STACK_NAME:DataPhantomClusterStack}
  cluster_logical_id: ${AWS_CLUSTER_LOGICAL_ID:DataPhantomCluster}

database:
  driverClass: org.mariadb.jdbc.Driver
  url: ${MYSQL_URL:jdbc:mariadb://localhost:3306/your_database}
  user: ${MYSQL_USER:your_username}
  password: ${MYSQL_PASSWORD:your_password}
```

### Frontend Setup

1. **Navigate to dashboard directory**
   ```bash
   cd ../data-phantom-dashboard
   ```

2. **Install dependencies**
   ```bash
   npm install
   ```

3. **Configure API endpoint**
   ```bash
   # Update package.json or create .env file
   REACT_APP_API_URL=http://localhost:9092
   ```

4. **Start development server**
   ```bash
   npm start
   ```

5. **Access the application**
   - Frontend: http://localhost:3000
   - Backend API: http://localhost:9092

## 🔧 Troubleshooting

### Common Issues

**1. Build Failures**
```bash
# Clean and rebuild
mvn clean install -U

# Skip tests if needed
mvn clean install -DskipTests
```

**2. Database Connection Issues**
```bash
# Check database is running
mysql -u your_username -p your_database

# Verify connection string in config-dev.yml
# Ensure MariaDB driver is available
```

**3. AWS Credentials Issues**
```bash
# Verify AWS credentials
aws sts get-caller-identity

# Check environment variables
echo $AWS_ACCESS_KEY_ID
echo $AWS_SECRET_ACCESS_KEY
```

**4. Port Conflicts**
```bash
# Check if ports are in use
lsof -i :8080
lsof -i :8081

# Kill process if needed
kill -9 <PID>
```

**5. Memory Issues**
```bash
# Increase JVM memory
java -Xmx2g -jar target/annihilator-data-phantom-1.0-SNAPSHOT.jar server config-dev.yml
```

**6. Log Analysis**
```bash
# Check application logs
tail -f logs/app.log

# Check Maven build logs
mvn clean install -X
```

### Health Checks

**Backend Health:**
- Health endpoint: http://localhost:8081/health
- Metrics: http://localhost:8081/metrics
- Threads: http://localhost:8081/threads

**Database Health:**
```sql
-- Test database connection
SELECT 1;

-- Check tables
SHOW TABLES;
```

## 📖 API Documentation

### Authentication
All API calls require a Bearer token in the Authorization header:
```
Authorization: Bearer {token}
Content-Type: application/json
```

### Key Endpoints

#### Playground Management
- `GET /data-phantom/playground/{user_id}` - Get all playgrounds for a user
- `POST /data-phantom/playground` - Create a new playground
- `PUT /data-phantom/playground/update` - Update playground settings
- `DELETE /data-phantom/playground/{id}` - Delete a playground

#### Task Management
- `GET /data-phantom/playground/{playgroundId}/tasks` - Get tasks for a playground
- `POST /data-phantom/task` - Create a new task
- `PUT /data-phantom/task/query` - Update task query
- `DELETE /data-phantom/task/{taskId}` - Delete a task

#### Execution Control
- `POST /data-phantom/adhoc-run/{playground_id}` - Run all tasks in playground
- `POST /data-phantom/limited-adhoc-run` - Run selected tasks
- `POST /data-phantom/playground/cancel/{id}` - Cancel running execution

#### UDF Management
- `POST /data-phantom/udf` - Register a new UDF
- `GET /data-phantom/udf/user/{userId}` - Get UDFs for a user
- `DELETE /data-phantom/udf/{udfId}` - Delete a UDF

## 🔧 Configuration

### Cron Expression Support
The platform supports both 5-part Unix and 6-part Quartz cron formats:

- **5-part Unix**: `minute hour day month day-of-week`
- **6-part Quartz**: `second minute hour day month day-of-week`

Common patterns:
- `0 0 12 * * ?` - Daily at noon
- `0 30 9 * * ?` - Daily at 9:30 AM
- `0 */5 * * * *` - Every 5 minutes
- `0 0 */2 * * *` - Every 2 hours

### AWS EMR Configuration
```yaml
aws:
  emr:
    clusterName: "data-phantom-cluster"
    instanceType: "m5.xlarge"
    instanceCount: 2
    region: "us-east-1"
    s3Bucket: "your-data-phantom-bucket"
```

## 🧪 Testing

### Backend Tests
```bash
mvn test
```

### Frontend Tests
```bash
cd data-phantom-dashboard
npm test
```

### Integration Tests
```bash
cd data-phantom-dashboard
npm run test:integration
```

## 🐳 Docker Deployment

### Backend Dockerfile
```dockerfile
FROM openjdk:11-jre-slim
COPY target/annihilator-data-phantom-1.0-SNAPSHOT.jar app.jar
EXPOSE 9092
ENTRYPOINT ["java", "-jar", "app.jar", "server", "config-prod.yml"]
```

### Frontend Dockerfile
```dockerfile
FROM node:18-alpine as build
WORKDIR /app
COPY package*.json ./
RUN npm install
COPY . .
RUN npm run build

FROM nginx:alpine
COPY --from=build /app/build /usr/share/nginx/html
EXPOSE 80
```

### Docker Compose
```yaml
version: '3.8'
services:
  backend:
    build: .
    ports:
      - "9092:9092"
    environment:
      - DATABASE_URL=jdbc:mariadb://db:3306/data_phantom
    depends_on:
      - db
  
  frontend:
    build: ./data-phantom-dashboard
    ports:
      - "3000:80"
    depends_on:
      - backend
  
  db:
    image: mariadb:10.6
    environment:
      MYSQL_ROOT_PASSWORD: rootpassword
      MYSQL_DATABASE: data_phantom
    volumes:
      - db_data:/var/lib/mysql

volumes:
  db_data:
```

## 🤝 Contributing

We welcome contributions! Please see our [Contributing Guidelines](CONTRIBUTING.md) for details.

### Development Setup
1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests for new functionality
5. Submit a pull request

### Code Style
- Java: Follow Google Java Style Guide
- JavaScript/React: Follow Airbnb Style Guide
- Use meaningful commit messages
- Add documentation for new features

## 📄 License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.

## 👨‍💻 Author

**Abhijeet Kumar**
- 📧 **Email**: [searchabhijeet@gmail.com](mailto:searchabhijeet@gmail.com)
- 💼 **LinkedIn**: [@abhijeet-kumar-983b57a4](https://www.linkedin.com/in/abhijeet-kumar-983b57a4/)

## 🆘 Support

- **Documentation**: Check this README and the inline code documentation
- **Issues**: Report bugs and request features via GitHub Issues
- **Discussions**: Join our community discussions for questions and ideas
- **Email**: Contact the maintainer at [searchabhijeet@gmail.com](mailto:searchabhijeet@gmail.com)

## 🗺️ Roadmap

### Upcoming Features
- **Multi-cloud Support**: Azure HDInsight, Google Cloud Dataproc
- **Containerization**: Kubernetes deployment with Helm charts
- **Advanced Monitoring**: Centralized logging and metrics with Prometheus/Grafana
- **Workflow Templates**: Pre-built workflow patterns for common use cases
- **Collaboration**: Multi-user playground sharing and team management
- **Real-time Updates**: WebSocket connections for live status updates
- **Mobile Support**: Progressive Web App with offline capabilities

### Performance Improvements
- **Advanced Caching**: Redis integration for improved performance
- **Query Optimization**: Automatic query optimization and indexing
- **Resource Management**: Dynamic resource allocation based on workload
- **Parallel Processing**: Enhanced parallel task execution

## 🙏 Acknowledgments

- **Apache Software Foundation** for Hive, Spark, and other open source projects
- **Dropwizard** for the excellent Java web framework
- **React Team** for the powerful frontend framework
- **AWS** for cloud infrastructure services
- **Community Contributors** who help improve the platform

---

**Data Phantom Platform** - Making data processing workflows simple, visual, and powerful.

For more information, visit our [documentation](docs/) or join our [community discussions](https://github.com/your-username/data-phantom-platform/discussions).

---

**Created by**: [Abhijeet Kumar](https://www.linkedin.com/in/abhijeet-kumar-983b57a4/) | [searchabhijeet@gmail.com](mailto:searchabhijeet@gmail.com)
