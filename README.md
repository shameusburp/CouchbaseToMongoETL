# Couchbase to MongoDB ETL Application

This application performs an Extract, Transform, Load (ETL) operation from Couchbase to MongoDB using a multithreaded architecture for efficient data transfer. It's designed to handle large datasets with configurable parameters and supports various use cases where integrating data between these two NoSQL database systems is required.

## Features
- Multithreaded design for parallel processing
- Configurable number of threads based on workload
- Supports batch processing for optimized performance
- Clear separation of reader and writer components
- Error handling with logging capabilities
- Command-line interface for easy configuration
- Flexible query options for data extraction
- Document transformation support (optional)

## Prerequisites
1. Python 3.8+
2. Couchbase SDK for Python (`couchbase`)
3. MongoDB driver for Python (`pymongo`)

Install required libraries:
```bash
pip install couchbase pymongo
Configuration Options
The application supports the following configuration parameters via command line arguments:

--host: Couchbase host address (required)
--user: Couchbase username (required)
--password: Couchbase password (required)
--source_bucket: Source bucket in Couchbase (required)
--destination_db: MongoDB database name (required)
--collection: MongoDB collection name (optional, default: 'imported_data')
--num_threads: Number of threads to use (optional, default: 4)
--batch_size: Documents to process per batch (optional, default: 1000)
--query: N1QL query to extract data from Couchbase (optional, default: "SELECT * FROM your_collection")
Usage Examples
Full ETL Process
To perform a full ETL operation from Couchbase to MongoDB:

python CouchbaseToMongoETL.py --host <couchbase_host> --user <username> --password <password> --source_bucket <source_bucket_name> --destination_db <mongodb_database> --num_threads 8

With Specific Collection and Query
To import data from a specific Couchbase collection with a custom query:

python CouchbaseToMongoETL.py --host mycouchbase.example.com --user admin --password password123 --source_bucket production --destination_db analytics --collection sales_data --query "SELECT * FROM `sales` WHERE date >= '2023-01-01'"
With Batch Size Adjustment
To optimize performance with a larger batch size:

python CouchbaseToMongoETL.py --host localhost --user myuser --password securepass --source_bucket test --destination_db mydb --collection data --num_threads 4 --batch_size 5000

Architecture Overview
The application follows a multithreaded queue-based architecture:

Reader Threads: Read documents from Couchbase in batches using the specified query and place them into a queue
Queue: Acts as a buffer between readers and writers
Writer Threads: Retrieve documents from the queue and write them to MongoDB
This design enables parallel processing, improved throughput, and resilience against temporary database issues.

Additional Notes
The Couchbase query defaults to SELECT * FROM \your_collection`` - update this for your specific use case
Ensure that indexes are configured in both Couchbase and MongoDB for optimal performance
Consider adding data transformation logic if the schemas differ between systems
For production deployments, implement monitoring and alerting
Contributing
We welcome contributions! Please follow these guidelines:

Create a new branch from main
Write clear, concise code with comments
Add unit tests for any new functionality
Submit a pull request with a detailed description of your changes
License
This project is licensed under the MIT License - see LICENSE file for details
