import threading
import time
from couchbase.cluster import Cluster, ClusterOptions
from pymongo import MongoClient
from queue import Queue
import argparse
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class CouchbaseReader(threading.Thread):
    def __init__(self, cluster, bucket_name, collection_name, query, batch_size=1000, timeout=60):
        super().__init__()
        self.cluster = cluster
        self.bucket = cluster.get_bucket(bucket_name)
        self.collection_name = collection_name
        self.query = query
        self.batch_size = batch_size
        self.timeout = timeout
        self.stop_event = threading.Event()
        self.doc_count = 0  # Track number of documents processed

    def run(self):
        try:
            index_name = "idx_" + self.collection_name
            if not self.bucket.indexes[index_name]:
                print(f"Creating index {index_name} for collection {self.collection_name}")
                self.bucket.create_index(
                    index_name, 
                    [(self.query, "desc")])  # Example: Descending order by timestamp
                time.sleep(5)  # Wait for index to be built

            print(f"Couchbase reader thread started for collection {self.collection_name}")
            while not self.stop_event.is_set():
                try:
                    result = self.bucket.query(
                        index=index_name, 
                        query=self.query, 
                        batch_size=self.batch_size, 
                        timeout=self.timeout)

                    # Process results in batches
                    for row in result.rows():
                        if doc := row.document:  # Using walrus operator for concise assignment
                            yield doc
                            self.doc_count += 1
                except Exception as e:
                    logging.error(f"Error reading from Couchbase: {e}")
                    time.sleep(5)

        except Exception as e:
            logging.exception(f"Couchbase reader thread error: {e}")
        finally:
            print(f"Couchbase reader processed {self.doc_count} documents")

    def stop(self):
        self.stop_event.set()

class MongoDBWriter(threading.Thread):
    def __init__(self, client, db_name, collection_name, queue):
        super().__init__()
        self.client = client
        self.db = client[db_name]
        self.collection = self.db[collection_name]
        self.queue = queue
        self.stop_event = threading.Event()
        self.doc_count = 0

    def run(self):
        while not self.stop_event.is_set():
            try:
                # Get document from queue with timeout
                doc = self.queue.get(timeout=1)
                if doc:
                    try:
                        # Insert or update in MongoDB (upsert=True for both insert and update)
                        result = self.collection.update_one({
                            "key": doc.get("key", None)  # Assuming Couchbase key is unique
                        }, {"$set": doc}, upsert=True)

                        if result.matched_count == 0:
                            logging.info(f"Inserted document with key: {doc.get('key', 'N/A')}")
                        else:
                            logging.info(f"Updated document with key: {doc.get('key', 'N/A')}")
                        self.doc_count += 1
                    except Exception as e:
                        logging.error(f"Error writing to MongoDB: {e}")
                else:
                    # Queue empty, check if we should stop
                    if self.stop_event.is_set():
                        break
            except Empty:
                # Queue is empty, continue looping
                pass

        print(f"MongoDB writer processed {self.doc_count} documents")

    def stop(self):
        self.stop_event.set()

def etl_data(couchbase_host, couchbase_user, couchbase_password, 
             target_bucket, destination_collection, num_threads=4, query="SELECT * FROM `"+destination_collection+"`"):
    """ETL data from Couchbase to MongoDB using multithreading."""
    logging.info(f"Starting ETL process with {num_threads} threads")

    # Create queue for documents
    queue = Queue()

    # Initialize connections
    cluster = Cluster(f"{couchbase_host}:9361", options=ClusterOptions().credentials(
        username=couchbase_user, password=couchbase_password))
    bucket = cluster.get_bucket(target_bucket)
    client = MongoClient("mongodb://localhost:27017/")  # Replace with your MongoDB connection string
    db_name = "couchbase_etl"

    # Create reader and writer threads
    readers = [CouchbaseReader(cluster, target_bucket, destination_collection, query) for _ in range(num_threads)]
    writers = [MongoDBWriter(client, db_name, destination_collection, queue) for _ in range(num_threads)]

    # Start threads
    for reader in readers:
        reader.start()
    for writer in writers:
        writer.start()

    # Wait for all readers to finish
    for reader in readers:
        reader.join()

    # Signal writers to stop and wait for them to finish
    logging.info("All readers have finished, signaling writers to stop")
    for reader in readers:
        reader.stop()
    for writer in writers:
        writer.stop()
        writer.join()

    logging.info("ETL process complete!")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Couchbase to MongoDB ETL with multithreading")
    parser.add_argument("--host", required=True, help="Couchbase host address")
    parser.add_argument("--user", required=True, help="Couchbase username")
    parser.add_argument("--password", required=True, help="Couchbase password")
    parser.add_argument("--target_bucket", required=True, help="Target bucket in Couchbase")
    parser.add_argument("--dest_collection", required=True, help="Destination collection in MongoDB")
    parser.add_argument("--num_threads", type=int, default=4, help="Number of threads to use (default: 4)")
    args = parser.parse_args()

    etl_data(args.host, args.user, args.password, args.target_bucket, args.dest_collection, args.num_threads)
