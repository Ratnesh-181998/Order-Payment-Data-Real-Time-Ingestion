# Order Payment Data Real Time Ingestion

<img width="977" height="503" alt="image" src="https://github.com/user-attachments/assets/7d39bd63-3e8f-48bb-be9a-5e7cef3420b7" />

---

🚀 Sales Order & Payment Data -- Real-Time Ingestion



A production-oriented real-time data ingestion pipeline forprocessing Sales Order and Payment events using Google CloudPub/Sub, Python, Docker, and Apache Cassandra.

The project demonstrates an event-driven architecture in which order andpayment events are published to a Pub/Sub topic, consumed by a Pythonservice, validated/transformed, and persisted into Cassandra forscalable and low-latency access.

📌 Project Overview

Modern e-commerce and payment platforms generate a continuous stream oforder and transaction events. Batch processing can introduce delays andis not ideal when downstream systems require near real-time information.

This project solves that problem by implementing a real-time streamingingestion pipeline:

Sales / Payment Source
        │
        ▼
┌───────────────────────┐
│   Python Producer     │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│    Google Pub/Sub     │
│                       │
│       Topic           │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│   Python Consumer     │
│                       │
│ Validate              │
│ Transform             │
│ Error Handling        │
└───────────┬───────────┘
            │
            ▼
┌───────────────────────┐
│      Cassandra        │
│       NoSQL DB        │
└───────────────────────┘

🎯 Objectives

Ingest sales order and payment events in real time.

Implement asynchronous event communication using Google CloudPub/Sub.

Build reliable Python publisher and subscriber services.

Validate and transform incoming event data.

Store high-volume event data in Apache Cassandra.

Containerize services using Docker.

Implement retry and failure-handling mechanisms.

Design the system for scalability and high write throughput.

Maintain an architecture that can be extended for productionworkloads.

🛠️ Technology Stack

Technology                 Purpose

Python                 Producer, consumer, validation and processingGoogle Cloud Pub/Sub   Real-time messaging and event streamingApache Cassandra       Distributed NoSQL data storageDocker                 Application containerizationJSON                   Event/message formatPytest                 Unit and integration testingGit/GitHub             Version control and source management

📂 Project Structure

order-payment-realtime-ingestion/
│
├── producer/
│   ├── __init__.py
│   └── publisher.py
│
├── consumer/
│   ├── __init__.py
│   └── subscriber.py
│
├── database/
│   ├── __init__.py
│   ├── cassandra_client.py
│   └── queries.py
│
├── models/
│   ├── __init__.py
│   └── order_payment.py
│
├── config/
│   └── config.py
│
├── utils/
│   ├── logger.py
│   └── validation.py
│
├── tests/
│   ├── test_producer.py
│   ├── test_consumer.py
│   └── test_database.py
│
├── requirements.txt
├── Dockerfile
├── docker-compose.yml
├── .env.example
├── .gitignore
└── README.md

🔄 Data Flow

1. Event Generation

A sales order/payment system generates an event.

Example:

{
  "order_id": "ORD10001",
  "customer_id": "CUS5001",
  "product_id": "PROD101",
  "quantity": 2,
  "amount": 2499.99,
  "payment_method": "UPI",
  "payment_status": "SUCCESS",
  "order_status": "CONFIRMED",
  "timestamp": "2026-08-11T10:30:15Z"
}

2. Publishing

The Python producer publishes the event to a Google Cloud Pub/Sub topic.

Python Producer
      ↓
Pub/Sub Topic

3. Consumption

A Python subscriber listens to the Pub/Sub subscription and receivesincoming messages.

Pub/Sub Subscription
          ↓
    Python Consumer

4. Validation & Transformation

The consumer:

Validates required fields.

Checks data types.

Handles malformed messages.

Normalizes timestamps.

Performs required transformations.

Prepares the record for Cassandra.

5. Persistence

Validated events are inserted into Cassandra.

Python Consumer
      ↓
Validation
      ↓
Transformation
      ↓
Cassandra

6. Message Acknowledgement

After successful database persistence, the consumer acknowledges thePub/Sub message.

Successful Cassandra Write
          ↓
       ACK Message

If processing fails, the message can be negatively acknowledged/retriedaccording to the configured retry policy.

🗄️ Cassandra Data Model

Example keyspace:

CREATE KEYSPACE order_payment
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};

Example table:

CREATE TABLE order_payment.orders (
    order_id TEXT,
    customer_id TEXT,
    product_id TEXT,
    quantity INT,
    amount DECIMAL,
    payment_method TEXT,
    payment_status TEXT,
    order_status TEXT,
    event_timestamp TIMESTAMP,
    PRIMARY KEY (order_id)
);

For production deployments, use an appropriate replication strategysuch as NetworkTopologyStrategy and design partition keys based onactual query patterns and expected data volume.

🐍 Python Producer

The producer is responsible for creating and publishing order/paymentevents.

Conceptually:

from google.cloud import pubsub_v1
import json

publisher = pubsub_v1.PublisherClient()

topic_path = publisher.topic_path(
    "YOUR_GCP_PROJECT_ID",
    "order-payment-topic"
)

event = {
    "order_id": "ORD10001",
    "amount": 2499.99,
    "payment_status": "SUCCESS"
}

data = json.dumps(event).encode("utf-8")

future = publisher.publish(topic_path, data)
print(f"Published message ID: {future.result()}")

📥 Python Consumer

The consumer subscribes to the Pub/Sub subscription and processesmessages.

Conceptually:

from google.cloud import pubsub_v1

subscriber = pubsub_v1.SubscriberClient()

subscription_path = subscriber.subscription_path(
    "YOUR_GCP_PROJECT_ID",
    "order-payment-subscription"
)

def callback(message):
    try:
        data = message.data.decode("utf-8")

        # Validate event
        # Transform event
        # Persist to Cassandra

        message.ack()

    except Exception:
        message.nack()

streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback
)

streaming_pull_future.result()

🐳 Docker

The application is containerized to provide a consistent runtimeenvironment.

Example Dockerfile:

FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "consumer.subscriber"]

Build:

docker build -t order-payment-ingestion .

Run:

docker run --env-file .env order-payment-ingestion

☁️ Google Cloud Pub/Sub Setup

Create a Topic

gcloud pubsub topics create order-payment-topic

Create a Subscription

gcloud pubsub subscriptions create order-payment-subscription \
    --topic=order-payment-topic

Publish a Test Message

gcloud pubsub topics publish order-payment-topic \
    --message='{"order_id":"ORD10001","amount":2499.99,"payment_status":"SUCCESS"}'

Pull Messages

gcloud pubsub subscriptions pull order-payment-subscription \
    --auto-ack

⚙️ Environment Variables

Create a .env file based on .env.example.

GCP_PROJECT_ID=your-gcp-project-id
PUBSUB_TOPIC=order-payment-topic
PUBSUB_SUBSCRIPTION=order-payment-subscription

CASSANDRA_HOST=localhost
CASSANDRA_PORT=9042
CASSANDRA_KEYSPACE=order_payment
CASSANDRA_TABLE=orders

Do not commit credentials, service-account JSON files, passwords, orprivate keys to GitHub.

📦 Installation

Clone the repository:

git clone https://github.com/<YOUR_USERNAME>/order-payment-realtime-ingestion.git

cd order-payment-realtime-ingestion

Create a virtual environment:

python -m venv venv

Activate it on Windows:

venv\Scripts\activate

Activate it on Linux/macOS:

source venv/bin/activate

Install dependencies:

pip install -r requirements.txt

▶️ Running the Project

Start Cassandra

If using Docker:

docker-compose up -d cassandra

Start the Consumer

python -m consumer.subscriber

Start the Producer

In another terminal:

python -m producer.publisher

The producer publishes events to Pub/Sub and the consumer processes thembefore storing them in Cassandra.

🔁 Reliability & Error Handling

The pipeline is designed with reliability in mind.

Message Processing

Receive Message
      │
      ▼
Validate Message
      │
   ┌──┴───┐
   │      │
 Valid   Invalid
   │      │
   ▼      ▼
Transform  Log Error
   │
   ▼
Write to Cassandra
   │
 ┌─┴──────┐
 │        │
Success  Failure
 │        │
ACK     Retry/NACK

Recommended production capabilities include:

Pub/Sub retry policies

Dead-letter topics

Exponential backoff

Structured logging

Idempotent message processing

Cassandra replication

Monitoring and alerting

Health checks

Graceful shutdown

🔐 Security Considerations

For production environments:

Use Google Cloud IAM with least-privilege permissions.

Store secrets in a secure secret-management system.

Do not hard-code service-account credentials.

Encrypt data in transit.

Restrict Cassandra network access.

Use TLS for Cassandra connections where applicable.

Keep credentials outside source control.

Separate development, staging, and production environments.

📈 Scalability

The architecture can scale horizontally:

                 ┌───────────────┐
                 │ Pub/Sub Topic │
                 └───────┬───────┘
                         │
              ┌──────────┼──────────┐
              ▼          ▼          ▼
          Consumer 1  Consumer 2  Consumer 3
              │          │          │
              └──────────┼──────────┘
                         ▼
                  ┌─────────────┐
                  │  Cassandra  │
                  │   Cluster   │
                  └─────────────┘

Pub/Sub provides decoupling between producers and consumers, whileCassandra supports distributed storage and high-volume writes.

🧪 Testing

Run the test suite using:

pytest

Example test areas:

Producer message creation

Pub/Sub publishing

Message validation

Consumer processing

Cassandra insertion

Error handling

Invalid payload handling

📊 Observability

For a production-ready implementation, monitor:

Metric               Description

Message throughput   Events processed per secondProcessing latency   Time from message receipt to Cassandra writeError rate           Failed message processing percentageRetry count          Number of retried messagesConsumer health      Subscriber availabilityCassandra latency    Database write/read latencyBacklog              Unprocessed Pub/Sub messages

🚀 Future Enhancements

Potential improvements include:

Add FastAPI REST endpoints.

Add Google Cloud Monitoring integration.

Add structured JSON logging.

Add Pub/Sub dead-letter topic.

Add schema validation using Pydantic.

Add Redis caching.

Add CI/CD using GitHub Actions.

Deploy consumer services to Google Cloud Run or GKE.

Add automated integration tests.

Add data quality checks.

Add OpenTelemetry-based distributed tracing.

Add dashboarding with Grafana/Cloud Monitoring.

Implement event schema versioning.

💼 Business Use Cases

This architecture can be adapted for:

E-commerce order processing

Payment transaction processing

Retail sales analytics

Financial transaction ingestion

Inventory updates

Customer activity tracking

Logistics and shipment events

Real-time operational dashboards

🎓 Key Learning Outcomes

Through this project, you can demonstrate practical knowledge of:

Event-driven architecture

Real-time data ingestion

Google Cloud Pub/Sub

Python asynchronous processing

NoSQL database design

Apache Cassandra

Docker containerization

Message acknowledgement and retries

Error handling

Idempotent processing

Distributed systems

Scalable data pipelines

📌 Interview Explanation

Short version:

"I built a real-time Sales Order and Payment data ingestion pipelineusing Python, Google Cloud Pub/Sub, Docker, and Cassandra. Order andpayment events are published to a Pub/Sub topic. A Python consumersubscribes to the topic, validates and transforms the incoming events,and persists them into Cassandra. After successful persistence, themessage is acknowledged. For failures, the pipeline supports retry anderror-handling mechanisms. The architecture is event-driven,decoupled, horizontally scalable, and suitable for high-volumereal-time data processing."

👨‍💻 Author

Your Name

GitHub: https://github.com/<YOUR_USERNAME>

LinkedIn: https://www.linkedin.com/in/<YOUR_LINKEDIN_USERNAME>/

⭐ Support

If you find this project useful, consider giving the repository a ⭐ onGitHub.
