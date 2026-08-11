# Sales Order & Payment Data — Real-Time Ingestion

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.9%2B-3776AB?style=for-the-badge&logo=python&logoColor=white" alt="Python"/>
  <img src="https://img.shields.io/badge/GCP-Pub%2FSub-4285F4?style=for-the-badge&logo=googlecloud&logoColor=white" alt="GCP Pub/Sub"/>
  <img src="https://img.shields.io/badge/Cassandra-NoSQL-1287B1?style=for-the-badge&logo=apachecassandra&logoColor=white" alt="Cassandra"/>
  <img src="https://img.shields.io/badge/Docker-Containerized-2496ED?style=for-the-badge&logo=docker&logoColor=white" alt="Docker"/>
</p>

<p align="center">
  <b> Real-Time Sales Order & Payment Data Ingestion Pipeline</b>
</p>

<p align="center">
  An event-driven data ingestion system that receives order and payment events through
  <b>Google Cloud Pub/Sub</b>, processes them with <b>Python</b>, and persists them in
  <b>Apache Cassandra</b>.
</p>

---

## Project at a Glance

| Item | Details |
|---|---|
| **Project** | Sales Order & Payment Data — Real-Time Ingestion |
| **Domain** | E-Commerce / Payments / Real-Time Data Engineering |
| **Architecture** | Event-Driven / Asynchronous |
| **Input** | Sales Order & Payment Events |
| **Message Broker** | Google Cloud Pub/Sub |
| **Processing** | Python |
| **Database** | Apache Cassandra |
| **Containerization** | Docker |
| **Data Format** | JSON |
| **Processing Mode** | Real-Time / Streaming |

# Order Payment Data Real Time Ingestion

<img width="977" height="503" alt="image" src="https://github.com/user-attachments/assets/7d39bd63-3e8f-48bb-be9a-5e7cef3420b7" />

---

## 🎯 Project Objective

The objective of this project is to build a **real-time, scalable and reliable data ingestion pipeline** for sales order and payment events.

Instead of waiting for batch jobs, every order/payment event is published to **Google Cloud Pub/Sub** and processed immediately by a Python consumer.

The processed data is then stored in **Apache Cassandra**, which is suitable for high-volume distributed writes and horizontally scalable workloads.

### Business Flow

```text
Customer places an order
          │
          ▼
Payment is processed
          │
          ▼
Order / Payment Event Generated
          │
          ▼
Google Cloud Pub/Sub
          │
          ▼
Python Consumer
          │
    ┌─────┴─────┐
    │           │
 Validate     Transform
    │           │
    └─────┬─────┘
          ▼
      Cassandra
          │
          ▼
 Real-Time Order & Payment Data
```

---

# 🏗️ System Architecture

```mermaid
flowchart LR

    A[Sales / Payment Source] --> B[Python Producer]

    B --> C[GCP Pub/Sub Topic]

    C --> D[GCP Pub/Sub Subscription]

    D --> E[Python Consumer]

    E --> F[Validate Event]

    F --> G[Transform / Normalize]

    G --> H{Cassandra Write}

    H -->|Success| I[ACK Pub/Sub Message]
    H -->|Failure| J[Retry / NACK]

    J --> D

    H --> K[(Apache Cassandra)]

    K --> L[Orders & Payment Data]

    style A fill:#E8F1FF,stroke:#4285F4
    style B fill:#E8F1FF,stroke:#4285F4
    style C fill:#FFF3CD,stroke:#F9AB00
    style D fill:#FFF3CD,stroke:#F9AB00
    style E fill:#E8F1FF,stroke:#4285F4
    style F fill:#E8F1FF,stroke:#4285F4
    style G fill:#E8F1FF,stroke:#4285F4
    style H fill:#FCE8E6,stroke:#EA4335
    style K fill:#E6F4EA,stroke:#34A853
    style L fill:#E6F4EA,stroke:#34A853
```

### 🔄 End-to-End Data Flow

```text
┌──────────────────────┐
│  Sales / Payment     │
│       Source         │
└──────────┬───────────┘
           │
           │ JSON Event
           ▼
┌──────────────────────┐
│  Python Producer     │
│                      │
│  Create Event        │
│  Serialize JSON      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Google Cloud       │
│      Pub/Sub         │
│                      │
│      Topic           │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│   Subscription       │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│  Python Consumer     │
│                      │
│  1. Receive          │
│  2. Validate         │
│  3. Transform        │
│  4. Persist          │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      Cassandra       │
│      NoSQL DB        │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Orders & Payments    │
│ Real-Time Data       │
└──────────────────────┘
```

---

# 🛠️ Tech Stack

| Technology | Role in Project | Why It Is Used |
|:---:|---|---|
| 🐍 **Python** | Producer & Consumer | Event processing, validation and transformation |
| ☁️ **GCP Pub/Sub** | Message Broker | Real-time asynchronous event ingestion |
| 🗄️ **Apache Cassandra** | NoSQL Database | Distributed storage and high write throughput |
| 🐳 **Docker** | Containerization | Consistent and portable deployment |
| 📦 **JSON** | Event Format | Lightweight structured event payload |
| 🧪 **Pytest** | Testing | Unit and integration testing |
| 🔧 **Git/GitHub** | Version Control | Source-code management and collaboration |

---

# 📂 Project Structure

```text
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
```

---

# 📩 Sample Order & Payment Event

```json
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
```

---

# 🔄 Processing Pipeline

### Step 1 — Event Creation

The application creates an order/payment JSON event.

### Step 2 — Publish to Pub/Sub

The Python producer publishes the event to the Pub/Sub topic.

### Step 3 — Consume Event

The Python consumer listens to the Pub/Sub subscription.

### Step 4 — Validate

The consumer checks:

- Required fields
- Data types
- Order ID
- Payment status
- Amount
- Timestamp
- Payload format

### Step 5 — Transform

The event is normalized into the format required by Cassandra.

### Step 6 — Store

The validated event is inserted into Cassandra.

### Step 7 — Acknowledge

Only after successful persistence, the Pub/Sub message is acknowledged.

```text
Receive
   │
   ▼
Validate
   │
   ▼
Transform
   │
   ▼
Cassandra Write
   │
 ┌─┴──────────┐
 │            │
Success     Failure
 │            │
 ▼            ▼
ACK        NACK / Retry
```

---

# 🗄️ Cassandra Data Model

### Keyspace

```sql
CREATE KEYSPACE order_payment
WITH replication = {
    'class': 'SimpleStrategy',
    'replication_factor': 1
};
```

### Orders Table

```sql
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
```

> **Production note:** For a multi-node production Cassandra cluster, use `NetworkTopologyStrategy` and design partition/clustering keys according to actual query patterns and expected data volume.

---

# 🐍 Python Producer

Example publishing logic:

```python
from google.cloud import pubsub_v1
import json

publisher = pubsub_v1.PublisherClient()

project_id = "YOUR_GCP_PROJECT_ID"
topic_id = "order-payment-topic"

topic_path = publisher.topic_path(
    project_id,
    topic_id
)

event = {
    "order_id": "ORD10001",
    "amount": 2499.99,
    "payment_status": "SUCCESS"
}

data = json.dumps(event).encode("utf-8")

future = publisher.publish(topic_path, data)

print(f"Published message ID: {future.result()}")
```

---

# 📥 Python Consumer

Example subscriber:

```python
from google.cloud import pubsub_v1

subscriber = pubsub_v1.SubscriberClient()

project_id = "YOUR_GCP_PROJECT_ID"
subscription_id = "order-payment-subscription"

subscription_path = subscriber.subscription_path(
    project_id,
    subscription_id
)


def callback(message):

    try:
        data = message.data.decode("utf-8")

        # 1. Validate
        # 2. Transform
        # 3. Write to Cassandra

        message.ack()

    except Exception as error:

        print(f"Processing failed: {error}")

        message.nack()


streaming_pull_future = subscriber.subscribe(
    subscription_path,
    callback=callback
)

streaming_pull_future.result()
```

---

# ☁️ GCP Pub/Sub Setup

### Create Topic

```bash
gcloud pubsub topics create order-payment-topic
```

### Create Subscription

```bash
gcloud pubsub subscriptions create order-payment-subscription \
    --topic=order-payment-topic
```

### Publish Test Message

```bash
gcloud pubsub topics publish order-payment-topic \
    --message='{"order_id":"ORD10001","amount":2499.99,"payment_status":"SUCCESS"}'
```

### Pull Message

```bash
gcloud pubsub subscriptions pull order-payment-subscription \
    --auto-ack
```

---

# 🐳 Docker

### Dockerfile

```dockerfile
FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD ["python", "-m", "consumer.subscriber"]
```

### Build

```bash
docker build -t order-payment-ingestion .
```

### Run

```bash
docker run --env-file .env order-payment-ingestion
```

---

# ⚙️ Environment Configuration

Create a `.env` file:

```env
GCP_PROJECT_ID=your-gcp-project-id

PUBSUB_TOPIC=order-payment-topic

PUBSUB_SUBSCRIPTION=order-payment-subscription

CASSANDRA_HOST=localhost

CASSANDRA_PORT=9042

CASSANDRA_KEYSPACE=order_payment

CASSANDRA_TABLE=orders
```

### 🔐 Security

Never commit:

```text
.env
*.json
service-account.json
private keys
passwords
API keys
```

Use Google Cloud IAM and a secure secret-management solution for production credentials.

---

# 📦 Installation

### 1. Clone Repository

```bash
git clone https://github.com/<YOUR_USERNAME>/order-payment-realtime-ingestion.git

cd order-payment-realtime-ingestion
```

### 2. Create Virtual Environment

```bash
python -m venv venv
```

### 3. Activate Environment

**Windows**

```bash
venv\Scripts\activate
```

**Linux / macOS**

```bash
source venv/bin/activate
```

### 4. Install Dependencies

```bash
pip install -r requirements.txt
```

---

# ▶️ Run the Application

### Start Cassandra

```bash
docker-compose up -d cassandra
```

### Start Consumer

```bash
python -m consumer.subscriber
```

### Start Producer

Open another terminal:

```bash
python -m producer.publisher
```

---

# 🔁 Reliability & Error Handling

The system is designed to avoid losing messages during processing failures.

```mermaid
flowchart TD

    A[Receive Pub/Sub Message] --> B{Valid Payload?}

    B -->|Yes| C[Transform Data]
    B -->|No| D[Log Validation Error]

    C --> E{Cassandra Write}

    E -->|Success| F[ACK Message]
    E -->|Failure| G[NACK / Retry]

    G --> H{Retry Limit Reached?}

    H -->|No| A
    H -->|Yes| I[Dead Letter Topic]

    style A fill:#E8F1FF
    style B fill:#FFF3CD
    style C fill:#E8F1FF
    style E fill:#FCE8E6
    style F fill:#E6F4EA
    style G fill:#FCE8E6
    style I fill:#FCE8E6
```

### Recommended Production Features

- Pub/Sub retry policies
- Dead-letter topics
- Exponential backoff
- Idempotent processing
- Structured logging
- Cassandra replication
- Health checks
- Graceful shutdown
- Monitoring and alerting

---

# 📈 Scalability Architecture

```mermaid
flowchart LR

    A[Multiple Producers] --> B[Pub/Sub Topic]

    B --> C[Subscription]

    C --> D1[Consumer 1]
    C --> D2[Consumer 2]
    C --> D3[Consumer N]

    D1 --> E[(Cassandra Cluster)]
    D2 --> E
    D3 --> E

    E --> F[Real-Time Applications]

    style A fill:#E8F1FF
    style B fill:#FFF3CD
    style C fill:#FFF3CD
    style D1 fill:#E8F1FF
    style D2 fill:#E8F1FF
    style D3 fill:#E8F1FF
    style E fill:#E6F4EA
    style F fill:#E6F4EA
```

The architecture can scale by increasing consumer instances and Cassandra nodes based on workload requirements.

---

# 🧪 Testing

Run:

```bash
pytest
```

Test coverage should include:

| Test Area | Example |
|---|---|
| Producer | Event creation and publishing |
| Consumer | Message processing |
| Validation | Invalid/missing fields |
| Cassandra | Insert operation |
| Error Handling | Retry/NACK behavior |
| Integration | Pub/Sub → Consumer → Cassandra |

---

# 📊 Observability

Important production metrics:

| Metric | Purpose |
|---|---|
| Message Throughput | Events processed per second |
| Processing Latency | Pub/Sub → Cassandra processing time |
| Error Rate | Failed processing percentage |
| Retry Count | Number of retried messages |
| Pub/Sub Backlog | Unprocessed messages |
| Cassandra Latency | Database operation latency |
| Consumer Health | Consumer service availability |

---

# 🔐 Production Security

Recommended controls:

- Google Cloud IAM with least-privilege access
- Secure secret management
- TLS for database connections
- Private networking where applicable
- No credentials in source code
- Separate Dev / QA / Production environments
- Audit logging
- Encryption in transit and at rest

---

#  Future Enhancements

- [ ] FastAPI monitoring APIs
- [ ] Pydantic event validation
- [ ] Pub/Sub Dead Letter Topic
- [ ] GitHub Actions CI/CD
- [ ] Google Cloud Run deployment
- [ ] GKE deployment
- [ ] Google Cloud Monitoring
- [ ] Grafana dashboards
- [ ] OpenTelemetry tracing
- [ ] Event schema versioning
- [ ] Data quality monitoring
- [ ] Automated integration testing
- [ ] Horizontal consumer scaling

---

# 💼 Real-World Use Cases

This architecture can be adapted for:

- 🛒 E-commerce order processing
- 💳 Payment transaction processing
- 🏪 Retail sales ingestion
- 📦 Inventory updates
- 🚚 Logistics and shipment tracking
- 📊 Real-time operational dashboards
- 💰 Financial transaction ingestion
- 👤 Customer activity tracking

---

# 🎓 Key Skills Demonstrated

```text
Python
   │
   ├── Event Processing
   ├── JSON
   ├── Validation
   └── Error Handling

GCP Pub/Sub
   │
   ├── Topics
   ├── Subscriptions
   ├── ACK / NACK
   └── Retry

Cassandra
   │
   ├── NoSQL
   ├── Data Modeling
   ├── Distributed Storage
   └── High-Volume Writes

Docker
   │
   ├── Containerization
   └── Deployment
```


---


<img src="https://capsule-render.vercel.app/api?type=rect&color=gradient&customColorList=24,20,12,6&height=3" width="100%">


# 📞 **CONTACT & NETWORKING** 📞


## 💼 Professional Networks

[![LinkedIn](https://img.shields.io/badge/💼_LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ratneshkumar1998/)
[![GitHub](https://img.shields.io/badge/🐙_GitHub-181717?style=for-the-badge&logo=github&logoColor=white)](https://github.com/Ratnesh-181998)
[![X](https://img.shields.io/badge/X-000000?style=for-the-badge&logo=x&logoColor=white)](https://x.com/RatneshS16497)
[![Portfolio](https://img.shields.io/badge/🌐_Portfolio-FF6B6B?style=for-the-badge&logo=google-chrome&logoColor=white)](https://share.streamlit.io/user/ratnesh-181998)
[![Email](https://img.shields.io/badge/✉️_Email-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:rattudacsit2021gate@gmail.com)
[![Medium](https://img.shields.io/badge/Medium-000000?style=for-the-badge&logo=medium&logoColor=white)](https://medium.com/@rattudacsit2021gate)
[![Stack Overflow](https://img.shields.io/badge/Stack_Overflow-F58025?style=for-the-badge&logo=stack-overflow&logoColor=white)](https://stackoverflow.com/users/32068937/ratnesh-kumar)

## 🚀 AI/ML & Data Science  [AI/ML 1620+ Problem Solved](https://github.com/Ratnesh-181998/DSML)
[![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)](https://share.streamlit.io/user/ratnesh-181998)
[![HuggingFace](https://img.shields.io/badge/HuggingFace-FFD21E?style=for-the-badge&logo=huggingface&logoColor=black)](https://huggingface.co/RattuDa98)
[![Kaggle](https://img.shields.io/badge/Kaggle-20BEFF?style=for-the-badge&logo=kaggle&logoColor=white)](https://www.kaggle.com/rattuda)

## 💻 Competitive Programming [Including all coding plateform's 5000+ Problems/Questions solved](https://github.com/Ratnesh-181998/Algorithms-and-Data-Structures)
[![LeetCode](https://img.shields.io/badge/LeetCode-FFA116?style=for-the-badge&logo=leetcode&logoColor=black)](https://leetcode.com/u/Ratnesh_1998/)
[![HackerRank](https://img.shields.io/badge/HackerRank-00EA64?style=for-the-badge&logo=hackerrank&logoColor=black)](https://www.hackerrank.com/profile/rattudacsit20211)
[![CodeChef](https://img.shields.io/badge/CodeChef-5B4638?style=for-the-badge&logo=codechef&logoColor=white)](https://www.codechef.com/users/ratnesh_181998)
[![Codeforces](https://img.shields.io/badge/Codeforces-1F8ACB?style=for-the-badge&logo=codeforces&logoColor=white)](https://codeforces.com/profile/Ratnesh_181998)
[![GeeksforGeeks](https://img.shields.io/badge/GeeksforGeeks-2F8D46?style=for-the-badge&logo=geeksforgeeks&logoColor=white)](https://www.geeksforgeeks.org/profile/ratnesh1998)
[![HackerEarth](https://img.shields.io/badge/HackerEarth-323754?style=for-the-badge&logo=hackerearth&logoColor=white)](https://www.hackerearth.com/@ratnesh138/)
[![InterviewBit](https://img.shields.io/badge/InterviewBit-4285F4?style=for-the-badge&logo=google&logoColor=white)](https://www.interviewbit.com/profile/rattudacsit2021gate_d9a25bc44230/)


---

## 📊 **GitHub Stats & Metrics** 📊



![Profile Views](https://komarev.com/ghpvc/?username=Ratnesh-181998&color=blueviolet&style=for-the-badge&label=PROFILE+VIEWS)





<img 
  src="https://streak-stats.demolab.com?user=Ratnesh-181998&theme=radical&hide_border=true&background=0D1117&stroke=4ECDC4&ring=F38181&fire=FF6B6B&currStreakLabel=4ECDC4"
  alt="GitHub Streak Stats"
width="48%"/>



<img src="https://github-readme-activity-graph.vercel.app/graph?username=Ratnesh-181998&theme=react-dark&hide_border=true&bg_color=0D1117&color=4ECDC4&line=F38181&point=FF6B6B" width="48%" />

---

<img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&size=24&duration=3000&pause=1000&color=4ECDC4&center=true&vCenter=true&width=600&lines=Ratnesh+Kumar+Singh;Data+Scientist+%7C+AI%2FML+Engineer;4%2B+Years+Building+Production+AI+Systems" alt="Typing SVG" />

<img src="https://readme-typing-svg.herokuapp.com?font=Fira+Code&size=18&duration=2000&pause=1000&color=F38181&center=true&vCenter=true&width=600&lines=Built+with+passion+for+the+AI+Community+🚀;Innovating+the+Future+of+AI+%26+ML;MLOps+%7C+LLMOps+%7C+AIOps+%7C+GenAI+%7C+AgenticAI+Excellence" alt="Footer Typing SVG" />


<img src="https://capsule-render.vercel.app/api?type=waving&color=gradient&customColorList=6,11,20&height=120&section=footer" width="100%">
