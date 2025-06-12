
# 📊 Real-Time Retail Analytics Pipeline

## 🚀 Built by a Data Engineer for streaming, transforming, and visualizing point-of-sale transactions in real-time.

---

### 🧠 Project Summary

This project demonstrates an end-to-end real-time data pipeline using:

- Kafka for ingesting simulated transaction events
- Apache Spark Structured Streaming for transformation and enrichment
- PostgreSQL for storing enriched fact data
- Metabase for real-time analytics and dashboards
- Docker Compose for orchestration

---

### 📦 Stack

| Component         | Purpose                                 |
|------------------|-----------------------------------------|
| **Kafka**        | Message queue for transaction events    |
| **Zookeeper**    | Coordination service for Kafka          |
| **Spark**        | Streaming transformations and joins     |
| **PostgreSQL**   | Stores dimension and fact tables        |
| **Metabase**     | Visualization layer                     |
| **Jupyter Notebook** | Development and execution of Spark jobs |
| **POS Simulator**| Sends random transaction events to Kafka|

---

### 📁 Directory Structure

```
.
├── docker-compose.yml
├── notebooks/
│   └── spark_streaming_etl.ipynb     # Spark streaming logic
├── pos-simulator/
│   └── Dockerfile.pos_simulator.dockerfile
├── jars/                             # Optional: Additional Spark JARs
├── metabase_data/                    # Volume for Metabase
└── README.md                         # You're here
```

---

### 🛠️ Setup Instructions

> Ensure Docker & Docker Compose are installed.

```bash
# Step 1: Clone the repo
git clone https://github.com/your-username/retail-streaming-pipeline.git
cd retail-streaming-pipeline

# Step 2: Start the pipeline
docker-compose up --build
```

---

### 🔄 Streaming Workflow

1. **Kafka Topic:** `transactions`
2. **Producer:** A POS simulator sends transaction JSON messages to Kafka.
3. **Spark Notebook**:
   - Reads messages from Kafka
   - Reads dimension tables (`dim_products`, `dim_location`) from PostgreSQL
   - Filters and transforms data
   - Writes valid transactions to the `facts_transaction` table
   - Writes invalid (bad) data to a JSON directory and optionally to Postgres
4. **Metabase:** Visualizes real-time transactions with auto-refresh enabled.

---

### 📊 Metabase Dashboard

> Access: [http://localhost:3000](http://localhost:3000)

- Default user: `admin@metabase.com`
- Password: `admin123` *(or your custom)*

You can build dashboards to show:
- Transactions per product
- Revenue per store/city
- Payment method distribution
- Streaming data with 1-minute refresh

---

### 🔧 Customization

- Kafka topic configurable via env: `KAFKA_BROKER=kafka:9092`
- PostgreSQL creds: see `docker-compose.yml`
- Spark JARs configured in `spark-notebook` env vars.

---

### 📌 Example SQL: `facts_transaction`

```sql
SELECT 
  transaction_id,
  product_id,
  name AS product_name,
  category AS product_category,
  store_id,
  city,
  state,
  country,
  quantity,
  price AS product_price,
  payment_method,
  transaction_timestamp
FROM facts_transaction
ORDER BY transaction_timestamp DESC
LIMIT 10;
```

---

### 🧪 Sample Kafka Message

```json
{
  "transaction_id": "a1b2c3d4-5678-1234-9876-a1b2c3d4e5f6",
  "product_id": 101,
  "store_id": 10,
  "quantity": 2,
  "price": 15.5,
  "payment_method": "CASH",
  "timestamp": "2024-06-10T14:33:45Z"
}
```

---

### 📌 Future Improvements

- Schema Registry & Kafka Avro support
- Delta Lake for immutable fact tables
- Airflow DAG for orchestration
- CI/CD for notebooks and data validation

---

### 👨‍💻 Author

**Prakhar Durag**  
_Data Engineer | Streaming Specialist | Python & AWS Enthusiast_  
[LinkedIn](https://www.linkedin.com/in/prakhar-durag-ba9743199/) | [GitHub](https://github.com/prakhardurag)
