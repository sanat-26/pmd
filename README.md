# pmd : A Project Manager's Dashboard
---

# Event-Driven Telemetry & Real-Time Funnel Analytics Pipeline

A high-throughput event-driven telemetry pipeline designed to capture, stream, process, and visualize user behavioral analytics and checkout funnel drop-off rates in real-time.

This repository implements a decoupled architecture capable of tracking user actions seamlessly as they progress through multi-step conversion funnels, ensuring business visibility with minimal performance overhead on client-facing applications.

---

## Architectural Overview

Modern consumer applications require instant insight into how users navigate checkout paths. Relying on batch processing or synchronous database logs introduces latency and performance bottlenecks. This project solves that by utilizing an **Event-Driven Telemetry Blueprint**:

```text
[ UI Trigger ] ──(User Actions)──> [ Spring Boot Producer ]
                                              │
                                   (Kafka Event Stream)
                                              ▼
                                     [ Kafka Cluster (VM) ]
                                              │
                                      (Event Ingestion)
                                              ▼
[ Grafana Dashboard ] <──(PromQL)── [ Prometheus ] <──(Scrape)── [ Consumer Service ]

```

### Component Breakdown

1. **UI Trigger (Client App):** A realistic mock storefront platform ("Streamclass") tracking a 3-step enrollment funnel:
* **Stage 1:** Click `ENROLL_NOW_CLICKED`
* **Stage 2:** Submit form data `USER_INFO_SUBMITTED`
* **Stage 3:** Click `BUY_NOW_CLICKED`


2. **Spring Boot Producer:** Receives synchronous HTTP triggers from the client layer, translates them into structured telemetry event payloads, and asynchronously dispatches them to an Apache Kafka broker.
3. **Kafka Cluster (Hosted on VM):** Serves as the immutable, highly available commit log that decouples the data producers from the data consumers.
4. **Consumer Service:** Subscribes to the respective Kafka telemetry topics, aggregates raw streaming events into meaningful timeseries counter variables, and exposes a Prometheus-compatible metrics scraping endpoint.
5. **Prometheus Server (Hosted on VM):** Frequently scrapes the metrics engine exposed by the consumer service at a defined interval, storing timeseries data.
6. **Grafana Dashboard (Hosted on VM):** Connects to Prometheus as a data source to plot real-time horizontal conversion bars and compute immediate operational drop-off rates.

---

## Visual Walkthrough

### 1. User Conversion Funnel (UI)

The conversion funnel monitors prospective users across critical registration boundaries.

![Streamclass Landing Page - Core Telemetry Pipeline Entrypoint]<img width="1920" height="1200" alt="Screenshot (105)" src="https://github.com/user-attachments/assets/125f84b1-5f86-4851-986d-2fd6460beaf6" />

![Funnel Step 1 - User Account Profiling and Information Submission]<img width="1920" height="1200" alt="Screenshot (104)" src="https://github.com/user-attachments/assets/b976fa06-ac77-4a41-a188-402addb79ef0" />

![Funnel Step 2 - Transaction Checkout and Conversion Completion]<img width="1920" height="1200" alt="Screenshot (103)" src="https://github.com/user-attachments/assets/619a60f3-2045-4774-aba4-1eb19f7a666e" />

### 2. Monitoring & Metrics Scraping

Prometheus monitors target health and stores structural telemetry counters containing labels for each individual stage of the user journey.

![Prometheus Targets Status Showing Connected Active Scraping Endpoints]<img width="1920" height="1200" alt="Screenshot (107)" src="https://github.com/user-attachments/assets/aafa8e1f-35cd-486f-8abd-6b17ff3c6f42" />

![Prometheus Multi-series Stacked Area Graph Charting Total Checkout Funnel Events]<img width="1920" height="1200" alt="Screenshot (110)" src="https://github.com/user-attachments/assets/5ee229eb-ce1a-4b77-a9af-fc46d833366d" />

![Prometheus Unstacked Line Graphs Highlighting Incremental Telemetry Growth]<img width="1920" height="1200" alt="Screenshot (111)" src="https://github.com/user-attachments/assets/9a246075-e646-431b-bc56-3e0d40bd2f6a" />

### 3. Real-Time Insights Dashboard

Grafana visualizes volumetric user ingestion patterns along with a live calculation of the funnel drop-off rate.

![Grafana Live Dashboard Tracking User Action Frequencies and Conversion Drop-off Percentages]<img width="1920" height="1200" alt="Screenshot (113)" src="https://github.com/user-attachments/assets/1f98bce2-22af-4b1e-bec6-d024e1631114" />

---

## 🛠️ Telemetry Metrics & Math

The application exposes a custom multidimensional counter metric called `checkout_funnel_events_total`. It contains specific labels partitioned by the application's step sequence:

* `checkout_funnel_events_total{step="ENROLL_NOW_CLICKED"}`
* `checkout_funnel_events_total{step="USER_INFO_SUBMITTED"}`
* `checkout_funnel_events_total{step="BUY_NOW_CLICKED"}`

### Drop-Off Rate Calculation

The system tracks drop-offs between critical operational thresholds (e.g., users who provided information but abandoned the final transaction stage) using the following formula:

$$\text{Drop-Off Rate (\%)} = \left( 1 - \frac{\text{BUY\_NOW\_CLICKED}}{\text{USER\_INFO\_SUBMITTED}} \right) \times 100$$

This allows product stakeholders to recognize drop-off anomalies immediately as code deployments or system changes go live.

---

## ⚙️ Setup & Installation Guide

Follow these sequential steps to set up the infrastructure components and launch the application services.

### Prerequisites

* Java 17+ installed locally
* Maven installed locally
* Access to a Linux Virtual Machine (Ubuntu/Debian recommended) with ports `9092` (Kafka), `9090` (Prometheus), and `3000` (Grafana) exposed.

---

### Step 1: Set Up the Virtual Machine Infrastructure

SSH into your cloud or local Virtual Machine to install and start the streaming and monitoring services.

#### A. Run Apache Kafka Broker

Ensure Java is installed on your VM, then extract and spin up Zookeeper and Kafka:

```bash
# Download and extract Apache Kafka
wget https://archive.apache.org/dist/kafka/3.5.1/kafka_2.13-3.5.1.tgz
tar -xzf kafka_2.13-3.5.1.tgz
cd kafka_2.13-3.5.1

# Start Kafka (Background Service)
su -l kafka
cd kafka

# Configure Kafka listener to bind to your VM external/internal IP address
# Edit config/server.properties and update: listeners=PLAINTEXT://0.0.0.0:9092

# Start KRaft
bin/kafka-storage.sh random-uuid
bin/kafka-storage.sh format -t <ID_GENERATED> -c config/server.properties --standalone

# Start Apache Kafka Broker
bin/kafka-server-start.sh config/server.properties
```

#### B. Run Prometheus Server

Download Prometheus and configure it to target your Consumer application:

```bash
# Download Prometheus
wget https://github.com/prometheus/prometheus/releases/download/v2.45.0/prometheus-2.45.0.linux-amd64.tar.gz
tar -xzf prometheus-2.45.0.linux-amd64.tar.gz
cd prometheus-2.45.0.linux-amd64
```

Edit your configuration file (`prometheus.yml`) to add your Consumer endpoint under the scrape configurations:

```yaml
scrape_configs:
  - job_name: 'prometheus'
    static_configs:
      - targets: ['localhost:9090']

  - job_name: 'telemetry-consumer'
    metrics_path: '/actuator/prometheus'
    scrape_interval: 2s
    static_configs:
      - targets: ['<YOUR_LOCAL_CONSUMER_IP>:<CONSUMER_PORT>']
```

Start the Prometheus service:

```bash
sudo systemctl start prometheus
```

#### C. Run Grafana Server

Install Grafana on your virtual machine instance:

```bash
sudo apt-get install -y apt-transport-https software-properties-common wget
sudo mkdir -p /etc/apt/keyrings/
wget -q -O - https://apt.grafana.com/gpg.key | gpg --dearmor | sudo tee /etc/apt/keyrings/grafana.gpg > /dev/null

echo "deb [signed-by=/etc/apt/keyrings/grafana.gpg] https://apt.grafana.com stable main" | sudo tee /etc/apt/sources.list.pnd/grafana.list

sudo apt-get update
sudo apt-get install grafana

# Enable and start the background system service
sudo systemctl daemon-reload
sudo systemctl start grafana-server
sudo systemctl enable grafana-server
```

---

### Step 2: Download & Configure the Telemetry Producer

Clone the source component containing your Spring Boot Event Producer onto your local machine or development server.

```bash
# Clone and navigate to the producer package directory
git clone https://github.com/sanat-26/pmd.git
cd pmd/producer # Adjust based on your explicit folder routing

```

Open `src/main/resources/application.properties` (or `yaml`) and point the application toward your virtual machine's Kafka broker:

```properties
spring.kafka.producer.bootstrap-servers=<YOUR_VM_IP_ADDRESS>:9092
spring.kafka.producer.key-serializer=org.apache.kafka.common.serialization.StringSerializer
spring.kafka.producer.value-serializer=org.apache.kafka.common.serialization.StringSerializer
server.port=8081

```

Compile and execute the Spring Boot microservice:

```bash
mvn clean install
mvn spring-boot:run

```

---

### Step 3: Download & Run the Telemetry Consumer

Open a separate terminal window to clone, configure, and start the processing consumer service.

```bash
# Navigate to consumer directory package
cd pmd/consumer

```

Configure your consumer's properties to listen to the exact same Kafka topic instance and expose its application metrics server port:

```properties
spring.kafka.consumer.bootstrap-servers=<YOUR_VM_IP_ADDRESS>:9092
spring.kafka.consumer.group-id=telemetry-analytics-group
server.port=8082

```

Run the application:

```bash
mvn clean install
mvn spring-boot:run

```

---

### Step 4: Connecting the Data Visualization Layer

1. **Access Grafana UI:** Open your browser and navigate to `http://<YOUR_VM_IP_ADDRESS>:3000` (Default credentials: `admin` / `admin`).
2. **Add Prometheus Data Source:** * Navigate to **Connections** -> **Data Sources** -> **Add Data Source**.
* Choose **Prometheus**.
* Set the Server URL to pointing directly to your VM's local host instance: `http://localhost:9090`.
* Click **Save & Test**.


3. **Build the Conversion Dashboard:**
* Create a new Dashboard panel.
* Use PromQL queries to display your metrics:
```promql
# Query to extract funnel counts
checkout_funnel_events_total
```


* Configure Bar Gauge visualizations to monitor real-time pipeline activity.
