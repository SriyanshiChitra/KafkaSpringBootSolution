# 🚀 Kafka Batch Service

A **Spring Boot microservice** for reliable Kafka batch processing with **at-least-once delivery**, idempotent PostgreSQL persistence, dead-letter queue (DLQ) handling, and secured REST APIs.

## 🛠️ Tech Stack

**Java 21** · **Spring Boot 4** · **Apache Kafka** · **PostgreSQL** · **Flyway** · **Spring Security** · **Docker** · **Kubernetes** · **Testcontainers** · **JaCoCo**

## ✨ Key Features

- **Batch Processing:** Kafka batch consumer with manual acknowledgment.
- **At-Least-Once Delivery:** Messages acknowledged after successful processing.
- **Idempotency:** Prevents duplicate records using PostgreSQL `ON CONFLICT`.
- **Error Handling:** Automatic retries and dead-letter queue (`messages.DLQ`).
- **API Security:** HTTP Basic authentication with role-based access.
- **Health Monitoring:** Kubernetes liveness and readiness probes.
- **Testing:** Unit and integration tests using Mockito and Testcontainers.

## 🏗️ Architecture

```text
REST API → Kafka Producer → Kafka Topic
                               |
                               ▼
                         Batch Consumer
                               |
                     ┌─────────┴─────────┐
                     ▼                   ▼
                PostgreSQL         Error Handler
                (Idempotent)            |
                                        ▼
                                  Retry → DLQ
```

## 🚀 Quick Start


### Step 1: Start Docker Containers

```bash
docker compose -f infra.yml up -d
```

### Step 2: Create the Kafka Topic

```bash
docker compose -f infra.yml exec broker \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic messages \
  --partitions 3 \
  --replication-factor 1
```


another topic for DLQ 
```bash
docker compose -f infra.yml exec broker \
  /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create \
  --if-not-exists \
  --topic messages.DLQ \
  --partitions 3 \
  --replication-factor 1
```



### Step 4: Start Spring Boot

```bash
./mvnw spring-boot:run
```

The application can now publish and consume messages using the `messages` Kafka topic.
**1. Start Kafka and PostgreSQL**

```bash
docker compose -f infra.yml up -d
```

**2. Run Spring Boot**

```bash
./mvnw spring-boot:run
```

Application: `http://localhost:8080`

## 📡 API Endpoints

| Method | Path | Auth | Response |
|---|---|---|---|
| POST | `/api/messages` | TESTER | 202 + receipt `{id,topic,partition,offset}` |
| GET | `/api/messages` | TESTER | 200 + message list |
| GET | `/api/messages/{id}` | TESTER | 200 / 404 |
| GET | `/actuator/health` | None | 200 / 503 |
| GET | `/actuator/health/liveness` | None | Process alive |
| GET | `/actuator/health/readiness` | None | Kafka + DB reachable |

### API Examples

**Publish a message**

```bash
curl -u admin:admin -X POST localhost:8080/api/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"550e8400-e29b-41d4-a716-446655440000",
    "type":"TEST",
    "description":"Sample message",
    "payload":"Hello Kafka",
    "createdBy":"me",
    "createdAt":"2026-10-26T12:00:00Z"
  }'
```

**Retrieve messages**

```bash
curl -u admin:admin localhost:8080/api/messages
```

**Check application health**

```bash
curl localhost:8080/actuator/health
```

**Authentication:** HTTP Basic

- Username: `admin`
- Password: `admin`
- Role: `TESTER` (in-memory user)

*Credentials are for local development only.*

## 🧪 Tests & Coverage

```bash
./mvnw verify
```

- **Unit Tests:** Mockito — batch processing, deduplication, REST controller.
- **Integration Tests:** Testcontainers with real Kafka and PostgreSQL.
- **Code Coverage:** JaCoCo report at `target/site/jacoco/index.html`.

## ☸️ Kubernetes Deployment (kind)
* This project is build in MACOS. 
**Prerequisites:** Docker Desktop, kind, kubectl.

```bash
# 1. Start infrastructure
docker compose -f infra.yml up -d

# 2. Build Docker image
docker build -t kafka-batch-service:latest .

# 3. Create cluster and load image
kind create cluster
kind load docker-image kafka-batch-service:latest

# 4. Deploy application
kubectl apply -f k8s/
kubectl get pods

# 5. Forward application port
kubectl port-forward svc/kafka-batch-service 9091:8080
```

**Test Kubernetes deployment (new terminal)**

```bash
curl -u admin:admin -X POST localhost:9091/api/messages \
  -H 'Content-Type: application/json' \
  -d '{
    "id":"550e8400-e29b-41d4-a716-446655440000",
    "type":"TEST",
    "description":"k8s",
    "payload":"Hello from pod",
    "createdBy":"curl",
    "createdAt":"2026-10-26T14:00:00Z"
  }'

curl -u admin:admin localhost:9091/api/messages

curl localhost:9091/actuator/health/readiness
```

### Deployment Notes

- **Image:** `imagePullPolicy: Never` for local kind deployment.
- **Networking:** Pods connect to Docker Compose Kafka/PostgreSQL via `host.docker.internal`.
- **Kafka:** Dedicated `DOCKERHOST` listener on port `29094`.
- **Image Updates:** Reload the image and restart the pod, or use a new image tag.
- **Production:** Use a container registry, versioned images, and managed Kafka/PostgreSQL services.

## ⚙️ Configuration

| Property / Environment Variable | Default | Purpose |
|---|---|---|
| `spring.kafka.bootstrap-servers` / `SPRING_KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | Kafka broker |
| `app.kafka.topic.name` | `messages` | Main topic |
| `app.kafka.dlq-topic.name` | `messages.DLQ` | Dead-letter topic |
| `spring.datasource.url` / `SPRING_DATASOURCE_URL` | `jdbc:postgresql://localhost:5432/messages` | Database |
| `spring.datasource.password` / `SPRING_DATASOURCE_PASSWORD` | — | DB password |
| `management.endpoint.health.probes.enabled` | `true` | Kubernetes probes |

Kubernetes configuration is injected using the **`app-config` ConfigMap** and **`app-secrets` Secret** (`k8s/deployment.yml`).

## 🔑 Design Decisions

| Component | Implementation |
|---|---|
| **Batch Consumer** | Manual acknowledgment after successful batch persistence |
| **Delivery Guarantee** | At-least-once processing |
| **Idempotency** | `INSERT ... ON CONFLICT (id) DO NOTHING`; first write wins |
| **Duplicate Handling** | Identical messages ignored; conflicting content retried and sent to DLQ |
| **Error Handling** | `DefaultErrorHandler` + `DeadLetterPublishingRecoverer` + `FixedBackOff` |
| **Async Processing** | HTTP 202 confirms Kafka acknowledgment, not consumption |
| **Health Probes** | Liveness checks process; readiness checks Kafka + PostgreSQL |
| **Security** | `/error` permitted to preserve actual 5xx responses |

## 💻 Windows Notes

- Use `.\mvnw.cmd` instead of `./mvnw`.
- Use `curl.exe` in PowerShell.
- `host.docker.internal` is supported by Docker Desktop for Windows.

---

**Built with Java 21, Spring Boot 4, Apache Kafka, PostgreSQL, Docker & Kubernetes.**
