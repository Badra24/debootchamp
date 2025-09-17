# ETL Batch Pipeline with Apache Airflow

## 📌 Project Overview
Project ini adalah implementasi **ETL batch pipeline** menggunakan **Apache Airflow** sebagai workflow orchestrator.  
Pipeline ini dirancang dengan pendekatan **dimensional design** untuk mendukung analitik berbasis data warehouse, dengan pemisahan layer data menjadi **Bronze, Silver, dan Gold schema**.

---

## 🛠️ Tech Stack
- **Apache Airflow** → Orchestration & scheduling DAGs  
- **Python** → ETL processing  
- **PostgreSQL / Data Warehouse** → Target penyimpanan dimensional  
- **Docker & docker-compose** → Deployment environment  
- **Grafana** → Monitoring dan visualization (opsional)

---

## 📂 Data Architecture

### 🔹 Bronze Schema
- Menyimpan **raw data** hasil extract dari berbagai sumber (CSV, API, database OLTP).
- Data masih dalam bentuk original tanpa banyak transformasi.
- Tujuan: arsip data mentah untuk kebutuhan traceability.

### 🔸 Silver Schema
- Menyimpan **cleaned & conformed data** hasil proses transformasi.
- Dilakukan data cleansing, standarisasi, penggabungan dari berbagai source.
- Data sudah lebih rapi untuk kebutuhan analisis lanjutan.

### 🟡 Gold Schema
- Menyimpan **dimensional data model** (fact & dimension tables).
- Data siap digunakan untuk reporting, dashboard BI, atau advanced analytics.
- Menggunakan **star schema / snowflake schema** sesuai kebutuhan.

---

## ⚙️ Airflow DAGs
Pipeline ini terdiri dari beberapa DAGs utama:

1. **Extract DAG**  
   - Mengambil data dari berbagai sumber.  
   - Load ke **Bronze schema**.  

2. **Transform DAG**  
   - Proses data cleansing, deduplication, dan standardization.  
   - Simpan ke **Silver schema**.  

3. **Load DAG**  
   - Bangun **dimensional model** (fact & dimension tables).  
   - Simpan ke **Gold schema** untuk konsumsi BI/analytics.  

---

## 🚀 How to Run

1. **Clone repository**  
   ```bash
   git clone https://github.com/Badra24/debootchamp.git
   cd debootchamp
