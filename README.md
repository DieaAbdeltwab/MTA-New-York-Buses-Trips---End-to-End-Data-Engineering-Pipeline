
# 🚌 MTA New York Buses - End-to-End Data Engineering Pipeline

<div align="center">


[![Apache Airflow](https://img.shields.io/badge/Airflow-017CEE?style=for-the-badge&logo=apacheairflow&logoColor=white)](https://airflow.apache.org/)
[![Apache Kafka](https://img.shields.io/badge/Kafka-231F20?style=for-the-badge&logo=apachekafka&logoColor=white)](https://kafka.apache.org/)
[![Apache Spark](https://img.shields.io/badge/Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](https://spark.apache.org/)
[![Azure](https://img.shields.io/badge/Azure-0078d4?style=for-the-badge&logo=microsoftazure&logoColor=white)](https://azure.microsoft.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-336791?style=for-the-badge&logo=postgresql&logoColor=white)](https://postgresql.org/)

[![ClickHouse](https://img.shields.io/badge/ClickHouse-FFCC01?style=for-the-badge&logo=clickhouse&logoColor=black)](https://clickhouse.com/)
[![Power BI](https://img.shields.io/badge/Power%20BI-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](https://powerbi.microsoft.com/)
[![Metabase](https://img.shields.io/badge/Metabase-509EE3?style=for-the-badge&logo=metabase&logoColor=white)](https://metabase.com/)
[![Docker](https://img.shields.io/badge/Docker-2496ED?style=for-the-badge&logo=docker&logoColor=white)](https://www.docker.com/)


</div>

---
## 🏗️ Architecture  🔥
![Architecture](./images/pipeline_architecture.png)

---
<div align="center">

 **🔥 A comprehensive real-time data engineering solution for NYC MTA bus operations 🔥**  
 *🚀 Combining batch processing and streaming analytics to optimize transit performance and enhance passenger experience 🌟*

</div>

---


## 📈 **Business Impact** 💼

### **🎯 Key Business Benefits** 🔥

- **⚡ Improving Operational Efficiency**: Analyze delays 🕒, on-time performance ⏱️, and alerts 🚨 to identify underperforming routes. Better resource allocation 💰 for maintenance 🔧 and peak hour routing 🚦.

- **😊 Enhancing Passenger Experience**: Real-time vehicle tracking 🚌 and arrival predictions 🔮 reduce wait times ⏳ and increase rider satisfaction 🌟.

- **💰 Cost and Resource Optimization**: Monitor performance trends 📊 to minimize operational costs 💸 and optimize fleet usage 🚌⚡.

### **👥 Target Users** 🎯
<div align="center">

🏛️ **City planners and government officials**  
🚌 **MTA operators and management**  
👨‍👩‍👧‍👦 **Public transport users**  
🏢 **Transport agencies**  

</div>

---

## 📊 **Data Sources** 🔥

<div align="center">
<img src="https://media.giphy.com/media/l46Cy1rHbQ92uuLXa/giphy.gif" width="350"/>
</div>

### **📦 Batch Data Source** 
<div align="center">

🌐 **Transitland Website**: Batch files containing historical transit data for NYC regions 🗽

</div>

### **🌊 Streaming Data Sources** ⚡
<div align="center">

🚀 **GTFS Realtime APIs from Transitland** 📡

</div>

| API Endpoint | Update Frequency | Description |
|-------------|------------------|-------------|
| **🚌 Vehicle Position API** | ⏰ Every 60 seconds | 📍 Actual GPS positions of buses in motion |
| **🚇 Trip Updates API** | ⏰ Every 60 seconds | ⏱️ Estimated delays, arrival/departure per stop for running trips |
| **🚨 Service Alerts API** | 🔴 Real-time | ⚠️ Service disruptions (road closures, diversions, etc.) |

---


## 📦 **Batch Data Processing Pipeline** ⚡
<div align="center">
  <img src="https://media.giphy.com/media/qgQUggAC3Pfv687qPC/giphy.gif" width="400"/>
</div>

### **🕷️ Data Extraction Process** 
**🛠️ Airflow as the Orchestration Engine** - runs on a fixed schedule every day at 🕛 **12:00 AM (NYC time)** ⏰

#### **🕸️ Web Scraping Strategy**
- **⚠️ Challenge**: No API available for batch processing - only download button on website 🖱️
- **✅ Solution**: Automated web scraping to check latest available updates 🤖
- **🗺️ Coverage**: Table for each NYC region on the website 🌆

#### **🧠 Smart Update Detection** 💡
The most important aspect is the **📅 last update timestamp** which determines whether the batch pipeline should run or be skipped:

- **📝 JSON File Maintenance**: Stores last update date for each region 🗃️
- **🔍 Daily Comparison**: Website's latest update vs. stored date in JSON file ⚖️
- **🚫 Logic**: If dates match → no new update → skip pipeline (except final task)
- **✅ Logic**: If dates don't match → proceed with full workflow 🚀
---
## 🚫 skip pipeline
![Airflow 1](./images/Airflow1.png)
---
## ✅ proceed with full workflow 
![Airflow 2](./images/Airflow2.png)
---

### **📁 Data Processing Workflow** 🔥
When updates are detected, the workflow proceeds:

1. **🕷️ Scrape and Download**: Download data as ZIP file 📦
2. **📂 Unzip and Convert**: Extract .txt files and convert to .CSV 📊
3. **☁️ Upload to Azure Data Lake**: Upload with proper naming convention (region name, company name, upload date) 🏷️
4. **⚡ Spark Processing**: Airflow triggers Spark Jupyter notebook to extract from Azure Data Lake, apply transformations, and load into PostgreSQL staging database 🐘

### **⚡ SCD Type 2 Implementation** 🎯
**🔄 Slowly Changing Dimensions (SCD Type 2)** for historical tracking:

```sql
-- Control columns added to each batch record
ALTER TABLE bus_routes ADD COLUMN start_date TIMESTAMP;
ALTER TABLE bus_routes ADD COLUMN end_date TIMESTAMP;
ALTER TABLE bus_routes ADD COLUMN is_current BOOLEAN;
```

**🔧 Process**:
- **🟢 start_date**: When record became active ✅
- **🔴 end_date**: When record ceased to be current ❌
- **🏷️ is_current**: Flag indicating latest version 🚩

**⚡ When changes occur**:
- 💀 Expire old record (set end_date + is_current = False)
- 🆕 Insert new record (set start_date, is_current = True)

### **🎯 Optimized Data Transfer Strategy** 🚀
**📈 Daily Batch Data Transfer to ClickHouse**:
- **💡 Strategy**: Only transfer daily batch data for current day (not entire historical warehouse) 🎯
- **🔥 Benefits**:
  - ⚡ **Optimized Spark Load**: Processes only today's data
  - 🏎️ **Faster Comparison**: Quick reconciliation with real-time API data
  - 💰 **Reduced Overhead**: Lower storage and processing costs

---

## 🌊 **Real-time Streaming Pipeline** ⚡

<div align="center">
<img src="https://media.giphy.com/media/3o7qE1YN7aBOFPRw8E/giphy.gif" width="400"/>
</div>

### **🎯 Streaming Objectives** 🔥
- 🏗️ Build comprehensive real-time data streaming pipeline for transit data
- 📡 Continuous ingestion of live transit data with real-time transformation
- 🎛️ Processing and analytics-ready storage capabilities

### **📡 Kafka Configuration** ⚡
<div align="center">

🎵 **Apache Kafka - The Heart of Our Streaming! 💓**

</div>

**🎯 Topic Setup**:
| Topic | Partitions | Retention | Compression |
|-------|-----------|-----------|-------------|
| 🚇 gtfs-trip-updates | 1️⃣ | 📅 7 days | 🗜️ snappy |
| 🚌 gtfs-vehicle-positions | 1️⃣ | 📅 7 days | 🗜️ snappy |
| 🚨 gtfs-alerts | 1️⃣ | 📅 7 days | 🗜️ snappy |

**💡 Partition Reasoning**:
- ⚡ Data processed quickly
- 1️⃣ Only 1 message per topic every minute
- 🚫 No need for parallelism
- 🔄 Replication factor: 1
- 📊 Max message size: 25MB

### **🔄 Kafka Producer Responsibilities** 💪
- **📥 Data Fetching**: Fetch API data every minute with automatic API key rotation to avoid rate limits 🔑🔄
- **📊 Metrics Collection**: Push JSON payloads to Kafka topics 📤
- **👁️ Monitoring**: Log update intervals and payload sizes to ClickHouse 📈

### **⚡ Spark Structured Streaming** 🔥
<div align="center">

🎯 **Apache Spark - Processing Powerhouse! 💥**

</div>

**🏗️ Architecture Benefits**:
- 🚀 High-throughput, low-latency pipeline processing
- 💾 Built-in checkpointing and fault tolerance
- 🛡️ Comprehensive error handling

**🔄 Processing Flow**:
1. **📥 Kafka Consumption**: Offset management and JSON parsing 📋
2. **✅ Schema Validation**: JSON parsing and schema creation 🗂️
3. **🔄 Data Transformation**: DataFrame explode and flatten techniques 📊
4. **💾 Storage Integration**: Write processed data to ClickHouse ⚡

### **🚌 Vehicle Positions Processing** 🎯

**⚡ Advanced Processing Steps**:
1. **📝 JSON Parsing**: Parse nested JSON structure 🗂️
2. **🏗️ Schema Creation**: Define data structure 📐
3. **🔗 Static Data Integration**: Read scheduled data from ClickHouse (stops, stop_times) 🚏
4. **🤝 Data Joining**: Join vehicle positions with static stops and stop_times 🔗
5. **📏 Distance Calculation**: Calculate distance using Haversine formula 🌍
6. **🚦 Status Determination**: 
   - ✅ "arrived" if distance < 100m from stop 🚌🚏
   - 🛣️ "on_way" otherwise 🚌➡️
   - ⏱️ Calculate delay_seconds


---

## 🎛️ **Dashboards **

### **📊 Dashboard**
- **Live Vehicle Tracking**: Current positions and status
- **Performance Metrics**: OTP, average delays, alert counts
- **Borough Analysis**: Route performance by geographic region
- **Real-time Monitoring**: Active vehicles and arrival rates
  
### **📊 Day-to-Day Dashboard**
![Dashboard](./images/Day-to-Day-Dashboard.png)
---
### **📊 Real-time Dashboard**
![Dashboard](./images/Real-time-Dashboard.png)
---

## 🚀 **Key Technical Challenges & Solutions** 💪

<div align="center">
<img src="https://media.giphy.com/media/zOvBKUUEERdNm/giphy.gif" width="350"/>

*💪 Overcoming Technical Mountains! 🏔️*
</div>

### **⚡ Performance Tuning** 🔥
- **🛠️ Optimized Configurations**: Enhanced Kafka and Spark processing parameters ⚙️
- **💾 Resource Management**: Due to RAM limitations on Azure VM, strategic container management needed 🐳
- **🔮 Future Enhancement**: Kubernetes recommended for dynamic container orchestration ☸️

### **🔧 Reliability Engineering** 🛡️
- **🔄 Error Handling**: Comprehensive retry mechanisms 🔁
- **👁️ Monitoring**: 24/7 system monitoring on Azure VM 🌙☀️
- **💪 Fault Tolerance**: Built-in checkpointing and recovery mechanisms 🏥

### **📈 System Optimization** 🎯
- **🚀 High-throughput Processing**: Optimized for low-latency streaming ⚡
- **📏 Scalable Architecture**: Cloud-native design for horizontal scaling 📈
- **💰 Cost Efficiency**: Resource-optimized processing strategies 💸

---


## 👥 **Project Team** 🏆

**🎓 ITI Data Engineering Cohort** 🚀
<div align="center">

| 👤 **Team Member** | 🔗 **LinkedIn Profile** |
|-------------------|------------------------|
| **📦 Ahmed Rabie** | [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ahmed-m-rabie-0ba5b120b/) |
| **📦 Diea AbdelTawab** | [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/dieaabdeltwab/) |
| **⚡ Mohanad Hossam** | [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/ho03/) |
| **⚡ Mohamed Ibrahim** | [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohamed-abu-en-neel-b7a70a280/) |
| **🏗️ Mohamed Zaki** | [![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/mohamed-zaki-266126189/) |

</div>

🤝 **Teamwork Makes the Great Work! 💫**

**🔥 Built with ❤️ by the ITI Data Engineering Team 🔥**


