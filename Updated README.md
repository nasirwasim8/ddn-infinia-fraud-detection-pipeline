# DDN Infinia Fraud Detection Pipeline - 
Developed by Nasir Wasim Senior AI Solutions Architect

Real-time fraud detection system with DDN Infinia as the centralized data hub for ML analytics, pattern analysis, and anomaly detection.

## Architecture

- **Apache Kafka**: Real-time transaction streaming
- **DDN Infinia**: Centralized object storage for ML-ready data
- **Grafana Cloud**: Enterprise monitoring and dashboards
- **Python/Gradio**: Web-based management interface

## Quick Start

### Prerequisites
- Python 3.8+
- Docker & Docker Compose
- DDN Infinia cluster access
- Grafana Cloud account (optional)

### Installation

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/ddn-infinia-fraud-detection-pipeline.git
cd ddn-infinia-fraud-detection-pipeline

## Quick Setup
1. Clone repository
2. Install requirements:
3. Configure environment: Copy `config/config_template.env` to `.env`
4. Start Kafka: `cd docker && docker-compose up -d`
5. Run application: `python src/kafka_infinia_fraud_detection_pipeline.py`
6. Access UI: http://localhost:7861
