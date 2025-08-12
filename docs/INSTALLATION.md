# Installation Guide

## Prerequisites
- Python 3.8+
- Docker & Docker Compose
- DDN Infinia cluster access
- Grafana Cloud account (optional)

## Quick Setup
1. Clone repository
2. Install requirements: `pip install -r requirements.txt`
3. Configure environment: Copy `config/config_template.env` to `.env`
4. Start Kafka: `cd docker && docker-compose up -d`
5. Run application: `python src/kafka_infinia_fraud_detection_pipeline.py`
6. Access UI: http://localhost:7861