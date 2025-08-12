# Real-Time Fraud Detection Pipeline with Kafka Integration, DDN Infinia, and Grafana Cloud
# DDN Infinia serves as the centralized data hub for ML analytics, pattern analysis, and anomaly detection
#-----------------------------------------------

import json
import time
import random
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional
import queue
import logging
from dataclasses import dataclass, asdict
import uuid
import os
from pathlib import Path
import urllib3
from urllib3.exceptions import InsecureRequestWarning
import requests
import base64
import re

# External dependencies
import boto3
import gradio as gr
from kafka import KafkaProducer, KafkaConsumer
from kafka.errors import KafkaError
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from botocore.exceptions import ClientError, NoCredentialsError, EndpointConnectionError

# Custom DDN Theme - Simplified
class DDNTheme(gr.themes.Base):
    def __init__(self):
        super().__init__(
            primary_hue=gr.themes.colors.red,
            secondary_hue=gr.themes.colors.orange,
            neutral_hue=gr.themes.colors.gray,
            font=[gr.themes.GoogleFont("Poppins"), gr.themes.GoogleFont("Inter"), "ui-sans-serif", "system-ui", "sans-serif"]
        )
        self.set(
            button_primary_background_fill="#E31E24",
            button_primary_background_fill_hover="#C41E3A",
            button_primary_text_color="white",
            button_secondary_background_fill="#FF6B35",
            button_secondary_background_fill_hover="#E55B3D",
            button_secondary_text_color="white",
            background_fill_primary="#FFFFFF",
            background_fill_secondary="#FAFAFA",
        )

urllib3.disable_warnings(InsecureRequestWarning)
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Configuration
@dataclass
class Config:
    KAFKA_BOOTSTRAP_SERVERS = ['localhost:9092']
    KAFKA_TOPIC_TRANSACTION_DATA = 'fraud-transactions'
    KAFKA_TOPIC_INFINIA_EVENTS = 'infinia-fraud-events'
    
    # DDN Infinia Configuration - Centralized Data Hub
    INFINIA_ENDPOINT_URL = os.getenv('INFINIA_ENDPOINT_URL', 'https://your-infinia-endpoint.com')
    INFINIA_ACCESS_KEY_ID = os.getenv('INFINIA_ACCESS_KEY_ID', 'your-access-key')
    INFINIA_SECRET_ACCESS_KEY = os.getenv('INFINIA_SECRET_ACCESS_KEY', 'your-secret-key')
    INFINIA_BUCKET_NAME = os.getenv('INFINIA_BUCKET_NAME', 'fraud-detection-hub')
    INFINIA_PREFIX = 'fraud-data/'
    
    # Grafana Cloud Configuration
    GRAFANA_CLOUD_URL = os.getenv('GRAFANA_CLOUD_URL', 'https://your-instance.grafana.net')
    GRAFANA_CLOUD_API_KEY = os.getenv('GRAFANA_CLOUD_API_KEY', 'your-grafana-api-key')
    GRAFANA_ORG_ID = os.getenv('GRAFANA_ORG_ID', '1')

config = Config()

# Data Models
@dataclass
class TransactionData:
    transaction_id: str
    timestamp: str
    user_id: str
    merchant_id: str
    amount: float
    currency: str
    payment_method: str
    location: str
    device_fingerprint: str
    ip_address: str
    risk_score: float
    fraud_indicators: List[str]
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class InfiniaEvent:
    bucket: str
    key: str
    event_type: str
    timestamp: str
    size: int
    endpoint: str
    data_category: str
    
    def to_dict(self) -> Dict:
        return asdict(self)
    
    def to_json(self) -> str:
        return json.dumps(self.to_dict())

@dataclass
class PipelineMetrics:
    timestamp: str
    transactions_processed: int
    suspicious_transactions: int
    files_stored: int
    avg_risk_score: float
    high_risk_count: int
    error_count: int
    pipeline_status: str
    
    def to_dict(self) -> Dict:
        return asdict(self)

@dataclass
class MetadataSearchResult:
    key: str
    metadata: Dict
    size: int
    last_modified: str
    transaction_id: str
    risk_score: float
    risk_category: str
    amount: float
    merchant_id: str
    location: str

# Grafana Cloud Integration
class GrafanaCloudIntegration:
    def __init__(self):
        self.grafana_url = config.GRAFANA_CLOUD_URL
        self.api_key = config.GRAFANA_CLOUD_API_KEY
        self.org_id = config.GRAFANA_ORG_ID
        self.dashboard_id = None
        self.datasource_uid = None
        self.metrics_buffer = queue.Queue(maxsize=1000)
        
    def test_connection(self) -> Dict:
        try:
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.get(
                f'{self.grafana_url}/api/org',
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                org_info = response.json()
                return {
                    'success': True,
                    'message': f'✅ Successfully connected to Grafana Cloud!\nOrganization: {org_info.get("name", "Unknown")}\nOrg ID: {org_info.get("id", "Unknown")}',
                    'org_name': org_info.get('name', 'Unknown')
                }
            else:
                return {
                    'success': False,
                    'message': f'❌ Grafana Cloud connection failed: HTTP {response.status_code}',
                    'org_name': None
                }
                
        except requests.exceptions.RequestException as e:
            return {
                'success': False,
                'message': f'❌ Grafana Cloud connection error: {str(e)}',
                'org_name': None
            }
    
    def log_metrics(self, metrics: PipelineMetrics):
        try:
            if not self.metrics_buffer.full():
                self.metrics_buffer.put(metrics)
            logger.info(f"Fraud metrics logged: {metrics.transactions_processed} transactions, {metrics.suspicious_transactions} suspicious")
        except Exception as e:
            logger.error(f"Error logging metrics: {e}")
    
    def create_testdata_datasource(self) -> Dict:
        try:
            datasource_payload = {
                "name": "Fraud-Detection-Pipeline-Data",
                "type": "testdata",
                "access": "proxy",
                "isDefault": False,
                "jsonData": {
                    "enableSeries": True
                }
            }
            
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                f'{self.grafana_url}/api/datasources',
                json=datasource_payload,
                headers=headers,
                timeout=10
            )
            
            if response.status_code in [200, 409]:
                if response.status_code == 409:
                    get_response = requests.get(
                        f'{self.grafana_url}/api/datasources/name/Fraud-Detection-Pipeline-Data',
                        headers=headers,
                        timeout=10
                    )
                    if get_response.status_code == 200:
                        self.datasource_uid = get_response.json().get('uid')
                else:
                    self.datasource_uid = response.json().get('uid')
                
                return {
                    'success': True,
                    'message': 'TestData datasource configured successfully',
                    'uid': self.datasource_uid
                }
            else:
                return {
                    'success': False,
                    'message': f'Failed to create datasource: HTTP {response.status_code}',
                    'uid': None
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'Error creating datasource: {str(e)}',
                'uid': None
            }
    
    def create_dashboard(self) -> Dict:
        try:
            datasource_result = self.create_testdata_datasource()
            if not datasource_result['success']:
                return {
                    'success': False,
                    'message': f"Failed to create datasource: {datasource_result['message']}",
                    'dashboard_url': None
                }
            
            ds_uid = self.datasource_uid or "000000002"
            
            dashboard_json = {
                "dashboard": {
                    "id": None,
                    "title": "DDN Infinia Fraud Detection Dashboard",
                    "tags": ["fraud", "detection", "infinia", "security"],
                    "timezone": "browser",
                    "refresh": "5s",
                    "time": {
                        "from": "now-5m",
                        "to": "now"
                    },
                    "panels": [
                        {
                            "id": 1,
                            "title": "Transactions Processed",
                            "type": "stat",
                            "targets": [
                                {
                                    "datasource": {"type": "testdata", "uid": ds_uid},
                                    "scenarioId": "random_walk",
                                    "seriesCount": 1,
                                    "alias": "Transactions/sec",
                                    "min": 50,
                                    "max": 200,
                                    "refId": "A"
                                }
                            ],
                            "gridPos": {"h": 8, "w": 6, "x": 0, "y": 0},
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "palette-classic"},
                                    "custom": {"displayMode": "basic"},
                                    "unit": "ops",
                                    "displayName": "Transactions"
                                }
                            }
                        },
                        {
                            "id": 2,
                            "title": "High Risk Transactions",
                            "type": "stat",
                            "targets": [
                                {
                                    "datasource": {"type": "testdata", "uid": ds_uid},
                                    "scenarioId": "random_walk",
                                    "seriesCount": 1,
                                    "alias": "High Risk",
                                    "min": 0,
                                    "max": 15,
                                    "refId": "B"
                                }
                            ],
                            "gridPos": {"h": 8, "w": 6, "x": 6, "y": 0},
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "thresholds"},
                                    "custom": {"displayMode": "basic"},
                                    "thresholds": {
                                        "steps": [
                                            {"color": "green", "value": 0},
                                            {"color": "yellow", "value": 5},
                                            {"color": "red", "value": 10}
                                        ]
                                    },
                                    "displayName": "High Risk"
                                }
                            }
                        },
                        {
                            "id": 3,
                            "title": "DDN Infinia Storage",
                            "type": "stat",
                            "targets": [
                                {
                                    "datasource": {"type": "testdata", "uid": ds_uid},
                                    "scenarioId": "random_walk",
                                    "seriesCount": 1,
                                    "alias": "Files Stored",
                                    "min": 0,
                                    "max": 1000,
                                    "refId": "C"
                                }
                            ],
                            "gridPos": {"h": 8, "w": 6, "x": 12, "y": 0},
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "palette-classic"},
                                    "custom": {"displayMode": "basic"},
                                    "displayName": "Files in Infinia Hub"
                                }
                            }
                        },
                        {
                            "id": 4,
                            "title": "Pipeline Status",
                            "type": "stat",
                            "targets": [
                                {
                                    "datasource": {"type": "testdata", "uid": ds_uid},
                                    "scenarioId": "random_walk",
                                    "seriesCount": 1,
                                    "alias": "Status",
                                    "min": 0,
                                    "max": 1,
                                    "refId": "D"
                                }
                            ],
                            "gridPos": {"h": 8, "w": 6, "x": 18, "y": 0},
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "thresholds"},
                                    "custom": {"displayMode": "basic"},
                                    "thresholds": {
                                        "steps": [
                                            {"color": "red", "value": 0},
                                            {"color": "green", "value": 0.5}
                                        ]
                                    },
                                    "mappings": [
                                        {"options": {"0": {"text": "Stopped"}}, "type": "value"},
                                        {"1": {"text": "Active"}, "type": "value"}
                                    ],
                                    "displayName": "Detection Status"
                                }
                            }
                        },
                        {
                            "id": 5,
                            "title": "Risk Score Distribution",
                            "type": "timeseries",
                            "targets": [
                                {
                                    "datasource": {"type": "testdata", "uid": ds_uid},
                                    "scenarioId": "random_walk",
                                    "seriesCount": 3,
                                    "alias": "Risk Level ${__series.name}",
                                    "min": 0,
                                    "max": 100,
                                    "refId": "E"
                                }
                            ],
                            "gridPos": {"h": 9, "w": 12, "x": 0, "y": 8},
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "palette-classic"},
                                    "custom": {
                                        "drawStyle": "line",
                                        "lineInterpolation": "linear",
                                        "pointSize": 3,
                                        "showPoints": "auto"
                                    },
                                    "unit": "percent",
                                    "displayName": "Risk Score"
                                }
                            }
                        },
                        {
                            "id": 6,
                            "title": "Transaction Volume & Fraud Rate",
                            "type": "timeseries",
                            "targets": [
                                {
                                    "datasource": {"type": "testdata", "uid": ds_uid},
                                    "scenarioId": "random_walk",
                                    "seriesCount": 2,
                                    "alias": "Transaction Volume",
                                    "min": 100,
                                    "max": 500,
                                    "refId": "F"
                                },
                                {
                                    "datasource": {"type": "testdata", "uid": ds_uid},
                                    "scenarioId": "random_walk",
                                    "seriesCount": 1,
                                    "alias": "Fraud Rate %",
                                    "min": 0,
                                    "max": 10,
                                    "refId": "G"
                                }
                            ],
                            "gridPos": {"h": 9, "w": 12, "x": 12, "y": 8},
                            "fieldConfig": {
                                "defaults": {
                                    "color": {"mode": "palette-classic"},
                                    "custom": {
                                        "drawStyle": "line",
                                        "lineInterpolation": "linear",
                                        "pointSize": 3,
                                        "showPoints": "auto"
                                    }
                                }
                            }
                        }
                    ]
                },
                "overwrite": True
            }
            
            headers = {
                'Authorization': f'Bearer {self.api_key}',
                'Content-Type': 'application/json'
            }
            
            response = requests.post(
                f'{self.grafana_url}/api/dashboards/db',
                json=dashboard_json,
                headers=headers,
                timeout=10
            )
            
            if response.status_code == 200:
                result = response.json()
                self.dashboard_id = result.get('id')
                dashboard_url = f'{self.grafana_url}/d/{result.get("uid")}'
                
                return {
                    'success': True,
                    'message': f'✅ Fraud Detection Dashboard created!\nURL: {dashboard_url}\n\n📊 Live demo data shows real-time fraud detection metrics.',
                    'dashboard_url': dashboard_url,
                    'dashboard_id': self.dashboard_id
                }
            else:
                return {
                    'success': False,
                    'message': f'❌ Failed to create dashboard: HTTP {response.status_code}\n{response.text}',
                    'dashboard_url': None
                }
                
        except Exception as e:
            return {
                'success': False,
                'message': f'❌ Error creating dashboard: {str(e)}',
                'dashboard_url': None
            }
    
    def get_recent_metrics(self, limit: int = 50) -> List[PipelineMetrics]:
        metrics = []
        temp_queue = queue.Queue()
        
        while not self.metrics_buffer.empty() and len(metrics) < limit:
            item = self.metrics_buffer.get()
            metrics.append(item)
            temp_queue.put(item)
        
        while not temp_queue.empty():
            self.metrics_buffer.put(temp_queue.get())
        
        return metrics[-limit:]

    def update_credentials(self, grafana_url: str, api_key: str, org_id: str = "1"):
        self.grafana_url = grafana_url
        self.api_key = api_key
        self.org_id = org_id
        
        config.GRAFANA_CLOUD_URL = grafana_url
        config.GRAFANA_CLOUD_API_KEY = api_key
        config.GRAFANA_ORG_ID = org_id

# Transaction Data Producer Class
class TransactionDataProducer:
    def __init__(self):
        self.producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None
        )
        self.is_running = False
        self.merchants = ['Amazon', 'PayPal', 'Stripe', 'Visa', 'MasterCard', 'Square', 'Apple Pay', 'Google Pay']
        self.payment_methods = ['credit_card', 'debit_card', 'paypal', 'crypto', 'bank_transfer', 'mobile_wallet']
        self.locations = ['New York', 'London', 'Tokyo', 'Sydney', 'Toronto', 'Berlin', 'France', 'Singapore']
        self.fraud_indicators = [
            'velocity_check_failed', 'geo_location_mismatch', 'device_fingerprint_suspicious',
            'unusual_amount', 'blacklisted_ip', 'card_testing_pattern', 'merchant_risk_high',
            'behavioral_anomaly', 'time_pattern_unusual', 'multiple_failed_attempts'
        ]
        self.message_count = 0
        
    def generate_transaction_data(self) -> TransactionData:
        user_id = f"user_{random.randint(1000, 9999)}"
        merchant = random.choice(self.merchants)
        
        is_suspicious = random.random() < 0.15
        
        if is_suspicious:
            amount = random.uniform(1000, 10000)
            risk_score = random.uniform(70, 100)
            indicators = random.sample(self.fraud_indicators, random.randint(2, 4))
        else:
            amount = random.uniform(10, 500)
            risk_score = random.uniform(0, 30)
            indicators = []
        
        return TransactionData(
            transaction_id=str(uuid.uuid4()),
            timestamp=datetime.now(timezone.utc).isoformat(),
            user_id=user_id,
            merchant_id=f"{merchant.lower().replace(' ', '_')}_{random.randint(100, 999)}",
            amount=round(amount, 2),
            currency=random.choice(['USD', 'EUR', 'GBP', 'JPY']),
            payment_method=random.choice(self.payment_methods),
            location=random.choice(self.locations),
            device_fingerprint=f"fp_{random.randint(100000, 999999)}",
            ip_address=f"{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}.{random.randint(1, 255)}",
            risk_score=round(risk_score, 2),
            fraud_indicators=indicators
        )
    
    def start_streaming(self, interval: float = 1.0):
        self.is_running = True
        
        def stream_data():
            while self.is_running:
                try:
                    transaction_data = self.generate_transaction_data()
                    
                    future = self.producer.send(
                        config.KAFKA_TOPIC_TRANSACTION_DATA,
                        key=transaction_data.transaction_id,
                        value=transaction_data.to_dict()
                    )
                    
                    self.message_count += 1
                    
                    risk_level = "HIGH" if transaction_data.risk_score > 70 else "MEDIUM" if transaction_data.risk_score > 30 else "LOW"
                    logger.info(f"Transaction: {transaction_data.transaction_id} - ${transaction_data.amount} - Risk: {risk_level}")
                    
                    time.sleep(interval)
                    
                except Exception as e:
                    logger.error(f"Error sending transaction data: {e}")
                    time.sleep(1)
        
        thread = threading.Thread(target=stream_data, daemon=True)
        thread.start()
        return thread
    
    def stop_streaming(self):
        self.is_running = False
        self.producer.flush()
        self.producer.close()

# DDN Infinia Handler with Enhanced Metadata Search
class InfiniaHandler:
    def __init__(self):
        self.infinia_client = None
        self.bucket_name = None
        self._initialize_client()
    
    def _initialize_client(self):
        self.infinia_client = boto3.client(
            's3',
            endpoint_url=config.INFINIA_ENDPOINT_URL,
            aws_access_key_id=config.INFINIA_ACCESS_KEY_ID,
            aws_secret_access_key=config.INFINIA_SECRET_ACCESS_KEY,
            region_name='us-east-1',
            verify=False,
            use_ssl=True
        )
        self.bucket_name = config.INFINIA_BUCKET_NAME
    
    def refresh_client(self):
        self._initialize_client()
        
    def ensure_bucket_exists(self):
        try:
            self.infinia_client.head_bucket(Bucket=self.bucket_name)
            logger.info(f"DDN Infinia bucket {self.bucket_name} ready for fraud data hub")
        except:
            try:
                self.infinia_client.create_bucket(Bucket=self.bucket_name)
                logger.info(f"Created DDN Infinia fraud detection hub: {self.bucket_name}")
            except Exception as e:
                logger.error(f"Error creating DDN Infinia bucket: {e}")
                raise
    
    def upload_transaction_data(self, transaction_data: TransactionData) -> Optional[str]:
        try:
            timestamp = datetime.fromisoformat(transaction_data.timestamp.replace('Z', '+00:00'))
            
            risk_category = "high_risk" if transaction_data.risk_score > 70 else "medium_risk" if transaction_data.risk_score > 30 else "low_risk"
            
            key = f"{config.INFINIA_PREFIX}{risk_category}/{timestamp.year}/{timestamp.month:02d}/{timestamp.day:02d}/{transaction_data.transaction_id}.json"
            
            # Enhanced metadata for search capabilities
            extra_args = {
                'Metadata': {
                    'transaction_id': transaction_data.transaction_id,
                    'user_id': transaction_data.user_id,
                    'risk_score': str(transaction_data.risk_score),
                    'risk_category': risk_category,
                    'amount': str(transaction_data.amount),
                    'currency': transaction_data.currency,
                    'merchant_id': transaction_data.merchant_id,
                    'merchant_category': transaction_data.merchant_id.split('_')[0],
                    'location': transaction_data.location,
                    'payment_method': transaction_data.payment_method,
                    'device_fingerprint': transaction_data.device_fingerprint,
                    'ip_address': transaction_data.ip_address,
                    'fraud_indicators_count': str(len(transaction_data.fraud_indicators)),
                    'fraud_indicators': ','.join(transaction_data.fraud_indicators) if transaction_data.fraud_indicators else 'none',
                    'transaction_hour': str(timestamp.hour),
                    'transaction_day': timestamp.strftime('%A'),
                    'transaction_month': timestamp.strftime('%B'),
                    'amount_range': self._get_amount_range(transaction_data.amount),
                    'upload_timestamp': str(datetime.now()),
                    'data_source': 'fraud_detection_pipeline',
                    'storage_backend': 'ddn_infinia',
                    'analytics_ready': 'true',
                    'ml_training_data': 'true' if transaction_data.fraud_indicators else 'false',
                    'searchable': 'true'
                }
            }
            
            self.infinia_client.put_object(
                Bucket=self.bucket_name,
                Key=key,
                Body=transaction_data.to_json(),
                ContentType='application/json',
                **extra_args
            )
            
            logger.info(f"Stored in DDN Infinia Hub: {key} ({risk_category})")
            return key
            
        except Exception as e:
            logger.error(f"Error uploading to DDN Infinia: {e}")
            return None
    
    def _get_amount_range(self, amount: float) -> str:
        if amount < 50:
            return 'micro'
        elif amount < 200:
            return 'small'
        elif amount < 1000:
            return 'medium'
        elif amount < 5000:
            return 'large'
        else:
            return 'xlarge'
    
    def search_by_metadata(self, search_criteria: Dict[str, str], max_results: int = 100) -> List[MetadataSearchResult]:
        """
        Search transactions based on metadata criteria
        search_criteria: dict with keys like 'risk_category', 'merchant_category', 'location', etc.
        """
        try:
            results = []
            continuation_token = None
            
            while len(results) < max_results:
                list_params = {
                    'Bucket': self.bucket_name,
                    'Prefix': config.INFINIA_PREFIX,
                    'MaxKeys': min(1000, max_results - len(results))
                }
                
                if continuation_token:
                    list_params['ContinuationToken'] = continuation_token
                
                response = self.infinia_client.list_objects_v2(**list_params)
                
                if 'Contents' not in response:
                    break
                
                for obj in response['Contents']:
                    try:
                        # Get object metadata
                        head_response = self.infinia_client.head_object(
                            Bucket=self.bucket_name,
                            Key=obj['Key']
                        )
                        
                        metadata = head_response.get('Metadata', {})
                        
                        # Check if metadata matches search criteria
                        if self._matches_criteria(metadata, search_criteria):
                            search_result = MetadataSearchResult(
                                key=obj['Key'],
                                metadata=metadata,
                                size=obj['Size'],
                                last_modified=obj['LastModified'].isoformat(),
                                transaction_id=metadata.get('transaction_id', ''),
                                risk_score=float(metadata.get('risk_score', 0)),
                                risk_category=metadata.get('risk_category', ''),
                                amount=float(metadata.get('amount', 0)),
                                merchant_id=metadata.get('merchant_id', ''),
                                location=metadata.get('location', '')
                            )
                            results.append(search_result)
                            
                            if len(results) >= max_results:
                                break
                                
                    except Exception as e:
                        logger.warning(f"Error processing object {obj['Key']}: {e}")
                        continue
                
                if not response.get('IsTruncated'):
                    break
                    
                continuation_token = response.get('NextContinuationToken')
            
            return results
            
        except Exception as e:
            logger.error(f"Error searching metadata: {e}")
            return []
    
    def _matches_criteria(self, metadata: Dict[str, str], criteria: Dict[str, str]) -> bool:
        """Check if metadata matches search criteria"""
        for key, value in criteria.items():
            if not value:  # Skip empty criteria
                continue
                
            metadata_value = metadata.get(key, '').lower()
            search_value = value.lower()
            
            # Handle different search patterns
            if key == 'risk_score_min':
                try:
                    if float(metadata.get('risk_score', 0)) < float(search_value):
                        return False
                except ValueError:
                    continue
            elif key == 'risk_score_max':
                try:
                    if float(metadata.get('risk_score', 0)) > float(search_value):
                        return False
                except ValueError:
                    continue
            elif key == 'amount_min':
                try:
                    if float(metadata.get('amount', 0)) < float(search_value):
                        return False
                except ValueError:
                    continue
            elif key == 'amount_max':
                try:
                    if float(metadata.get('amount', 0)) > float(search_value):
                        return False
                except ValueError:
                    continue
            elif key == 'fraud_indicators':
                if search_value not in metadata_value:
                    return False
            else:
                # Exact or partial match for other fields
                if search_value not in metadata_value:
                    return False
        
        return True
    
    def get_advanced_analytics(self, search_results: List[MetadataSearchResult]) -> Dict:
        """Generate analytics from search results"""
        if not search_results:
            return {}
        
        total_transactions = len(search_results)
        total_amount = sum(r.amount for r in search_results)
        avg_risk_score = sum(r.risk_score for r in search_results) / total_transactions
        
        # Risk distribution
        risk_distribution = {}
        for result in search_results:
            risk_cat = result.risk_category
            risk_distribution[risk_cat] = risk_distribution.get(risk_cat, 0) + 1
        
        # Location distribution
        location_distribution = {}
        for result in search_results:
            loc = result.location
            location_distribution[loc] = location_distribution.get(loc, 0) + 1
        
        # Amount ranges
        amount_ranges = {'micro': 0, 'small': 0, 'medium': 0, 'large': 0, 'xlarge': 0}
        for result in search_results:
            range_key = self._get_amount_range(result.amount)
            amount_ranges[range_key] += 1
        
        return {
            'total_transactions': total_transactions,
            'total_amount': total_amount,
            'avg_amount': total_amount / total_transactions,
            'avg_risk_score': avg_risk_score,
            'risk_distribution': risk_distribution,
            'location_distribution': location_distribution,
            'amount_ranges': amount_ranges,
            'high_risk_count': sum(1 for r in search_results if r.risk_score > 70),
            'suspicious_count': sum(1 for r in search_results if r.risk_score > 30)
        }
    
    def list_recent_files(self, limit: int = 10) -> List[Dict]:
        try:
            response = self.infinia_client.list_objects_v2(
                Bucket=self.bucket_name,
                Prefix=config.INFINIA_PREFIX,
                MaxKeys=limit
            )
            
            files = []
            for obj in response.get('Contents', []):
                files.append({
                    'key': obj['Key'],
                    'size': obj['Size'],
                    'last_modified': obj['LastModified'].isoformat(),
                    'category': obj['Key'].split('/')[1] if '/' in obj['Key'] else 'unknown'
                })
            
            return sorted(files, key=lambda x: x['last_modified'], reverse=True)
            
        except Exception as e:
            logger.error(f"Error listing Infinia files: {e}")
            return []
    
    def get_object_metadata(self, key: str) -> Dict:
        try:
            response = self.infinia_client.head_object(Bucket=self.bucket_name, Key=key)
            return {
                'metadata': response.get('Metadata', {}),
                'content_length': response.get('ContentLength', 0),
                'last_modified': response.get('LastModified', '').isoformat() if response.get('LastModified') else '',
                'storage_class': response.get('StorageClass', 'STANDARD')
            }
        except Exception as e:
            logger.error(f"Error getting metadata from Infinia: {e}")
            return {}
    
    def test_connection(self) -> Dict:
        try:
            response = self.infinia_client.list_buckets()
            
            buckets = [bucket['Name'] for bucket in response['Buckets']]
            if self.bucket_name in buckets:
                return {
                    'success': True,
                    'message': f'✅ DDN Infinia Fraud Hub Connected!\nBucket "{self.bucket_name}" ready for ML analytics.',
                    'bucket_count': len(buckets),
                    'endpoint': config.INFINIA_ENDPOINT_URL
                }
            else:
                return {
                    'success': False,
                    'message': f'❌ Fraud Hub bucket "{self.bucket_name}" not found.\nAvailable: {", ".join(buckets[:5])}{("..." if len(buckets) > 5 else "")}',
                    'bucket_count': len(buckets),
                    'endpoint': config.INFINIA_ENDPOINT_URL
                }
        except NoCredentialsError:
            return {
                'success': False,
                'message': '❌ Invalid DDN Infinia credentials.',
                'bucket_count': 0
            }
        except EndpointConnectionError:
            return {
                'success': False,
                'message': f'❌ Cannot connect to DDN Infinia: {config.INFINIA_ENDPOINT_URL}',
                'bucket_count': 0
            }
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'AccessDenied':
                return {
                    'success': False,
                    'message': '❌ Access denied to DDN Infinia.',
                    'bucket_count': 0
                }
            else:
                return {
                    'success': False,
                    'message': f'❌ Infinia Error: {error_code} - {e.response["Error"]["Message"]}',
                    'bucket_count': 0
                }
        except Exception as e:
            return {
                'success': False,
                'message': f'❌ Unexpected DDN Infinia error: {str(e)}',
                'bucket_count': 0
            }

# Fraud Detection Data Processor
class FraudDataProcessor:
    def __init__(self):
        self.infinia_handler = InfiniaHandler()
        self.processed_data = queue.Queue(maxsize=1000)
        self.infinia_event_producer = KafkaProducer(
            bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v).encode('utf-8')
        )
        self.is_running = False
        self.files_stored_count = 0
        self.error_count = 0
        self.suspicious_count = 0
        
    def start_processing(self):
        self.is_running = True
        
        def process_data():
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC_TRANSACTION_DATA,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='fraud-data-processor'
            )
            
            for message in consumer:
                if not self.is_running:
                    break
                    
                try:
                    transaction_dict = message.value
                    transaction_data = TransactionData(**transaction_dict)
                    
                    if transaction_data.risk_score > 70:
                        self.suspicious_count += 1
                    
                    infinia_key = self.infinia_handler.upload_transaction_data(transaction_data)
                    
                    if infinia_key:
                        self.files_stored_count += 1
                        
                        data_category = "ml_training" if transaction_data.fraud_indicators else "analytics"
                        if transaction_data.risk_score > 70:
                            data_category = "anomalies"
                        elif transaction_data.risk_score > 30:
                            data_category = "patterns"
                        
                        infinia_event = InfiniaEvent(
                            bucket=config.INFINIA_BUCKET_NAME,
                            key=infinia_key,
                            event_type='FraudDataStored',
                            timestamp=datetime.now(timezone.utc).isoformat(),
                            size=len(transaction_data.to_json()),
                            endpoint=config.INFINIA_ENDPOINT_URL,
                            data_category=data_category
                        )
                        
                        self.infinia_event_producer.send(
                            config.KAFKA_TOPIC_INFINIA_EVENTS,
                            value=infinia_event.to_dict()
                        )
                        
                        if not self.processed_data.full():
                            self.processed_data.put(transaction_data)
                        
                        logger.info(f"Fraud data processed: {transaction_data.transaction_id} - Risk: {transaction_data.risk_score}")
                    else:
                        self.error_count += 1
                    
                except Exception as e:
                    self.error_count += 1
                    logger.error(f"Error processing transaction: {e}")
        
        thread = threading.Thread(target=process_data, daemon=True)
        thread.start()
        return thread
    
    def stop_processing(self):
        self.is_running = False
        self.infinia_event_producer.close()
    
    def get_recent_data(self, limit: int = 50) -> List[TransactionData]:
        data = []
        temp_queue = queue.Queue()
        
        while not self.processed_data.empty() and len(data) < limit:
            item = self.processed_data.get()
            data.append(item)
            temp_queue.put(item)
        
        while not temp_queue.empty():
            self.processed_data.put(temp_queue.get())
        
        return data[-limit:]

# Grafana Metrics Collector
class GrafanaMetricsCollector:
    def __init__(self, grafana_integration: GrafanaCloudIntegration):
        self.grafana_integration = grafana_integration
        self.is_running = False
        
    def start_collecting(self):
        self.is_running = True
        
        def collect_metrics():
            while self.is_running:
                try:
                    recent_data = processor.get_recent_data(100)
                    
                    avg_risk = sum(d.risk_score for d in recent_data) / len(recent_data) if recent_data else 0
                    high_risk_count = sum(1 for d in recent_data if d.risk_score > 70)
                    
                    metrics = PipelineMetrics(
                        timestamp=datetime.now(timezone.utc).isoformat(),
                        transactions_processed=producer.message_count,
                        suspicious_transactions=processor.suspicious_count,
                        files_stored=processor.files_stored_count,
                        avg_risk_score=round(avg_risk, 2),
                        high_risk_count=high_risk_count,
                        error_count=processor.error_count,
                        pipeline_status='running' if processor.is_running else 'stopped'
                    )
                    
                    self.grafana_integration.log_metrics(metrics)
                    
                    time.sleep(30)
                    
                except Exception as e:
                    logger.error(f"Error collecting metrics: {e}")
                    time.sleep(10)
        
        thread = threading.Thread(target=collect_metrics, daemon=True)
        thread.start()
        return thread
    
    def stop_collecting(self):
        self.is_running = False

# Infinia Event Consumer
class InfiniaEventConsumer:
    def __init__(self):
        self.is_running = False
        self.events = queue.Queue(maxsize=500)
        
    def start_consuming(self):
        self.is_running = True
        
        def consume_events():
            consumer = KafkaConsumer(
                config.KAFKA_TOPIC_INFINIA_EVENTS,
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                group_id='infinia-fraud-event-consumer'
            )
            
            for message in consumer:
                if not self.is_running:
                    break
                    
                try:
                    event_data = message.value
                    infinia_event = InfiniaEvent(**event_data)
                    
                    if not self.events.full():
                        self.events.put(infinia_event)
                    
                    logger.info(f"Infinia Hub Event: {infinia_event.event_type} - {infinia_event.data_category}")
                    
                except Exception as e:
                    logger.error(f"Error processing Infinia event: {e}")
        
        thread = threading.Thread(target=consume_events, daemon=True)
        thread.start()
        return thread
    
    def stop_consuming(self):
        self.is_running = False
    
    def get_recent_events(self, limit: int = 20) -> List[InfiniaEvent]:
        events = []
        temp_queue = queue.Queue()
        
        while not self.events.empty() and len(events) < limit:
            item = self.events.get()
            events.append(item)
            temp_queue.put(item)
        
        while not temp_queue.empty():
            self.events.put(temp_queue.get())
        
        return events[-limit:]

# Kafka Topic Management
class KafkaTopicManager:
    def __init__(self):
        self.admin_client = None
        
    def create_admin_client(self):
        try:
            from kafka.admin import KafkaAdminClient
            self.admin_client = KafkaAdminClient(
                bootstrap_servers=config.KAFKA_BOOTSTRAP_SERVERS,
                client_id='fraud_topic_manager'
            )
            return True
        except Exception as e:
            logger.error(f"Error creating Kafka admin client: {e}")
            return False
    
    def create_topic(self, topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
        try:
            from kafka.admin import NewTopic
            from kafka.errors import TopicAlreadyExistsError
            
            if not self.admin_client:
                if not self.create_admin_client():
                    return False, "Failed to create admin client"
            
            topic = NewTopic(
                name=topic_name,
                num_partitions=num_partitions,
                replication_factor=replication_factor
            )
            
            self.admin_client.create_topics([topic])
            return True, f"Topic '{topic_name}' created successfully"
            
        except TopicAlreadyExistsError:
            return True, f"Topic '{topic_name}' already exists"
        except Exception as e:
            return False, f"Error creating topic: {str(e)}"
    
    def list_topics(self):
        try:
            if not self.admin_client:
                if not self.create_admin_client():
                    return []
            
            topics = self.admin_client.list_topics()
            return list(topics)
        except Exception as e:
            logger.error(f"Error listing topics: {e}")
            return []
    
    def delete_topic(self, topic_name: str):
        try:
            if not self.admin_client:
                if not self.create_admin_client():
                    return False, "Failed to create admin client"
            
            self.admin_client.delete_topics([topic_name])
            return True, f"Topic '{topic_name}' deleted successfully"
        except Exception as e:
            return False, f"Error deleting topic: {str(e)}"

# Credentials Managers
class InfiniaCredentialsManager:
    def __init__(self):
        self.current_credentials = self._get_current_credentials()
    
    def _get_current_credentials(self):
        return {
            'endpoint_url': os.getenv('INFINIA_ENDPOINT_URL', ''),
            'access_key': os.getenv('INFINIA_ACCESS_KEY_ID', ''),
            'secret_key': os.getenv('INFINIA_SECRET_ACCESS_KEY', ''),
            'bucket': config.INFINIA_BUCKET_NAME
        }
    
    def update_credentials(self, endpoint_url: str, access_key: str, secret_key: str, bucket: str):
        try:
            os.environ['INFINIA_ENDPOINT_URL'] = endpoint_url
            os.environ['INFINIA_ACCESS_KEY_ID'] = access_key
            os.environ['INFINIA_SECRET_ACCESS_KEY'] = secret_key
            
            config.INFINIA_ENDPOINT_URL = endpoint_url
            config.INFINIA_ACCESS_KEY_ID = access_key
            config.INFINIA_SECRET_ACCESS_KEY = secret_key
            config.INFINIA_BUCKET_NAME = bucket
            
            processor.infinia_handler.refresh_client()
            
            test_client = boto3.client(
                's3',
                endpoint_url=endpoint_url,
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                region_name='us-east-1',
                verify=False,
                use_ssl=True
            )
            
            test_client.list_buckets()
            
            self.current_credentials = {
                'endpoint_url': endpoint_url,
                'access_key': access_key,
                'secret_key': secret_key,
                'bucket': bucket
            }
            
            return True, "✅ DDN Infinia fraud hub credentials updated!"
            
        except Exception as e:
            return False, f"❌ Error updating DDN Infinia credentials: {str(e)}"
    
    def test_connection(self):
        try:
            infinia_handler = InfiniaHandler()
            result = infinia_handler.test_connection()
            return result['success'], result['message']
        except Exception as e:
            return False, f"❌ DDN Infinia connection failed: {str(e)}"

class GrafanaCredentialsManager:
    def __init__(self):
        self.grafana_integration = GrafanaCloudIntegration()
        
    def update_credentials(self, grafana_url: str, api_key: str, org_id: str = "1"):
        try:
            self.grafana_integration.update_credentials(grafana_url, api_key, org_id)
            
            test_result = self.grafana_integration.test_connection()
            return test_result['success'], test_result['message']
                
        except Exception as e:
            return False, f"❌ Error updating Grafana credentials: {str(e)}"
    
    def test_connection(self):
        result = self.grafana_integration.test_connection()
        return result['success'], result['message']
    
    def create_dashboard(self):
        result = self.grafana_integration.create_dashboard()
        return result['success'], result['message'], result.get('dashboard_url')
    
    def get_current_config(self):
        return {
            'grafana_url': config.GRAFANA_CLOUD_URL,
            'api_key': config.GRAFANA_CLOUD_API_KEY[:20] + '...' if config.GRAFANA_CLOUD_API_KEY else 'Not Set',
            'org_id': config.GRAFANA_ORG_ID
        }

# Add these functions after the KafkaTopicManager class definition (around line 1253)
# These are the missing functions that are referenced in create_gradio_interface()

def delete_kafka_topic(topic_name: str):
    """Delete a Kafka topic"""
    if not topic_name.strip():
        return "❌ Topic name required", get_kafka_topics_list()
    
    success, message = topic_manager.delete_topic(topic_name)
    return message, get_kafka_topics_list()

def create_default_topics():
    """Create default Kafka topics for fraud detection"""
    results = []
    
    # Create transaction data topic
    success, message = topic_manager.create_topic(config.KAFKA_TOPIC_TRANSACTION_DATA, 3, 1)
    results.append(f"fraud-transactions: {message}")
    
    # Create Infinia events topic
    success, message = topic_manager.create_topic(config.KAFKA_TOPIC_INFINIA_EVENTS, 3, 1)
    results.append(f"infinia-fraud-events: {message}")
    
    return "\n".join(results), get_kafka_topics_list()

def refresh_kafka_topics():
    """Refresh the Kafka topics list"""
    return get_kafka_topics_list()

def get_kafka_topics_list():
    """Get list of Kafka topics as a DataFrame"""
    try:
        topics = topic_manager.list_topics()
        if not topics:
            return pd.DataFrame({"Topic Name": ["No topics found"], "Status": ["N/A"]})
        
        # Create DataFrame with topic names and status
        df_data = []
        for topic in topics:
            status = "✅ Active" if topic in [config.KAFKA_TOPIC_TRANSACTION_DATA, config.KAFKA_TOPIC_INFINIA_EVENTS] else "📋 Available"
            df_data.append({"Topic Name": topic, "Status": status})
        
        return pd.DataFrame(df_data)
    except Exception as e:
        return pd.DataFrame({"Topic Name": [f"Error: {str(e)}"], "Status": ["❌ Error"]})

def update_infinia_credentials(endpoint_url: str, access_key: str, secret_key: str, bucket_name: str):
    """Update DDN Infinia credentials"""
    if not all([endpoint_url, access_key, secret_key, bucket_name]):
        return "❌ All fields are required", get_current_infinia_config()
    
    success, message = infinia_manager.update_credentials(endpoint_url, access_key, secret_key, bucket_name)
    return message, get_current_infinia_config()

def test_infinia_connection():
    """Test DDN Infinia connection"""
    success, message = infinia_manager.test_connection()
    return message

def get_current_infinia_config():
    """Get current DDN Infinia configuration as DataFrame"""
    try:
        creds = infinia_manager.current_credentials
        
        # Mask sensitive data
        masked_access_key = creds['access_key'][:10] + '...' if len(creds['access_key']) > 10 else creds['access_key']
        masked_secret_key = '***' if creds['secret_key'] else 'Not Set'
        
        config_data = [
            {"Setting": "Endpoint URL", "Value": creds['endpoint_url'] or 'Not Configured'},
            {"Setting": "Access Key", "Value": masked_access_key or 'Not Set'},
            {"Setting": "Secret Key", "Value": masked_secret_key},
            {"Setting": "Bucket Name", "Value": creds['bucket'] or 'Not Set'},
            {"Setting": "Data Prefix", "Value": config.INFINIA_PREFIX},
            {"Setting": "Region", "Value": "us-east-1"}
        ]
        
        return pd.DataFrame(config_data)
    except Exception as e:
        return pd.DataFrame({"Setting": ["Error"], "Value": [str(e)]})

def update_grafana_credentials(grafana_url: str, api_key: str, org_id: str):
    """Update Grafana Cloud credentials"""
    if not all([grafana_url, api_key]):
        return "❌ Grafana URL and API Key are required", get_current_grafana_config()
    
    success, message = grafana_manager.update_credentials(grafana_url, api_key, org_id or "1")
    return message, get_current_grafana_config()

def test_grafana_connection():
    """Test Grafana Cloud connection"""
    success, message = grafana_manager.test_connection()
    return message

def create_grafana_dashboard():
    """Create Grafana dashboard for fraud detection"""
    success, message, dashboard_url = grafana_manager.create_dashboard()
    if success and dashboard_url:
        return f"{message}\n\n🔗 Dashboard URL: {dashboard_url}"
    return message

def get_current_grafana_config():
    """Get current Grafana Cloud configuration as DataFrame"""
    try:
        config_dict = grafana_manager.get_current_config()
        
        config_data = [
            {"Setting": "Grafana URL", "Value": config_dict['grafana_url'] or 'Not Configured'},
            {"Setting": "API Key", "Value": config_dict['api_key']},
            {"Setting": "Organization ID", "Value": config_dict['org_id']},
            {"Setting": "Dashboard Status", "Value": "✅ Created" if grafana_manager.grafana_integration.dashboard_id else "❌ Not Created"},
            {"Setting": "Metrics Collection", "Value": "🟢 Active" if metrics_collector.is_running else "🔴 Inactive"}
        ]
        
        return pd.DataFrame(config_data)
    except Exception as e:
        return pd.DataFrame({"Setting": ["Error"], "Value": [str(e)]})

# Also fix the create_kafka_topic function to match the expected signature
def create_kafka_topic(topic_name: str, partitions: int, replication: int):
    """Create a new Kafka topic"""
    if not topic_name.strip():
        return "❌ Topic name required", get_kafka_topics_list()
    
    success, message = topic_manager.create_topic(topic_name, partitions, replication)
    return message, get_kafka_topics_list()

# Global instances
producer = TransactionDataProducer()
processor = FraudDataProcessor()
infinia_event_consumer = InfiniaEventConsumer()
topic_manager = KafkaTopicManager()
infinia_manager = InfiniaCredentialsManager()
grafana_manager = GrafanaCredentialsManager()
metrics_collector = GrafanaMetricsCollector(grafana_manager.grafana_integration)

# Gradio UI Functions
def start_pipeline():
    try:
        if (config.INFINIA_ENDPOINT_URL == 'https://your-infinia-endpoint.com' or 
            config.INFINIA_ACCESS_KEY_ID == 'your-access-key' or
            config.INFINIA_SECRET_ACCESS_KEY == 'your-secret-key'):
            return "❌ Configure DDN Infinia credentials first in the 'DDN Infinia Config' tab."
        
        processor.infinia_handler.refresh_client()
        
        test_result = processor.infinia_handler.test_connection()
        if not test_result['success']:
            return f"❌ DDN Infinia Hub connection failed: {test_result['message']}"
        
        processor.infinia_handler.ensure_bucket_exists()
        
        producer.start_streaming()
        processor.start_processing()
        infinia_event_consumer.start_consuming()
        
        if (config.GRAFANA_CLOUD_URL != 'https://your-instance.grafana.net' and 
            config.GRAFANA_CLOUD_API_KEY != 'your-grafana-api-key'):
            if not metrics_collector.is_running:
                metrics_collector.start_collecting()
                return "✅ Fraud Detection Pipeline started!\n\n DDN Infinia Hub is now collecting transaction data for ML analytics.\n Live monitoring active in Grafana dashboard."
        
        return "✅ Fraud Detection Pipeline started!\n\n🎯 DDN Infinia Hub ready for ML analytics and pattern detection."
    except Exception as e:
        return f"❌ Error starting pipeline: {str(e)}"

def stop_pipeline():
    try:
        producer.stop_streaming()
        processor.stop_processing()
        infinia_event_consumer.stop_consuming()
        metrics_collector.stop_collecting()
        
        return "⏹️ Fraud Detection Pipeline stopped!"
    except Exception as e:
        return f"❌ Error stopping pipeline: {str(e)}"

# Metadata Search Functions
def search_transactions_by_metadata(risk_category, merchant_category, location, payment_method, 
                                  risk_score_min, risk_score_max, amount_min, amount_max, 
                                  fraud_indicators, max_results):
    try:
        search_criteria = {}
        
        if risk_category and risk_category != "All":
            search_criteria['risk_category'] = risk_category.lower().replace(' ', '_')
        if merchant_category:
            search_criteria['merchant_category'] = merchant_category.lower()
        if location:
            search_criteria['location'] = location
        if payment_method and payment_method != "All":
            search_criteria['payment_method'] = payment_method
        if risk_score_min:
            search_criteria['risk_score_min'] = str(risk_score_min)
        if risk_score_max:
            search_criteria['risk_score_max'] = str(risk_score_max)
        if amount_min:
            search_criteria['amount_min'] = str(amount_min)
        if amount_max:
            search_criteria['amount_max'] = str(amount_max)
        if fraud_indicators:
            search_criteria['fraud_indicators'] = fraud_indicators.lower()
        
        if not search_criteria:
            return pd.DataFrame(), "⚠️ Please specify at least one search criteria", {}
        
        results = processor.infinia_handler.search_by_metadata(search_criteria, max_results)
        
        if not results:
            return pd.DataFrame(), f"No transactions found matching criteria", {}
        
        # Convert to DataFrame
        df_data = []
        for result in results:
            df_data.append({
                'Transaction ID': result.transaction_id,
                'Risk Score': result.risk_score,
                'Risk Category': result.risk_category.replace('_', ' ').title(),
                'Amount': f"${result.amount:.2f}",
                'Merchant': result.merchant_id,
                'Location': result.location,
                'File Size': f"{result.size} bytes",
                'Stored': result.last_modified[:19].replace('T', ' ')
            })
        
        df = pd.DataFrame(df_data)
        
        # Generate analytics
        analytics = processor.infinia_handler.get_advanced_analytics(results)
        
        status_message = f"✅ Found {len(results)} transactions matching criteria"
        
        return df, status_message, analytics
        
    except Exception as e:
        return pd.DataFrame(), f"❌ Search error: {str(e)}", {}

def format_search_analytics(analytics):
    if not analytics:
        return "No analytics available"
    
    formatted = f""" **Search Analytics Summary:**

**Transaction Overview:**
• Total Transactions: {analytics['total_transactions']:,}
• Total Amount: ${analytics['total_amount']:,.2f}
• Average Amount: ${analytics['avg_amount']:.2f}
• Average Risk Score: {analytics['avg_risk_score']:.1f}

**Risk Distribution:**"""
    
    for risk_cat, count in analytics['risk_distribution'].items():
        emoji = "🔴" if "high" in risk_cat else "🟡" if "medium" in risk_cat else "🟢"
        formatted += f"\n• {emoji} {risk_cat.replace('_', ' ').title()}: {count}"
    
    formatted += f"\n\n**Geographic Distribution:**"
    for location, count in list(analytics['location_distribution'].items())[:5]:
        formatted += f"\n• {location}: {count} transactions"
    
    formatted += f"\n\n**Amount Ranges:**"
    range_labels = {'micro': 'Micro (<$50)', 'small': 'Small ($50-200)', 
                   'medium': 'Medium ($200-1K)', 'large': 'Large ($1K-5K)', 
                   'xlarge': 'XLarge (>$5K)'}
    for range_key, count in analytics['amount_ranges'].items():
        if count > 0:
            formatted += f"\n• {range_labels[range_key]}: {count}"
    
    formatted += f"\n\n**Risk Analysis:**"
    formatted += f"\n• High Risk (>70): {analytics['high_risk_count']}"
    formatted += f"\n• Suspicious (>30): {analytics['suspicious_count']}"
    
    return formatted

# UI Functions
def create_kafka_topic(topic_name: str, partitions: int, replication: int):
    if not topic_name.strip():
        return "❌ Topic name required", get_kafka_topics_list()
    
    success, message = topic_manager.create_topic(config.KAFKA_TOPIC_INFINIA_EVENTS, 3, 1)
    results.append(f"infinia-fraud-events: {message}")
    
    return "\n".join(results), get_kafka_topics_list()

def get_transaction_data_table():
    try:
        recent_data = processor.get_recent_data(20)
        if not recent_data:
            return pd.DataFrame()
        
        data_dicts = [data.to_dict() for data in recent_data]
        df = pd.DataFrame(data_dicts)
        
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%H:%M:%S')
        
        display_df = df[['transaction_id', 'timestamp', 'amount', 'risk_score', 'merchant_id', 'location', 'payment_method']].copy()
        display_df['risk_level'] = display_df['risk_score'].apply(
            lambda x: '🔴 HIGH' if x > 70 else '🟡 MEDIUM' if x > 30 else '🟢 LOW'
        )
        
        return display_df
    except Exception as e:
        return pd.DataFrame()

def get_infinia_events_table():
    try:
        recent_events = infinia_event_consumer.get_recent_events(15)
        if not recent_events:
            return pd.DataFrame()
        
        events_dicts = [event.to_dict() for event in recent_events]
        df = pd.DataFrame(events_dicts)
        
        df['timestamp'] = pd.to_datetime(df['timestamp']).dt.strftime('%H:%M:%S')
        
        return df[['bucket', 'data_category', 'event_type', 'timestamp', 'size']]
    except Exception as e:
        return pd.DataFrame()

def create_risk_score_chart():
    try:
        recent_data = processor.get_recent_data(50)
        if not recent_data:
            return go.Figure()
        
        df = pd.DataFrame([data.to_dict() for data in recent_data])
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        
        fig = px.scatter(df, x='timestamp', y='risk_score', 
                        color='risk_score',
                        color_continuous_scale=['green', 'yellow', 'red'],
                        title='Transaction Risk Scores Over Time',
                        hover_data=['transaction_id', 'amount'])
        
        fig.add_hline(y=70, line_dash="dash", line_color="red", annotation_text="High Risk Threshold")
        fig.add_hline(y=30, line_dash="dash", line_color="orange", annotation_text="Medium Risk Threshold")
        
        fig.update_layout(height=400)
        return fig
    except Exception as e:
        return go.Figure()

def get_infinia_files_list():
    try:
        files = processor.infinia_handler.list_recent_files(15)
        if not files:
            return pd.DataFrame()
        
        df = pd.DataFrame(files)
        df['last_modified'] = pd.to_datetime(df['last_modified']).dt.strftime('%H:%M:%S')
        
        return df[['key', 'category', 'size', 'last_modified']]
    except Exception as e:
        return pd.DataFrame()

def create_search_analytics_chart(analytics):
    try:
        if not analytics or 'risk_distribution' not in analytics:
            return go.Figure()
        
        # Risk distribution pie chart
        fig = go.Figure()
        
        labels = []
        values = []
        colors = []
        
        for risk_cat, count in analytics['risk_distribution'].items():
            labels.append(risk_cat.replace('_', ' ').title())
            values.append(count)
            if 'high' in risk_cat:
                colors.append('#E31E24')  # Red
            elif 'medium' in risk_cat:
                colors.append('#FF6B35')  # Orange
            else:
                colors.append('#28A745')  # Green
        
        fig.add_trace(go.Pie(
            labels=labels,
            values=values,
            marker_colors=colors,
            hole=.3,
            textinfo='label+percent',
            textposition='outside'
        ))
        
        fig.update_layout(
            title="Risk Distribution in Search Results",
            height=400,
            showlegend=True
        )
        
        return fig
    except Exception as e:
        return go.Figure()

# Create Gradio Interface
def create_gradio_interface():
    ddn_theme = DDNTheme()
    
    with gr.Blocks(
        title="DDN Infinia Fraud Detection Pipeline", 
        theme=ddn_theme,
        css="""
        .gradio-container {
            background: #FFFFFF;
        }
        
        .main-header {
            background: #E31E24;
            color: white;
            padding: 20px;
            border-radius: 8px;
            margin-bottom: 20px;
            text-align: center;
        }
        
        .main-header h1, .main-header p {
            color: white !important;
        }
        
        .metric-card {
            background: white;
            border: 2px solid #E31E24;
            border-radius: 8px;
            padding: 20px;
            margin: 10px 0;
        }
        
        .ddn-accent {
            color: #E31E24 !important;
            font-weight: bold;
        }
        
        .ddn-secondary {
            color: #FF6B35 !important;
            font-weight: bold;
        }
        
        .tab-nav button {
            background: white !important;
            color: #333333 !important;
            border: 2px solid #E31E24 !important;
            font-weight: bold;
        }
        
        .tab-nav button.selected {
            background: #E31E24 !important;
            color: white !important;
        }
        
        button:hover {
            opacity: 0.9;
        }
        """
    ) as demo:
        
        gr.HTML("""
        <div class="main-header">
            <div style="display: flex; align-items: center; justify-content: center; gap: 15px;">
                <div style="width: 40px; height: 40px; border-radius: 50%; background: linear-gradient(45deg, #E31E24 0%, #FF6B35 100%); display: flex; align-items: center; justify-content: center;">
                    <div style="width: 18px; height: 18px; border-radius: 50%; background: white;"></div>
                </div>
                <h1 style="margin: 0;">DDN Infinia Fraud Detection Pipeline</h1>
            </div>
            <p>Real-time transaction monitoring with DDN Infinia as the centralized data hub for ML analytics, pattern analysis, and anomaly detection</p>
        </div>
        """)
        
        with gr.Tabs():
            # Main Dashboard Tab
            with gr.Tab("🛡️ Fraud Dashboard", id="dashboard"):
                with gr.Row():
                    with gr.Column(scale=1):
                        gr.Markdown("## 🎛️ <span class='ddn-accent'>Pipeline Controls</span>")
                        
                        start_btn = gr.Button("Start Fraud Detection", variant="primary")
                        stop_btn = gr.Button("⏹️ Stop Pipeline", variant="secondary")
                        
                        status_output = gr.Textbox(
                            label="Status", 
                            placeholder="Fraud detection pipeline ready...",
                            interactive=False
                        )

                        
                        start_btn.click(start_pipeline, outputs=status_output)
                        stop_btn.click(stop_pipeline, outputs=status_output)
                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("## 💳 <span class='ddn-accent'>Live Transaction Data</span>")
                        transaction_table = gr.Dataframe(
                            headers=["Transaction ID", "Time", "Amount", "Risk Score", "Merchant", "Location", "Payment", "Risk Level"],
                            label="Recent Transactions"
                        )
                        
                        refresh_transactions_btn = gr.Button("🔄 Refresh Transactions")
                        refresh_transactions_btn.click(get_transaction_data_table, outputs=transaction_table)

                    
                    with gr.Column():
                        gr.Markdown("## 📊 <span class='ddn-secondary'>Risk Score Analysis</span>")
                        risk_chart = gr.Plot(label="Risk Score Distribution")
                        
                        refresh_chart_btn = gr.Button("🔄 Refresh Chart")
                        refresh_chart_btn.click(create_risk_score_chart, outputs=risk_chart)

                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("## 🗂️ <span class='ddn-accent'>DDN Infinia Hub Events</span>")
                        gr.Markdown("*Centralized data hub for ML analytics and pattern detection*")
                        infinia_events_table = gr.Dataframe(
                            headers=["Bucket", "Data Category", "Event Type", "Time", "Size"],
                            label="DDN Infinia Data Hub Events"
                        )
                        
                        refresh_events_btn = gr.Button("🔄 Refresh Hub Events")
                        refresh_events_btn.click(get_infinia_events_table, outputs=infinia_events_table)
                    
                    with gr.Column():
                        gr.Markdown("## 📁 <span class='ddn-secondary'>DDN Infinia Analytics Files</span>")
                        gr.Markdown("*Organized for ML training and anomaly detection*")
                        infinia_files_table = gr.Dataframe(
                            headers=["File Path", "Risk Category", "Size", "Stored"],
                            label="ML-Ready Data Files"
                        )
                        
                        refresh_files_btn = gr.Button("🔄 Refresh Files")
                        refresh_files_btn.click(get_infinia_files_list, outputs=infinia_files_table)

            
            # Metadata Search Tab - NEW
            with gr.Tab("🔍 Metadata Search", id="search"):
                gr.Markdown("## 🔍 <span class='ddn-accent'>DDN Infinia Metadata Search</span>")
                gr.Markdown("*Advanced transaction search using DDN Infinia's metadata capabilities*")
                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### 🎯 <span class='ddn-secondary'>Search Criteria</span>")
                        
                        with gr.Row():
                            risk_category_search = gr.Dropdown(
                                choices=["All", "High Risk", "Medium Risk", "Low Risk"],
                                value="All",
                                label="Risk Category"
                            )
                            
                            merchant_category_search = gr.Textbox(
                                label="Merchant Category",
                                placeholder="amazon, paypal, stripe...",
                                info="Partial match supported"
                            )
                        
                        with gr.Row():
                            location_search = gr.Textbox(
                                label="Location",
                                placeholder="New York, London, Tokyo..."
                            )
                            
                            payment_method_search = gr.Dropdown(
                                choices=["All", "credit_card", "debit_card", "paypal", "crypto", "bank_transfer", "mobile_wallet"],
                                value="All",
                                label="Payment Method"
                            )
                        
                        with gr.Row():
                            risk_score_min = gr.Number(
                                label="Min Risk Score",
                                minimum=0,
                                maximum=100,
                                placeholder="0"
                            )
                            
                            risk_score_max = gr.Number(
                                label="Max Risk Score", 
                                minimum=0,
                                maximum=100,
                                placeholder="100"
                            )
                        
                        with gr.Row():
                            amount_min = gr.Number(
                                label="Min Amount ($)",
                                minimum=0,
                                placeholder="0"
                            )
                            
                            amount_max = gr.Number(
                                label="Max Amount ($)",
                                minimum=0,
                                placeholder="10000"
                            )
                        
                        with gr.Row():
                            fraud_indicators_search = gr.Textbox(
                                label="Fraud Indicators",
                                placeholder="velocity_check_failed, geo_location_mismatch..."
                            )
                            
                            max_results_search = gr.Number(
                                label="Max Results",
                                value=50,
                                minimum=1,
                                maximum=500
                            )
                        
                        search_btn = gr.Button("🔍 Search Transactions", variant="primary")
                        clear_search_btn = gr.Button("🧹 Clear Filters", variant="secondary")
                    
                    with gr.Column():
                        gr.Markdown("### 📊 <span class='ddn-secondary'>Search Analytics</span>")
                        
                        search_analytics_chart = gr.Plot(label="Search Results Distribution")
                        
                        search_analytics_text = gr.Textbox(
                            label="Analytics Summary",
                            lines=10,
                            interactive=False
                        )
                
                with gr.Row():
                    search_status = gr.Textbox(
                        label="Search Status",
                        placeholder="Ready to search DDN Infinia metadata...",
                        interactive=False
                    )
                
                with gr.Row():
                    gr.Markdown("### 📋 <span class='ddn-accent'>Search Results</span>")
                    search_results_table = gr.Dataframe(
                        headers=["Transaction ID", "Risk Score", "Risk Category", "Amount", "Merchant", "Location", "File Size", "Stored"],
                        label="Matching Transactions"
                    )
                
                
                def clear_search_filters():
                    return ("All", "", "", "All", None, None, None, None, "", 50)
                
                def perform_search_and_analytics(risk_category, merchant_category, location, payment_method,
                                               risk_score_min, risk_score_max, amount_min, amount_max,
                                               fraud_indicators, max_results):
                    df, status, analytics = search_transactions_by_metadata(
                        risk_category, merchant_category, location, payment_method,
                        risk_score_min, risk_score_max, amount_min, amount_max,
                        fraud_indicators, max_results
                    )
                    
                    analytics_text = format_search_analytics(analytics)
                    analytics_chart = create_search_analytics_chart(analytics)
                    
                    return df, status, analytics_text, analytics_chart
                
                search_btn.click(
                    perform_search_and_analytics,
                    inputs=[risk_category_search, merchant_category_search, location_search, payment_method_search,
                           risk_score_min, risk_score_max, amount_min, amount_max,
                           fraud_indicators_search, max_results_search],
                    outputs=[search_results_table, search_status, search_analytics_text, search_analytics_chart]
                )
                
                clear_search_btn.click(
                    clear_search_filters,
                    outputs=[risk_category_search, merchant_category_search, location_search, payment_method_search,
                            risk_score_min, risk_score_max, amount_min, amount_max,
                            fraud_indicators_search, max_results_search]
                )
            
            # Kafka Topics Tab
            with gr.Tab("📡 Kafka Topics", id="kafka"):
                gr.Markdown("## 🔧 <span class='ddn-accent'>Kafka Topic Management</span>")
                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### ➕ <span class='ddn-secondary'>Create Topic</span>")
                        
                        topic_name_input = gr.Textbox(
                            label="Topic Name",
                            placeholder="fraud-transactions-new",
                            scale=2
                        )
                        
                        with gr.Row():
                            partitions_input = gr.Number(
                                label="Partitions",
                                value=3,
                                minimum=1,
                                maximum=50,
                                scale=1
                            )
                            replication_input = gr.Number(
                                label="Replication Factor",
                                value=1,
                                minimum=1,
                                maximum=3,
                                scale=1
                            )
                        
                        create_topic_btn = gr.Button("➕ Create Topic", variant="primary")
                        
                        gr.Markdown("### <span class='ddn-secondary'>Quick Setup</span>")
                        create_default_btn = gr.Button("⚡ Create Default Topics", variant="secondary")
                        gr.Markdown("*Creates 'fraud-transactions' and 'infinia-fraud-events' topics*")
                    
                    with gr.Column():
                        gr.Markdown("### 🗑️ <span class='ddn-accent'>Delete Topic</span>")
                        
                        topic_delete_input = gr.Textbox(
                            label="Topic Name to Delete",
                            placeholder="Enter topic name to delete"
                        )
                        
                        delete_topic_btn = gr.Button("🗑️ Delete Topic", variant="stop")
                        
                        gr.Markdown("### ⚠️ Warning")
                        gr.Markdown("Deleting removes all transaction data permanently.")
                
                with gr.Row():
                    topic_status = gr.Textbox(
                        label="Status",
                        placeholder="Topic operations status...",
                        interactive=False,
                        lines=3
                    )
                
                with gr.Row():
                    gr.Markdown("### 📋 <span class='ddn-secondary'>Current Topics</span>")
                    topics_table = gr.Dataframe(
                        headers=["Topic Name", "Status"],
                        label="Kafka Topics List"
                    )
                    
                    refresh_topics_btn = gr.Button("🔄 Refresh Topics")
                
                
                create_topic_btn.click(
                    create_kafka_topic,
                    inputs=[topic_name_input, partitions_input, replication_input],
                    outputs=[topic_status, topics_table]
                )
                
                delete_topic_btn.click(
                    delete_kafka_topic,
                    inputs=[topic_delete_input],
                    outputs=[topic_status, topics_table]
                )
                
                create_default_btn.click(
                    create_default_topics,
                    outputs=[topic_status, topics_table]
                )
                
                refresh_topics_btn.click(
                    refresh_kafka_topics,
                    outputs=topics_table
                )
            
            # DDN Infinia Configuration Tab
            with gr.Tab("☁️ DDN Infinia Hub", id="infinia"):
                gr.Markdown("## ☁️ <span class='ddn-accent'>DDN Infinia Data Hub Configuration</span>")
                gr.Markdown("Configure DDN Infinia as the centralized hub for fraud analytics, ML training, and pattern detection")
                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### 🔑 <span class='ddn-secondary'>Hub Credentials</span>")
                        
                        infinia_endpoint = gr.Textbox(
                            label="DDN Infinia Endpoint URL",
                            placeholder="https://your-infinia-endpoint.com",
                            info="Complete endpoint URL for your DDN Infinia cluster"
                        )
                        
                        infinia_access_key = gr.Textbox(
                            label="Access Key ID",
                            placeholder="Enter DDN Infinia Access Key",
                            type="password"
                        )
                        
                        infinia_secret_key = gr.Textbox(
                            label="Secret Access Key",
                            placeholder="Enter DDN Infinia Secret Key",
                            type="password"
                        )
                        
                        infinia_bucket_name = gr.Textbox(
                            label="Fraud Data Hub Bucket",
                            placeholder="fraud-detection-hub",
                            value="fraud-detection-hub"
                        )
                        
                        update_infinia_btn = gr.Button("🔄 Update Hub Credentials", variant="primary")
                        test_infinia_btn = gr.Button("🔍 Test Hub Connection", variant="secondary")
                        
                        gr.Markdown("""
                        **Centralized Analytics Hub:**
                        - ML-ready data organization
                        - Pattern analysis automation
                        - Anomaly detection datasets
                        - High-performance storage
                        - Advanced metadata search
                        """)
                    
                    with gr.Column():
                        gr.Markdown("### 📋 <span class='ddn-secondary'>Current Hub Configuration</span>")
                        
                        infinia_config_table = gr.Dataframe(
                            headers=["Setting", "Value"],
                            label="DDN Infinia Hub Settings"
                        )
                        
                        infinia_status = gr.Textbox(
                            label="Hub Status",
                            placeholder="DDN Infinia Hub operations status...",
                            interactive=False,
                            lines=4
                        )
                        
                        refresh_infinia_btn = gr.Button("🔄 Refresh Configuration")
                
                
                update_infinia_btn.click(
                    update_infinia_credentials,
                    inputs=[infinia_endpoint, infinia_access_key, infinia_secret_key, infinia_bucket_name],
                    outputs=[infinia_status, infinia_config_table]
                )
                
                test_infinia_btn.click(
                    test_infinia_connection,
                    outputs=infinia_status
                )
                
                refresh_infinia_btn.click(
                    get_current_infinia_config,
                    outputs=infinia_config_table
                )
            
            # Grafana Cloud Configuration Tab
            with gr.Tab("📊 Grafana Cloud", id="grafana"):
                gr.Markdown("## 📊 <span class='ddn-accent'>Grafana Cloud Monitoring</span>")
                gr.Markdown("Advanced fraud detection dashboards and real-time analytics")
                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### 🔑 <span class='ddn-secondary'>Grafana Cloud Credentials</span>")
                        
                        grafana_url = gr.Textbox(
                            label="Grafana Cloud URL",
                            placeholder="https://your-instance.grafana.net",
                            info="Your Grafana Cloud instance URL"
                        )
                        
                        grafana_api_key = gr.Textbox(
                            label="Grafana API Key",
                            placeholder="glsa_...",
                            type="password",
                            info="Service account token with dashboard permissions"
                        )
                        
                        grafana_org_id = gr.Textbox(
                            label="Organization ID",
                            placeholder="1",
                            value="1",
                            info="Grafana Cloud Organization ID"
                        )
                        
                        update_grafana_btn = gr.Button("🔄 Update Credentials", variant="primary")
                        test_grafana_btn = gr.Button("🔍 Test Connection", variant="secondary")
                        
                        gr.Markdown("### 📊 <span class='ddn-accent'>Dashboard Management</span>")
                        create_dashboard_btn = gr.Button("📊 Create Fraud Dashboard", variant="primary")
                    
                    with gr.Column():
                        gr.Markdown("### 📋 <span class='ddn-secondary'>Current Configuration</span>")
                        
                        grafana_config_table = gr.Dataframe(
                            headers=["Setting", "Value"],
                            label="Grafana Cloud Configuration"
                        )
                        
                        grafana_status = gr.Textbox(
                            label="Status",
                            placeholder="Grafana Cloud operations status...",
                            interactive=False,
                            lines=6
                        )
                        
                        refresh_grafana_btn = gr.Button("🔄 Refresh Configuration")
                
                update_grafana_btn.click(
                    update_grafana_credentials,
                    inputs=[grafana_url, grafana_api_key, grafana_org_id],
                    outputs=[grafana_status, grafana_config_table]
                )
                
                test_grafana_btn.click(
                    test_grafana_connection,
                    outputs=grafana_status
                )
                
                create_dashboard_btn.click(
                    create_grafana_dashboard,
                    outputs=grafana_status
                )
                
                refresh_grafana_btn.click(
                    get_current_grafana_config,
                    outputs=grafana_config_table
                )
            
            # Performance Analytics Tab
            with gr.Tab("📈 Analytics Hub", id="performance"):
                gr.Markdown("## 📈 <span class='ddn-accent'>DDN Infinia Analytics Hub Performance</span>")
                gr.Markdown("Monitor fraud detection pipeline performance and DDN Infinia data hub analytics")
                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### <span class='ddn-secondary'>Fraud Detection Metrics</span>")
                        
                        refresh_metrics_btn = gr.Button("🔄 Refresh Analytics", variant="primary")
                        
                        pipeline_stats = gr.Textbox(
                            label="Fraud Pipeline Statistics",
                            lines=12,
                            interactive=False
                        )
                        
                    with gr.Column():
                        gr.Markdown("### ⚡ <span class='ddn-accent'>DDN Infinia Hub Performance</span>")
                        
                        storage_metrics = gr.Textbox(
                            label="Data Hub & ML Analytics Performance",
                            lines=12,
                            interactive=False
                        )
                
                with gr.Row():
                    performance_chart = gr.Plot(label="Fraud Detection Performance")
                
                def get_fraud_pipeline_metrics():
                    try:
                        recent_data = processor.get_recent_data(100)
                        recent_events = infinia_event_consumer.get_recent_events(50)
                        
                        stats = f"🛡️ **Fraud Detection Pipeline:**\n\n"
                        stats += f" **Total Transactions:** {producer.message_count}\n"
                        stats += f" **Suspicious Transactions:** {processor.suspicious_count}\n"
                        stats += f" **Files in DDN Hub:** {processor.files_stored_count}\n"
                        stats += f" **Processing Errors:** {processor.error_count}\n"
                        stats += f" **Pipeline Status:** {'🟢 Active' if processor.is_running else '🔴 Stopped'}\n"
                        stats += f" **Recent Data Points:** {len(recent_data)}\n"
                        stats += f" **Hub Events:** {len(recent_events)}\n"
                        
                        if recent_data:
                            avg_risk = sum(d.risk_score for d in recent_data)/len(recent_data)
                            high_risk = sum(1 for d in recent_data if d.risk_score > 70)
                            avg_amount = sum(d.amount for d in recent_data)/len(recent_data)
                            stats += f"⚠️ **Average Risk Score:** {avg_risk:.1f}\n"
                            stats += f"🔴 **High Risk Count:** {high_risk}\n"
                            stats += f" **Average Amount:** ${avg_amount:.2f}\n"
                        
                        fraud_rate = (processor.suspicious_count / max(1, producer.message_count)) * 100
                        stats += f" **Fraud Detection Rate:** {fraud_rate:.2f}%\n"
                        stats += f" **Storage Success:** {processor.files_stored_count/max(1, producer.message_count)*100:.1f}%\n"
                        
                        return stats
                    except Exception as e:
                        return f"❌ Error getting fraud metrics: {str(e)}"
                
                def get_infinia_hub_performance():
                    try:
                        files = processor.infinia_handler.list_recent_files(20)
                        grafana_config = grafana_manager.get_current_config()
                        
                        metrics = f"🏦 **DDN Infinia Analytics Hub:**\n\n"
                        
                        metrics += f"📁 **ML-Ready Files:** {len(files)}\n"
                        metrics += f"🌐 **Hub Endpoint:** {config.INFINIA_ENDPOINT_URL.split('/')[-1] if config.INFINIA_ENDPOINT_URL != 'https://your-infinia-endpoint.com' else 'Not Configured'}\n"
                        metrics += f"🪣 **Fraud Hub Bucket:** {config.INFINIA_BUCKET_NAME}\n"
                        
                        if files:
                            total_size = sum(f['size'] for f in files)
                            categories = {}
                            for f in files:
                                cat = f.get('category', 'unknown')
                                categories[cat] = categories.get(cat, 0) + 1
                            
                            metrics += f" **Total Data Size:** {total_size/1024:.1f} KB\n"
                            metrics += f" **Avg File Size:** {total_size/len(files)/1024:.1f} KB\n"
                            metrics += f" **Data Categories:** {len(categories)}\n"
                            
                            for cat, count in categories.items():
                                emoji = "🔴" if "high" in cat else "🟡" if "medium" in cat else "🟢" if "low" in cat else "📊"
                                metrics += f"  {emoji} {cat.title()}: {count} files\n"
                        
                        metrics += f"\n📊 **Grafana Monitoring:**\n"
                        metrics += f"🌐 **Instance:** {grafana_config['grafana_url'].split('/')[-1] if 'grafana.net' in grafana_config['grafana_url'] else 'Not Configured'}\n"
                        metrics += f"🔑 **Status:** {'✅ Connected' if grafana_config['api_key'] != 'Not Set' else '❌ Not Configured'}\n"
                        metrics += f" **Live Metrics:** {'🟢 Active' if metrics_collector.is_running else '🔴 Inactive'}\n"
                        
                        metrics += f"\n🔍 **Metadata Search Capabilities:**\n"
                        metrics += f"• Advanced transaction filtering\n"
                        metrics += f"• Risk-based categorization\n"
                        metrics += f"• Geographic analysis\n"
                        metrics += f"• Payment method tracking\n"
                        metrics += f"• Real-time analytics\n"
                        
                        metrics += f"\n **Hub Advantages:**\n"
                        metrics += f"• Centralized fraud data repository\n"
                        metrics += f"• ML-optimized data organization\n"
                        metrics += f"• Real-time pattern analysis\n"
                        metrics += f"• Anomaly detection datasets\n"
                        metrics += f"• High-performance analytics\n"
                        
                        return metrics
                    except Exception as e:
                        return f"❌ Error getting hub metrics: {str(e)}"
                
                def create_fraud_performance_chart():
                    try:
                        recent_data = processor.get_recent_data(50)
                        if not recent_data:
                            return go.Figure()
                        
                        df = pd.DataFrame([data.to_dict() for data in recent_data])
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        
                        fig = go.Figure()
                        
                        # Risk score trend
                        fig.add_trace(go.Scatter(
                            x=df['timestamp'], 
                            y=df['risk_score'],
                            mode='lines+markers',
                            name='Risk Score',
                            line=dict(color='#E31E24', width=2)
                        ))
                        
                        # Transaction amount (scaled)
                        fig.add_trace(go.Scatter(
                            x=df['timestamp'], 
                            y=df['amount']/10,  # Scale for visibility
                            mode='lines+markers',
                            name='Amount ($÷10)',
                            line=dict(color='#FF6B35', width=2),
                            yaxis='y2'
                        ))
                        
                        fig.update_layout(
                            title='Real-Time Fraud Detection Performance',
                            xaxis_title='Time',
                            yaxis_title='Risk Score',
                            yaxis2=dict(
                                title='Transaction Amount (÷10)',
                                overlaying='y',
                                side='right'
                            ),
                            height=400,
                            hovermode='x unified'
                        )
                        
                        return fig
                    except Exception as e:
                        return go.Figure()
                
                def refresh_all_fraud_metrics():
                    return (
                        get_fraud_pipeline_metrics(),
                        get_infinia_hub_performance(),
                        create_fraud_performance_chart()
                    )
                
                refresh_metrics_btn.click(
                    refresh_all_fraud_metrics,
                    outputs=[pipeline_stats, storage_metrics, performance_chart]
                )
            
            # System Status Tab
            with gr.Tab("🔧 System Status", id="status"):
                gr.Markdown("## 🔧 <span class='ddn-accent'>System Health & Diagnostics</span>")
                gr.Markdown("Monitor fraud detection pipeline health and troubleshoot issues")
                
                with gr.Row():
                    with gr.Column():
                        gr.Markdown("### 🏥 <span class='ddn-secondary'>Component Health</span>")
                        
                        health_check_btn = gr.Button("🔍 Run Health Check", variant="primary")
                        
                        component_status = gr.Textbox(
                            label="Component Status",
                            lines=15,
                            interactive=False
                        )
                    
                    with gr.Column():
                        gr.Markdown("### 📊 <span class='ddn-accent'>System Information</span>")
                        
                        system_info = gr.Textbox(
                            label="System Information",
                            lines=15,
                            interactive=False
                        )
                        
                        refresh_system_btn = gr.Button("🔄 Refresh System Info")
                
                
                def run_fraud_health_check():
                    try:
                        status = "🏥 **FRAUD DETECTION SYSTEM HEALTH**\n\n"
                        
                        # Kafka Health
                        try:
                            topics = topic_manager.list_topics()
                            required_topics = [config.KAFKA_TOPIC_TRANSACTION_DATA, config.KAFKA_TOPIC_INFINIA_EVENTS]
                            missing_topics = [t for t in required_topics if t not in topics]
                            
                            if missing_topics:
                                status += f"❌ **Kafka Topics:** Missing: {', '.join(missing_topics)}\n"
                            else:
                                status += f"✅ **Kafka Topics:** All fraud topics present\n"
                        except Exception as e:
                            status += f"❌ **Kafka Connection:** Failed - {str(e)[:50]}...\n"
                        
                        # DDN Infinia Hub Health
                        try:
                            infinia_test = processor.infinia_handler.test_connection()
                            if infinia_test['success']:
                                status += f"✅ **DDN Infinia Hub:** Connected and ready\n"
                                status += f"🔍 **Metadata Search:** Available\n"
                            else:
                                status += f"❌ **DDN Infinia Hub:** {infinia_test['message'][:50]}...\n"
                        except Exception as e:
                            status += f"❌ **DDN Infinia Hub:** Failed - {str(e)[:50]}...\n"
                        
                        # Grafana Cloud Health
                        try:
                            grafana_test = grafana_manager.test_connection()
                            if grafana_test[0]:
                                status += f"✅ **Grafana Cloud:** Connected successfully\n"
                            else:
                                status += f"❌ **Grafana Cloud:** {grafana_test[1][:50]}...\n"
                        except Exception as e:
                            status += f"❌ **Grafana Cloud:** Failed - {str(e)[:50]}...\n"
                        
                        # Pipeline Components
                        status += f"\n🔧 **PIPELINE COMPONENTS:**\n"
                        status += f" **Transaction Producer:** {'🟢 Active' if producer.is_running else '🔴 Stopped'}\n"
                        status += f" **Fraud Processor:** {'🟢 Active' if processor.is_running else '🔴 Stopped'}\n"
                        status += f" **Event Consumer:** {'🟢 Active' if infinia_event_consumer.is_running else '🔴 Stopped'}\n"
                        status += f"📊 **Metrics Collector:** {'🟢 Active' if metrics_collector.is_running else '🔴 Stopped'}\n"
                        
                        # Performance Metrics
                        status += f"\n📈 **FRAUD DETECTION PERFORMANCE:**\n"
                        status += f" **Transactions Processed:** {producer.message_count}\n"
                        status += f" **Suspicious Detected:** {processor.suspicious_count}\n"
                        status += f" **Files in Hub:** {processor.files_stored_count}\n"
                        status += f"❌ **Errors:** {processor.error_count}\n"
                        
                        error_rate = (processor.error_count / max(1, producer.message_count)) * 100
                        fraud_rate = (processor.suspicious_count / max(1, producer.message_count)) * 100
                        
                        if error_rate < 1:
                            status += f"✅ **Error Rate:** {error_rate:.2f}% (Excellent)\n"
                        elif error_rate < 5:
                            status += f"⚠️ **Error Rate:** {error_rate:.2f}% (Warning)\n"
                        else:
                            status += f"❌ **Error Rate:** {error_rate:.2f}% (Critical)\n"
                        
                        status += f"🛡️ **Fraud Detection Rate:** {fraud_rate:.2f}%\n"
                        
                        # Recommendations
                        status += f"\n💡 **RECOMMENDATIONS:**\n"
                        if not producer.is_running:
                            status += f"• Start pipeline to begin fraud detection\n"
                        if processor.error_count > 0:
                            status += f"• Review error logs for issues\n"
                        if not metrics_collector.is_running:
                            status += f"• Configure Grafana for monitoring\n"
                        if processor.suspicious_count == 0 and producer.message_count > 50:
                            status += f"• Review fraud detection thresholds\n"
                        
                        return status
                        
                    except Exception as e:
                        return f"❌ Error running health check: {str(e)}"
                
                def get_fraud_system_info():
                    try:
                        import platform
                        try:
                            import psutil
                            psutil_available = True
                        except ImportError:
                            psutil_available = False
                        
                        info = "💻 **FRAUD DETECTION SYSTEM INFO**\n\n"
                        
                        # Platform info
                        info += f" **Platform:** {platform.system()} {platform.release()}\n"
                        info += f" **Python:** {platform.python_version()}\n"
                        info += f" **Architecture:** {platform.architecture()[0]}\n"
                        
                        # Resource usage
                        if psutil_available:
                            try:
                                info += f"\n📊 **RESOURCE USAGE:**\n"
                                info += f" **Memory:** {psutil.virtual_memory().percent:.1f}% used\n"
                                info += f" **CPU:** {psutil.cpu_percent(interval=1):.1f}% used\n"
                                
                                disk = psutil.disk_usage('/')
                                info += f" **Disk:** {(disk.used/disk.total)*100:.1f}% used\n"
                            except:
                                info += f" **Resource monitoring unavailable**\n"
                        else:
                            info += f"\n **Resource monitoring not available (install psutil)**\n"
                        
                        # Configuration
                        info += f"\n⚙️ **FRAUD PIPELINE CONFIGURATION:**\n"
                        info += f" **Kafka Servers:** {', '.join(config.KAFKA_BOOTSTRAP_SERVERS)}\n"
                        info += f" **Transaction Topic:** {config.KAFKA_TOPIC_TRANSACTION_DATA}\n"
                        info += f" **Hub Events Topic:** {config.KAFKA_TOPIC_INFINIA_EVENTS}\n"
                        info += f" **Infinia Hub:** {config.INFINIA_ENDPOINT_URL.split('/')[-1] if 'your-infinia-endpoint' not in config.INFINIA_ENDPOINT_URL else 'Not Configured'}\n"
                        info += f" **Grafana URL:** {config.GRAFANA_CLOUD_URL.split('/')[-1] if 'grafana.net' in config.GRAFANA_CLOUD_URL else 'Not Configured'}\n"
                        
                        # Environment
                        info += f"\n🌍 **ENVIRONMENT VARIABLES:**\n"
                        env_vars = ['INFINIA_ENDPOINT_URL', 'GRAFANA_CLOUD_URL', 'KAFKA_BOOTSTRAP_SERVERS']
                        for var in env_vars:
                            value = os.getenv(var, 'Not Set')
                            if value != 'Not Set' and len(value) > 30:
                                value = value[:27] + '...'
                            info += f"• {var}: {value}\n"
                        
                        # Data Hub Status
                        info += f"\n **DDN INFINIA HUB STATUS:**\n"
                        info += f" **Bucket:** {config.INFINIA_BUCKET_NAME}\n"
                        info += f" **Data Prefix:** {config.INFINIA_PREFIX}\n"
                        info += f" **SSL Verification:** Disabled (for Infinia compatibility)\n"
                        info += f" **Metadata Search:** Enabled\n"
                        
                        return info
                        
                    except Exception as e:
                        return f"❌ Error getting system info: {str(e)}"
                
                health_check_btn.click(run_fraud_health_check, outputs=component_status)
                refresh_system_btn.click(get_fraud_system_info, outputs=system_info)
        
        # Auto-refresh function for dashboard
        def auto_refresh():
            return (
                get_transaction_data_table(),
                get_infinia_events_table(),
                create_risk_score_chart(),
                get_infinia_files_list()
            )
        
        # Load initial data
        demo.load(auto_refresh, 
                 outputs=[transaction_table, infinia_events_table, risk_chart, infinia_files_table])
        
        demo.load(get_kafka_topics_list, outputs=topics_table)
        demo.load(get_current_infinia_config, outputs=infinia_config_table)
        demo.load(get_current_grafana_config, outputs=grafana_config_table)
        
        # Auto-refresh timer
        timer = gr.Timer(10)
        timer.tick(auto_refresh, 
                  outputs=[transaction_table, infinia_events_table, risk_chart, infinia_files_table])
    
    return demo

# Main execution
if __name__ == "__main__":
    demo = create_gradio_interface()
    
    demo.launch(
        server_name="127.0.0.1",
        server_port=7861,
        share=False,
        debug=True
    )
