#!/bin/bash
ES_URL="https://192.168.1.113:9200"

curl -k -u elastic:$ES_PWD -X PUT "$ES_URL/_index_template/atm_tx_template" -H 'Content-Type: application/json' -d '{
  "index_patterns": ["atm-transactions-*"],
  "template": {
    "mappings": {
      "dynamic": "strict",
      "properties": {
        "transaction_id": {"type": "keyword"},
        "start_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time"},
        "end_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||strict_date_optional_time"},
        "duration_sec": {"type": "integer"},
        "cardholder_id": {"type": "keyword"},
        "atm_id": {"type": "keyword"},
        "location_name": {"type": "keyword"},
        "city": {"type": "keyword"},
        "state": {"type": "keyword"},
        "transaction_type": {"type": "keyword"},
        "amount": {"type": "scaled_float", "scaling_factor": 100},
        "account_type": {"type": "keyword"},
        "occupation": {"type": "keyword"},
        "gender": {"type": "keyword"},
        "is_holiday": {"type": "boolean"},
        "status": {"type": "keyword"},
        "run_id": {"type": "keyword"},
        "pipeline_version": {"type": "keyword"}
      }
    }
  }
}'
