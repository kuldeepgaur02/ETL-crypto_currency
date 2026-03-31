"""
Crypto Producer DAG — Full Pipeline
=====================================
- Fetches crypto prices from CoinGecko every ~30 seconds (2 fetches per 1-min DAG run)
- Validates API response (data quality checks)
- Checks Kafka health before pushing
- Pushes to Kafka with dead letter handling for failed records
- Verifies data landed in Kafka
- Logs all metrics to PostgreSQL (run count, records pushed, latency)
- Email alerts on failure/retry
- SLA monitoring (alerts if task exceeds expected duration)
- All credentials read from environment variables
"""


import os
from datetime import datetime, timedelta
import json
import time
import logging


import requests
import psycopg2
from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.errors import KafkaError


from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email



# ════════════════════════════════════════════
# Configuration — all from environment
# ════════════════════════════════════════════


KAFKA_BROKER = "kafka:9092"
KAFKA_TOPIC = "crypto-prices"


POSTGRES_CONN = {
   "host": os.getenv("POSTGRES_HOST", "postgres"),
   "port": int(os.getenv("POSTGRES_PORT", 5432)),
   "dbname": os.getenv("POSTGRES_DB"),
   "user": os.getenv("POSTGRES_USER"),
   "password": os.getenv("POSTGRES_PASSWORD"),
}


API_KEY = os.getenv("API_KEY")
ALERT_EMAIL = os.getenv("ALERT_EMAIL")


COINGECKO_URL = "https://api.coingecko.com/api/v3/coins/markets"
PARAMS = {
   "vs_currency": "usd",
   "ids": ",".join([
       "bitcoin", "ethereum", "solana", "cardano", "ripple",
       "dogecoin", "polkadot", "binancecoin", "avalanche", "chainlink",
       "polygon", "cosmos", "uniswap", "litecoin", "stellar",
       "vechain", "shiba-inu", "tron", "tezos", "neo",
   ]),
   "order": "market_cap_desc",
   "per_page": 20,
   "page": 1,
   "sparkline": "false",
   "price_change_percentage": "24h",
}


DESIRED_KEYS = [
   "id", "symbol", "current_price", "market_cap",
   "total_volume", "high_24h", "low_24h", "last_updated",
]


EXPECTED_COIN_COUNT = 20


logger = logging.getLogger(__name__)




# ════════════════════════════════════════════
# Helper: Log metrics to PostgreSQL
# ════════════════════════════════════════════


def log_metric(run_id, task, status, records_pushed=0, latency_ms=0, error_message=None):
   try:
       conn = psycopg2.connect(**POSTGRES_CONN)
       cur = conn.cursor()
       cur.execute(
           """
           INSERT INTO pipeline_metrics (run_id, task, status, records_pushed, latency_ms, error_message)
           VALUES (%s, %s, %s, %s, %s, %s)
           """,
           (run_id, task, status, records_pushed, latency_ms, error_message),
       )
       conn.commit()
       cur.close()
       conn.close()
   except Exception as e:
       logger.error(f"Failed to log metric: {e}")




def log_dead_letter(run_id, payload, error_message):
   try:
       conn = psycopg2.connect(**POSTGRES_CONN)
       cur = conn.cursor()
       cur.execute(
           """
           INSERT INTO dead_letter_queue (run_id, payload, error_message)
           VALUES (%s, %s, %s)
           """,
           (run_id, json.dumps(payload) if payload else None, error_message),
       )
       conn.commit()
       cur.close()
       conn.close()
   except Exception as e:
       logger.error(f"Failed to log dead letter: {e}")




# ════════════════════════════════════════════
# Callback functions
# ════════════════════════════════════════════


def on_failure_callback(context):
   task_id = context["task_instance"].task_id
   dag_id = context["task_instance"].dag_id
   exec_date = context["execution_date"]
   error = context.get("exception", "Unknown error")
   logger.error(f"TASK FAILED: {dag_id}.{task_id} at {exec_date} — {error}")




def on_success_callback(context):
   dag_id = context["task_instance"].dag_id
   exec_date = context["execution_date"]
   subject = f"DAG Success: {dag_id}"
   body = f"<h3>DAG {dag_id} completed successfully</h3><p>Execution date: {exec_date}</p>"
   try:
       send_email(to=[ALERT_EMAIL], subject=subject, html_content=body)
   except Exception as e:
       logger.error(f"Failed to send success email: {e}")
   logger.info(f"DAG SUCCESS: {dag_id} at {exec_date}")




def on_retry_callback(context):
   task_id = context["task_instance"].task_id
   try_number = context["task_instance"].try_number
   logger.warning(f"TASK RETRY: {task_id} — attempt {try_number}")




def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
   missed = [str(t) for t in task_list]
   logger.error(f"SLA MISS: tasks {missed} exceeded their expected duration")
   
   
# ════════════════════════════════════════════
# Task 1: Check Kafka health
# ════════════════════════════════════════════

def check_kafka_health(**context):
   run_id = context["run_id"]
   start = time.time()


   try:
       admin = KafkaAdminClient(bootstrap_servers=[KAFKA_BROKER], request_timeout_ms=10000)
       topics = admin.list_topics()
       admin.close()


       if KAFKA_TOPIC not in topics:
           raise Exception(f"Topic '{KAFKA_TOPIC}' not found. Available: {topics}")


       latency = int((time.time() - start) * 1000)
       log_metric(run_id, "check_kafka_health", "success", latency_ms=latency)
       logger.info(f"Kafka healthy — topic '{KAFKA_TOPIC}' exists. Latency: {latency}ms")


   except Exception as e:
       latency = int((time.time() - start) * 1000)
       log_metric(run_id, "check_kafka_health", "failure", latency_ms=latency, error_message=str(e))
       raise
    
    
# ════════════════════════════════════════════
# Task 2: Fetch, validate, and push to Kafka
# ════════════════════════════════════════════


def fetch_and_push(**context):
   run_id = context["run_id"]
   total_pushed = 0


   for batch in range(2):
       start = time.time()
       batch_label = f"batch_{batch + 1}"


       try:
           # Fetch from CoinGecko (Pro API with key)
           headers = {"x-cg-demo-api-key": API_KEY}
           response = requests.get(COINGECKO_URL, params=PARAMS, headers=headers, timeout=15)
           response.raise_for_status()
           data = response.json()


           # Data quality checks
           if not isinstance(data, list):
               raise ValueError(f"Expected list from API, got {type(data).__name__}")


           if len(data) == 0:
               raise ValueError("API returned empty list — possible rate limit or outage")


           if len(data) < EXPECTED_COIN_COUNT:
               logger.warning(
                   f"Expected {EXPECTED_COIN_COUNT} coins, got {len(data)}. "
                   "Some coins may be delisted or API may be throttled."
               )


           # Validate each coin
           valid_coins = []
           for coin in data:
               missing = [k for k in ["id", "symbol", "current_price"] if coin.get(k) is None]
               if missing:
                   logger.warning(f"Coin {coin.get('id', '?')} missing fields: {missing} — skipping")
                   log_dead_letter(run_id, coin, f"Missing required fields: {missing}")
                   continue


               if not isinstance(coin["current_price"], (int, float)) or coin["current_price"] <= 0:
                   logger.warning(f"Coin {coin['id']} has invalid price: {coin['current_price']} — skipping")
                   log_dead_letter(run_id, coin, f"Invalid price: {coin['current_price']}")
                   continue


               valid_coins.append({key: coin.get(key) for key in DESIRED_KEYS})


           if len(valid_coins) == 0:
               raise ValueError("No valid coins after quality checks")


           # Push to Kafka
           producer = KafkaProducer(
               bootstrap_servers=[KAFKA_BROKER],
               value_serializer=lambda v: json.dumps(v).encode("utf-8"),
               request_timeout_ms=10000,
               retries=3,
           )


           payload = {
               "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
               "data": valid_coins,
           }


           future = producer.send(KAFKA_TOPIC, value=payload)
           record_metadata = future.get(timeout=10)


           producer.flush()
           producer.close()


           latency = int((time.time() - start) * 1000)
           total_pushed += len(valid_coins)


           log_metric(
               run_id, f"fetch_and_push.{batch_label}", "success",
               records_pushed=len(valid_coins), latency_ms=latency,
           )
           logger.info(
               f"[{batch_label}] Pushed {len(valid_coins)} records to "
               f"partition {record_metadata.partition} offset {record_metadata.offset}. "
               f"Latency: {latency}ms"
           )


       except requests.exceptions.RequestException as e:
           latency = int((time.time() - start) * 1000)
           log_metric(run_id, f"fetch_and_push.{batch_label}", "failure", latency_ms=latency, error_message=str(e))
           log_dead_letter(run_id, None, f"API request failed: {e}")
           logger.error(f"[{batch_label}] API error: {e}")
           raise


       except KafkaError as e:
           latency = int((time.time() - start) * 1000)
           log_metric(run_id, f"fetch_and_push.{batch_label}", "failure", latency_ms=latency, error_message=str(e))
           log_dead_letter(run_id, None, f"Kafka push failed: {e}")
           logger.error(f"[{batch_label}] Kafka error: {e}")
           raise


       except Exception as e:
           latency = int((time.time() - start) * 1000)
           log_metric(run_id, f"fetch_and_push.{batch_label}", "failure", latency_ms=latency, error_message=str(e))
           logger.error(f"[{batch_label}] Unexpected error: {e}")
           raise


       # Wait 30 seconds before second fetch
       if batch < 1:
           logger.info("Waiting 30 seconds before next fetch...")
           time.sleep(30)


   context["ti"].xcom_push(key="total_pushed", value=total_pushed)
   
 

# ════════════════════════════════════════════
# Task 3: Verify data landed in Kafka
# ════════════════════════════════════════════


def verify_kafka_delivery(**context):
   run_id = context["run_id"]
   total_pushed = context["ti"].xcom_pull(task_ids="fetch_and_push_to_kafka", key="total_pushed")
   start = time.time()


   try:
       consumer = KafkaConsumer(
           KAFKA_TOPIC,
           bootstrap_servers=[KAFKA_BROKER],
           auto_offset_reset="latest",
           consumer_timeout_ms=15000,
           value_deserializer=lambda v: json.loads(v.decode("utf-8")),
           group_id=f"verify-{run_id}",
       )


       consumer.poll(timeout_ms=5000)
       
       for tp in consumer.assignment():
           end_offset = consumer.end_offsets([tp])[tp]
           if end_offset > 0:
               consumer.seek(tp, end_offset - 1)


       messages = consumer.poll(timeout_ms=10000)
       consumer.close()


       msg_count = sum(len(msgs) for msgs in messages.values())

       if msg_count == 0:
           raise Exception("No messages found in Kafka after push")


       latest_msg = None
       for msgs in messages.values():
           for msg in msgs:
               latest_msg = msg.value


       if latest_msg and "data" in latest_msg:
           record_count = len(latest_msg["data"])
           msg_timestamp = latest_msg.get("timestamp", "unknown")
           latency = int((time.time() - start) * 1000)


           log_metric(
               run_id, "verify_kafka_delivery", "success",
               records_pushed=record_count, latency_ms=latency,
           )
           logger.info(
               f"Verified: {record_count} records in Kafka. "
               f"Message timestamp: {msg_timestamp}. Latency: {latency}ms"
           )
       else:
           raise Exception(f"Unexpected message format: {latest_msg}")


   except Exception as e:
       latency = int((time.time() - start) * 1000)
       log_metric(run_id, "verify_kafka_delivery", "failure", latency_ms=latency, error_message=str(e))
       raise


#===================================
# Task 4  : log summary 
#===================================


def log_run_summary(**context):
    run_id = context["run_id"]
    total_pushed = context["ti"].xcom_pull(task_ids="fetch_and_push_to_kafka", key="total_pushed") or 0 
    
    
    log_metric(run_id , "dag_completed" ,"success",records_pushed=total_pushed)
    logger.info(f"DAG run complete , Total record pushed ::{total_pushed}")
    
    
#=======================================
# DAG Defination 
#=======================================


default_args  ={
    "owner" :"airflow",
    "retries" : 3,
    "retry_delay" : timedelta(seconds= 15),
    "email" : {ALERT_EMAIL},
    "email_on_failure" : True ,
    "email_on_retry" : True ,
    "email_on_success" : True ,
    "on_failure_callback" : on_failure_callback,
    "on_retry_callback" : on_retry_callback
}

with DAG (
    dag_id ="Crypto_Producer",
    default_args = default_args,
    description = "Fetch crypto prices from coingeeko ",
    schedule_interval = "*/1 * * * *",
    start_date = datetime(2026,3,18),
    catchup = False,
    tags = ["crypto","kafka","producer"],
    sla_miss_callback = sla_miss_callback,
    on_success_callback = on_success_callback,
    max_active_runs = 1
) as dag :
    
    t1_kafka_health = PythonOperator(
        task_id ="check_kafka_health",
        python_callable = check_kafka_health,
        sla = timedelta(seconds = 30 ),
    )
    t2_fetch_push = PythonOperator(
        task_id ="fetch_and_push_to_kafka",
        python_callable = fetch_and_push,
        sla = timedelta(seconds = 50 ),
    )
    t3_verify = PythonOperator(
        task_id ="verify_kafka_delivery",
        python_callable = verify_kafka_delivery,
        sla = timedelta(seconds = 30 ),
    )
    t4_summary = PythonOperator(
        task_id ="log_run_summary",
        python_callable = log_run_summary,
        sla = timedelta(seconds = 10 ),
    )
    
    t1_kafka_health >> t2_fetch_push >> t3_verify >> t4_summary
    
    