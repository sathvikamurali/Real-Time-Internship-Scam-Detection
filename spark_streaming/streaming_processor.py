# spark_streaming/streaming_processor.py 

import os
import sys
import pickle
import numpy as np

# Fix Windows Unicode encoding issues
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# SET BEFORE IMPORTING PYSPARK
os.environ['HADOOP_HOME'] = r"C:\hadoop"
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, BooleanType, ArrayType
import psycopg2

# Configuration
KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
KAFKA_TOPIC = "job-postings"

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin'
}

MODEL_PARAMS_PATH = r"C:\Rethika\Amrita\5th sem\big data analytics\part1\spark_streaming\model\rf_model_params.pkl"

# Define schema for Kafka messages
job_schema = StructType([
    StructField("job_id", StringType(), True),
    StructField("job_title", StringType(), True),
    StructField("company_name", StringType(), True),
    StructField("location", StringType(), True),
    StructField("job_description", StringType(), True),
    StructField("salary_range", StringType(), True),
    StructField("employment_type", StringType(), True),
    StructField("experience_level", StringType(), True),
    StructField("application_url", StringType(), True),
    StructField("posted_date", StringType(), True),
    StructField("scraped_at", StringType(), True),
    StructField("data_source", StringType(), True),
    StructField("has_company_logo", BooleanType(), True),
    StructField("telecommuting", BooleanType(), True),
    StructField("has_questions", BooleanType(), True),
    StructField("detected_keywords", ArrayType(StringType()), True),
    StructField("is_suspicious", BooleanType(), True)
])

def create_spark_session():
    """Create Spark session with proper Kafka package version"""
    
    winutils_path = r"C:\hadoop\bin\winutils.exe"
    if not os.path.exists(winutils_path):
        print("\n[ERROR] CRITICAL: winutils.exe not found!")
        print("   Download from: https://github.com/cdarlint/winutils/raw/master/hadoop-3.3.1/bin/winutils.exe")
        print("   Save to: C:\\hadoop\\bin\\winutils.exe")
        sys.exit(1)
    
    temp_dirs = [
        r"C:\tmp\spark-temp",
        r"C:\tmp\spark-warehouse", 
        r"C:\tmp\spark-checkpoint",
        r"C:\tmp\hive"
    ]
    
    for temp_dir in temp_dirs:
        os.makedirs(temp_dir, exist_ok=True)
    
    # Clear old checkpoint
    import shutil
    checkpoint_dir = r"C:\tmp\spark-checkpoint"
    if os.path.exists(checkpoint_dir):
        try:
            shutil.rmtree(checkpoint_dir)
        except:
            pass
    os.makedirs(checkpoint_dir, exist_ok=True)
    
    import pyspark
    pyspark_version = pyspark.__version__
    print(f"[INFO] PySpark version: {pyspark_version}")
    
    kafka_package = "org.apache.spark:spark-sql-kafka-0-10_2.13:4.0.1"
    additional_packages = [
        "org.apache.commons:commons-pool2:2.11.1",
        "org.apache.kafka:kafka-clients:3.7.0"
    ]
    
    all_packages = kafka_package + "," + ",".join(additional_packages)
    
    spark = SparkSession.builder \
        .appName("ScamJobsStreamingProcessor") \
        .master("local[*]") \
        .config("spark.jars.packages", all_packages) \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.host", "localhost") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.warehouse.dir", "file:///C:/tmp/spark-warehouse") \
        .config("spark.sql.shuffle.partitions", "4") \
        .config("spark.default.parallelism", "4") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .config("spark.driver.extraJavaOptions", "-Djava.io.tmpdir=C:/tmp") \
        .config("spark.executor.extraJavaOptions", "-Djava.io.tmpdir=C:/tmp") \
        .getOrCreate()
    
    print("[INFO] Spark session created successfully!")
    spark.sparkContext.setLogLevel("WARN")
    return spark

def load_model_params():
    """Load model parameters from pickle file"""
    print(f"\n[INFO] Loading model parameters from {MODEL_PARAMS_PATH}...")
    
    if not os.path.exists(MODEL_PARAMS_PATH):
        print(f"[WARN] Model params file not found: {MODEL_PARAMS_PATH}")
        return None
    
    try:
        with open(MODEL_PARAMS_PATH, 'rb') as f:
            params = pickle.load(f)
        
        print("[INFO] Model parameters loaded successfully!")
        print(f"  - Number of trees: {params.get('numTrees', 'N/A')}")
        print(f"  - Number of features: {params.get('numFeatures', 'N/A')}")
        
        return params
    except Exception as e:
        print(f"[ERROR] Error loading model params: {e}")
        return None

def enhanced_scam_detection(job_data):
    """FIXED: Enhanced rule-based scam detection with correct threshold"""
    import re
    
    text = f"{job_data.get('job_title', '')} {job_data.get('job_description', '')}".lower()
    
    # Define red flag patterns
    urgent_words = ['urgent', 'immediate', 'asap', 'hurry', 'now', 'today', 'quick', 'apply fast', 'limited time']
    fee_words = ['fee', 'payment', 'pay upfront', 'deposit', 'investment', 'cost', 'charge', 'registration fee', 'training fee']
    contact_words = ['whatsapp', 'telegram', 'personal email', 'gmail', 'yahoo', 'hotmail', 'contact directly']
    suspicious_words = ['guaranteed', 'easy money', 'work from home', 'no experience', 'earn money', 'unlimited earning', 'make money fast']
    
    # Count occurrences
    urgent_count = sum(text.count(word) for word in urgent_words)
    fee_count = sum(text.count(word) for word in fee_words)
    contact_count = sum(text.count(word) for word in contact_words)
    suspicious_count = sum(text.count(word) for word in suspicious_words)
    
    # Check for email/phone in description
    has_email = bool(re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text))
    has_phone = bool(re.search(r'\b\d{10}\b|\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', text))
    
    # Other suspicious indicators
    exclamation_count = text.count('!')
    capital_ratio = sum(1 for c in job_data.get('job_title', '') if c.isupper()) / max(len(job_data.get('job_title', 'x')), 1)
    no_salary = job_data.get('salary_range') in ['', None, 'N/A', 'Not disclosed']
    no_experience = job_data.get('experience_level') in ['', None, 'N/A', 'Not specified']
    
    # Calculate suspicion score (0-100)
    score = 0
    reasons = []
    
    # CRITICAL: Fee-related keywords are IMMEDIATE red flags
    if fee_count > 0:
        score += 40
        reasons.append(f'Contains {fee_count} fee/payment keywords')
    
    if urgent_count >= 2:
        score += 25
        reasons.append(f'Excessive urgency pressure ({urgent_count} instances)')
    
    if contact_count > 0:
        score += 30
        reasons.append(f'Suspicious contact methods ({contact_count} found)')
    
    if has_email:
        score += 20
        reasons.append('Email address in description')
    
    if has_phone:
        score += 20
        reasons.append('Phone number in description')
    
    if exclamation_count > 3:
        score += 15
        reasons.append(f'Excessive exclamation marks ({exclamation_count})')
    
    if capital_ratio > 0.5:
        score += 15
        reasons.append(f'Excessive capitalization')
    
    if suspicious_count > 0:
        score += 20
        reasons.append(f'Suspicious language patterns ({suspicious_count} found)')
    
    if no_salary and no_experience:
        score += 10
        reasons.append('Missing critical job information')
    
    # FIXED: Lower threshold to 35
    is_fraudulent = score >= 35
    confidence = min(score / 100.0, 1.0)
    
    return is_fraudulent, confidence, reasons, score

def save_to_postgres(batch_df, batch_id, model_params):
    """Save predictions to PostgreSQL - FIXED fraudulent field"""
    print(f"\n[INFO] Processing batch {batch_id}")
    
    if batch_df.count() == 0:
        print("[WARN] Empty batch, skipping...")
        return
    
    print(f"[INFO] Records in batch: {batch_df.count()}")
    
    pandas_df = batch_df.toPandas()
    
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
    except Exception as e:
        print(f"[ERROR] Database connection error: {e}")
        return
    
    inserted_count = 0
    scam_detected_count = 0
    
    for _, row in pandas_df.iterrows():
        try:
            job_data = {
                'job_title': row.get('job_title', ''),
                'job_description': row.get('job_description', ''),
                'salary_range': row.get('salary_range', ''),
                'experience_level': row.get('experience_level', ''),
                'has_company_logo': row.get('has_company_logo', False),
                'telecommuting': row.get('telecommuting', False),
                'has_questions': row.get('has_questions', False)
            }
            
            # FIXED: Make prediction with correct function
            is_fraudulent, confidence, scam_reasons, suspicion_score = enhanced_scam_detection(job_data)
            
            # CRITICAL FIX: Ensure fraudulent is actually set
            insert_query = """
            INSERT INTO jobs (
                external_job_id, job_title, company_name, location, job_description,
                salary_range, employment_type, experience_level,
                application_url, posted_date, data_source, scraped_at,
                has_company_logo, telecommuting, has_questions,
                fraudulent, scam_confidence, scam_reasons, suspicion_score
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (external_job_id) DO UPDATE SET
                fraudulent = EXCLUDED.fraudulent,
                scam_confidence = EXCLUDED.scam_confidence,
                scam_reasons = EXCLUDED.scam_reasons,
                suspicion_score = EXCLUDED.suspicion_score,
                updated_at = CURRENT_TIMESTAMP
            RETURNING id
            """
            
            cursor.execute(insert_query, (
                row.get('job_id', ''),
                row.get('job_title', ''),
                row.get('company_name', ''),
                row.get('location', ''),
                row.get('job_description', ''),
                row.get('salary_range', ''),
                row.get('employment_type', ''),
                row.get('experience_level', ''),
                row.get('application_url', ''),
                row.get('posted_date'),
                row.get('data_source', ''),
                row.get('scraped_at'),
                row.get('has_company_logo', False),
                row.get('telecommuting', False),
                row.get('has_questions', False),
                is_fraudulent,  # CRITICAL: This must be True for scams
                confidence,
                scam_reasons,
                suspicion_score
            ))
            
            result = cursor.fetchone()
            if result:
                inserted_count += 1
                if is_fraudulent:
                    scam_detected_count += 1
                    print(f"   [SCAM DETECTED] {row.get('job_title', 'N/A')[:50]}")
                    print(f"      Score: {suspicion_score}/100 | Confidence: {confidence:.0%}")
                    print(f"      Reasons: {', '.join(scam_reasons[:3])}")
                else:
                    print(f"   [SAFE] {row.get('job_title', 'N/A')[:50]} (Score: {suspicion_score}/100)")
            
        except psycopg2.errors.UniqueViolation:
            conn.rollback()
            try:
                update_query = """
                UPDATE jobs SET
                    fraudulent = %s,
                    scam_confidence = %s,
                    scam_reasons = %s,
                    suspicion_score = %s,
                    updated_at = CURRENT_TIMESTAMP
                WHERE external_job_id = %s
                """
                cursor.execute(update_query, (is_fraudulent, confidence, scam_reasons, suspicion_score, row.get('job_id', '')))
                print(f"   [UPDATED] {row.get('job_title', 'N/A')[:50]}")
            except Exception as e:
                print(f"   [WARN] Update error: {e}")
                conn.rollback()
                continue
            
        except Exception as e:
            print(f"[WARN] Error processing job: {e}")
            conn.rollback()
            continue
    
    conn.commit()
    cursor.close()
    conn.close()
    
    print(f"[SUCCESS] Batch {batch_id} complete: {inserted_count} processed, {scam_detected_count} scams detected")

def process_stream(spark, model_params):
    """Process Kafka stream with ML predictions"""
    print("[INFO] Starting Kafka stream consumer...")
    
    checkpoint_location = "C:/tmp/spark-checkpoint"
    
    try:
        df = spark.readStream \
            .format("kafka") \
            .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
            .option("subscribe", KAFKA_TOPIC) \
            .option("startingOffsets", "latest") \
            .option("failOnDataLoss", "false") \
            .option("maxOffsetsPerTrigger", "100") \
            .option("kafkaConsumer.pollTimeoutMs", "512") \
            .load()
        
        parsed_df = df.select(
            from_json(col("value").cast("string"), job_schema).alias("data")
        ).select("data.*")
        
        print("[SUCCESS] Kafka stream connected!")
        print("[INFO] Ready to process incoming job postings...\n")
        
        query = parsed_df.writeStream \
            .foreachBatch(lambda batch_df, batch_id: save_to_postgres(batch_df, batch_id, model_params)) \
            .outputMode("append") \
            .option("checkpointLocation", checkpoint_location) \
            .trigger(processingTime='5 seconds') \
            .start()
    
        print("[INFO] Streaming started! Waiting for messages...\n")
        print("Press Ctrl+C to stop...\n")
        query.awaitTermination()
        
    except Exception as e:
        print(f"[ERROR] Streaming error: {e}")
        import traceback
        traceback.print_exc()
        raise

def main():
    print("="*70)
    print("SPARK STREAMING SCAM DETECTOR")
    print("="*70)
    
    model_params = load_model_params()
    if model_params is None:
        print("[WARN] Cannot start without model parameters!")
        return
    
    print("\n[INFO] Using enhanced rule-based prediction (Threshold: 35/100)")
    
    spark = create_spark_session()
    
    try:
        process_stream(spark, model_params)
        
    except KeyboardInterrupt:
        print("\n[INFO] Stopping stream...")
    except Exception as e:
        print(f"\n[ERROR] Error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        spark.stop()
        print("[SUCCESS] Spark session closed")

if __name__ == "__main__":
    main()