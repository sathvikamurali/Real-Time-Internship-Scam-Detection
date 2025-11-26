# kafka_pipeline/consumer/postgres_consumer.py

from kafka import KafkaConsumer
import json
import sys
import os
import psycopg2
from datetime import datetime

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS, DB_CONFIG

def create_consumer():
    """Create Kafka consumer"""
    try:
        consumer = KafkaConsumer(
            TOPICS['job_postings'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            group_id='postgres-consumer-group',
            enable_auto_commit=False  # Manual commit after DB insert
        )
        print("✓ Kafka Consumer connected")
        return consumer
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)

def connect_db():
    """Connect to PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ PostgreSQL connected")
        return conn
    except Exception as e:
        print(f"❌ Failed to connect to PostgreSQL: {e}")
        sys.exit(1)

def insert_job_to_db(conn, job_data):
    """Insert job posting into PostgreSQL"""
    cursor = conn.cursor()
    
    try:
        # Detect suspicious patterns
        is_suspicious = 'suspicious_flags' in job_data and len(job_data.get('suspicious_flags', [])) > 0
        
        insert_query = """
        INSERT INTO jobs (
            job_title, company_name, location, job_description, requirements,
            salary_range, employment_type, experience_level, posted_date,
            application_url, company_profile, is_scam, scam_reasons,
            data_source, scraped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING
        RETURNING job_id
        """
        
        values = (
            job_data.get('job_title', ''),
            job_data.get('company_name', ''),
            job_data.get('location', ''),
            job_data.get('job_description', ''),
            job_data.get('requirements', ''),
            job_data.get('salary_range', ''),
            job_data.get('employment_type', ''),
            job_data.get('experience_level', ''),
            datetime.now().date(),
            job_data.get('application_url', ''),
            job_data.get('company_profile', ''),
            is_suspicious,
            job_data.get('suspicious_flags', []),
            job_data.get('data_source', 'kafka_stream'),
            datetime.now()
        )
        
        cursor.execute(insert_query, values)
        result = cursor.fetchone()
        conn.commit()
        
        if result:
            return result[0]  # job_id
        return None
        
    except Exception as e:
        print(f"❌ Failed to insert job: {e}")
        conn.rollback()
        return None
    finally:
        cursor.close()

def consume_and_store(consumer, conn):
    """Consume messages and store in PostgreSQL"""
    print("\n📥 Listening for messages and storing to PostgreSQL...")
    print("Press Ctrl+C to stop\n")
    
    message_count = 0
    stored_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            job_data = message.value
            
            print(f"📨 Received: {job_data.get('job_title')} at {job_data.get('company_name')}")
            
            # Insert to database
            job_id = insert_job_to_db(conn, job_data)
            
            if job_id:
                stored_count += 1
                print(f"✓ Stored to DB (job_id: {job_id})")
                
                # Check if suspicious
                if 'suspicious_flags' in job_data and job_data['suspicious_flags']:
                    print(f"  ⚠️  Marked as suspicious: {', '.join(job_data['suspicious_flags'])}")
            else:
                print(f"⚠️  Skipped (duplicate or error)")
            
            # Commit Kafka offset after successful DB insert
            consumer.commit()
            print()
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopped by user")
    finally:
        consumer.close()
        conn.close()
        print(f"\n✓ Consumer closed")
        print(f"📊 Statistics:")
        print(f"  Messages received: {message_count}")
        print(f"  Jobs stored: {stored_count}")

def main():
    print("=" * 70)
    print("💾 KAFKA → POSTGRESQL CONSUMER")
    print("=" * 70)
    
    consumer = create_consumer()
    conn = connect_db()
    
    consume_and_store(consumer, conn)

if __name__ == "__main__":
    main()