# kafka_pipeline/consumer/test_consumer.py

from kafka import KafkaConsumer
import json
import sys
import os

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS

def create_consumer():
    """Create Kafka consumer"""
    try:
        consumer = KafkaConsumer(
            TOPICS['job_postings'],
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            auto_offset_reset='earliest',
            group_id='test-consumer-group',
            enable_auto_commit=True
        )
        print("✓ Kafka Consumer connected successfully")
        print(f"✓ Subscribed to topic: {TOPICS['job_postings']}\n")
        return consumer
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)

def consume_messages(consumer):
    """Consume and display messages"""
    print("📥 Listening for messages...")
    print("Press Ctrl+C to stop\n")
    
    message_count = 0
    
    try:
        for message in consumer:
            message_count += 1
            
            print(f"=" * 70)
            print(f"📨 Message {message_count}")
            print(f"=" * 70)
            print(f"Key: {message.key}")
            print(f"Partition: {message.partition}")
            print(f"Offset: {message.offset}")
            print(f"\nJob Details:")
            print(f"  Title: {message.value.get('job_title')}")
            print(f"  Company: {message.value.get('company_name')}")
            print(f"  Location: {message.value.get('location')}")
            print(f"  Employment Type: {message.value.get('employment_type')}")
            
            # Check for suspicious flags
            if 'suspicious_flags' in message.value and message.value['suspicious_flags']:
                print(f"\n⚠️  SUSPICIOUS JOB DETECTED!")
                print(f"  Flags: {', '.join(message.value['suspicious_flags'])}")
            else:
                print(f"\n✅ Job appears legitimate")
            
            print()
            
    except KeyboardInterrupt:
        print("\n\n⚠️  Stopped by user")
    finally:
        consumer.close()
        print(f"\n✓ Consumer closed. Total messages received: {message_count}")

def main():
    print("=" * 70)
    print("🧪 KAFKA CONSUMER TEST")
    print("=" * 70)
    
    consumer = create_consumer()
    consume_messages(consumer)

if __name__ == "__main__":
    main()