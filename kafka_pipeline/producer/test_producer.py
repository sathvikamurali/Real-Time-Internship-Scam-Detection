# kafka_pipeline/producer/test_producer.py

from kafka import KafkaProducer
import json
import time
from datetime import datetime
import sys
import os

# Add parent directory to path
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import KAFKA_CONFIG, TOPICS

def create_producer():
    """Create Kafka producer with JSON serialization"""
    try:
        producer = KafkaProducer(
            bootstrap_servers=KAFKA_CONFIG['bootstrap_servers'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            key_serializer=lambda k: k.encode('utf-8') if k else None,
            acks='all',  # Wait for all replicas
            retries=3
        )
        print("✓ Kafka Producer connected successfully")
        return producer
    except Exception as e:
        print(f"❌ Failed to connect to Kafka: {e}")
        sys.exit(1)

def send_test_messages(producer, num_messages=10):
    """Send test job postings to Kafka"""
    
    test_jobs = [
        {
            'job_title': 'Software Engineer Intern',
            'company_name': 'Tech Corp',
            'location': 'Bangalore',
            'job_description': 'Looking for talented interns to join our team. Will work on real projects with mentorship.',
            'requirements': 'Python, Java basics, Good problem-solving skills',
            'salary_range': '15000-25000',
            'employment_type': 'Internship',
            'experience_level': 'Entry Level',
            'data_source': 'test_producer',
            'company_profile': 'Leading tech company with 500+ employees'
        },
        {
            'job_title': 'Data Science Intern - URGENT HIRING',
            'company_name': 'Quick Money Ltd',
            'location': 'Work From Home',
            'job_description': 'Earn 50000 per month! No experience needed. Pay registration fee of 5000 to get started. Limited slots available!',
            'requirements': 'Just basic computer knowledge. Deposit required.',
            'salary_range': '50000-100000',
            'employment_type': 'Full Time',
            'experience_level': 'Entry Level',
            'data_source': 'test_producer',
            'company_profile': '',
            'suspicious_flags': ['high_salary', 'registration_fee', 'urgent', 'pressure_tactics']
        },
        {
            'job_title': 'Marketing Intern',
            'company_name': 'ABC Solutions',
            'location': 'Mumbai',
            'job_description': 'Internship opportunity in digital marketing. Learn SEO, social media marketing, and content creation.',
            'requirements': 'Good communication skills, creative mindset',
            'salary_range': '10000-15000',
            'employment_type': 'Internship',
            'experience_level': 'Fresher',
            'data_source': 'test_producer',
            'company_profile': 'Marketing agency with 50+ clients'
        },
        {
            'job_title': 'Online Form Filling Job',
            'company_name': '',
            'location': 'Remote',
            'job_description': 'Fill online forms and earn guaranteed income. Immediate joining. Click here now!',
            'requirements': 'Investment of 3000 required to start',
            'salary_range': '30000-50000',
            'employment_type': 'Part Time',
            'experience_level': 'Any',
            'data_source': 'test_producer',
            'company_profile': '',
            'suspicious_flags': ['missing_company', 'guaranteed_income', 'investment_required']
        },
        {
            'job_title': 'Web Development Intern',
            'company_name': 'Infosys',
            'location': 'Pune',
            'job_description': 'Work on web development projects using modern frameworks. Training provided.',
            'requirements': 'HTML, CSS, JavaScript basics. ReactJS is a plus',
            'salary_range': '12000-18000',
            'employment_type': 'Internship',
            'experience_level': 'Fresher',
            'data_source': 'test_producer',
            'company_profile': 'Global IT services company'
        }
    ]
    
    print(f"\n📤 Sending {num_messages} test messages to Kafka...")
    print(f"Topic: {TOPICS['job_postings']}\n")
    
    success_count = 0
    
    for i in range(num_messages):
        # Rotate through test jobs
        job = test_jobs[i % len(test_jobs)].copy()
        
        # Add metadata
        job['posted_date'] = datetime.now().isoformat()
        job['scraped_at'] = datetime.now().isoformat()
        job['message_id'] = f"test_{i+1}"
        
        # Send to Kafka
        try:
            future = producer.send(
                TOPICS['job_postings'],
                key=f"job_{i+1}",
                value=job
            )
            
            # Wait for confirmation
            record_metadata = future.get(timeout=10)
            
            # Show status
            status = "⚠️  SUSPICIOUS" if 'suspicious_flags' in job and job['suspicious_flags'] else "✅ LEGIT"
            print(f"{status} - Sent [{i+1}/{num_messages}]: {job['job_title']} at {job.get('company_name', 'Unknown')}")
            print(f"  → Partition: {record_metadata.partition}, Offset: {record_metadata.offset}")
            
            if 'suspicious_flags' in job and job['suspicious_flags']:
                print(f"  → Flags: {', '.join(job['suspicious_flags'])}")
            
            success_count += 1
            
        except Exception as e:
            print(f"❌ Failed to send message {i+1}: {e}")
        
        time.sleep(0.5)  # Small delay between messages
    
    # Flush to ensure all messages are sent
    producer.flush()
    print(f"\n✅ Successfully sent {success_count}/{num_messages} messages")

def main():
    print("=" * 70)
    print("🧪 KAFKA PRODUCER TEST")
    print("=" * 70)
    
    # Create producer
    producer = create_producer()
    
    # Send test messages
    try:
        send_test_messages(producer, num_messages=10)
    except KeyboardInterrupt:
        print("\n\n⚠️  Interrupted by user")
    finally:
        producer.close()
        print("\n✓ Producer closed")

if __name__ == "__main__":
    main()