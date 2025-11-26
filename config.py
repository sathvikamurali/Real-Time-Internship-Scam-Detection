# kafka_pipeline/config.py

# Kafka Configuration
KAFKA_CONFIG = {
    'bootstrap_servers': ['localhost:9092'],
    'client_id': 'scamjobs-pipeline',
}

# Kafka Topics
TOPICS = {
    'job_postings': 'job-postings',
    'user_queries': 'user-queries',
    'scam_predictions': 'scam-predictions'
}

# PostgreSQL Configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin'  # UPDATE THIS if different
}

# Scraping Configuration
SCRAPING_CONFIG = {
    'max_jobs_per_run': 20,
    'delay_between_requests': 3,  # seconds
    'user_agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
}
# ---------------- LINKEDIN CREDENTIALS & SEARCH ----------------
LINKEDIN_CONFIG = {
    "email": "jkrethika@gmail.com",          # your LinkedIn email
    "password": "C9@-F-M33syyx_v",          # your LinkedIn password
    "search_url": "https://www.linkedin.com/jobs/search/?keywords=internship%20OR%20fresher%20OR%20trainee%20OR%20no%20experience&location=India&f_TPR=r86400"
}
