import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from datetime import datetime
import os

# Local PostgreSQL connection
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',  # Change if different
    'password': 'radmin'  # UPDATE THIS
}

def connect_db():
    """Connect to local PostgreSQL"""
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        print("✓ Connected to PostgreSQL")
        return conn
    except Exception as e:
        print(f"❌ Connection failed: {e}")
        exit(1)

def load_csv_data(csv_path):
    """Load and inspect the CSV file"""
    print(f"\n📂 Loading CSV from: {csv_path}")
    
    if not os.path.exists(csv_path):
        print(f"❌ File not found: {csv_path}")
        exit(1)
    
    df = pd.read_csv(csv_path)
    
    print(f"✓ Found {len(df)} records")
    print(f"\n📋 Columns in CSV:")
    for i, col in enumerate(df.columns, 1):
        print(f"  {i}. {col}")
    
    print(f"\n📊 Data types:")
    print(df.dtypes)
    
    print(f"\n🎯 Fraudulent distribution:")
    print(df['fraudulent'].value_counts())
    
    print(f"\n📝 Sample records:")
    print(df.head(2))
    
    return df

def prepare_data(df):
    """Clean and prepare data for insertion"""
    print("\n🧹 Cleaning data...")
    
    # Replace NaN with appropriate defaults
    df = df.fillna({
        'title': '',
        'location': '',
        'department': '',
        'salary_range': '',
        'company_profile': '',
        'description': '',
        'requirements': '',
        'benefits': '',
        'employment_type': '',
        'required_experience': '',
        'required_education': '',
        'industry': '',
        'function': '',
        'telecommuting': 0,
        'has_company_logo': 0,
        'has_questions': 0,
        'fraudulent': 0
    })
    
    # Convert boolean columns
    df['telecommuting'] = df['telecommuting'].astype(bool)
    df['has_company_logo'] = df['has_company_logo'].astype(bool)
    df['has_questions'] = df['has_questions'].astype(bool)
    df['fraudulent'] = df['fraudulent'].astype(bool)
    
    # Check for salary info
    df['has_salary'] = df['salary_range'].str.len() > 0
    
    # Check for company profile
    df['has_company_profile'] = df['company_profile'].str.len() > 0
    
    print(f"✓ Data cleaned")
    return df

def insert_to_postgres(conn, df):
    """Insert data into PostgreSQL"""
    print("\n💾 Inserting into PostgreSQL...")
    
    cursor = conn.cursor()
    
    # Prepare records
    records = []
    for _, row in df.iterrows():
        record = (
            row['title'],                    # job_title
            row['company_profile'][:500] if len(row['company_profile']) > 0 else 'Unknown',  # company_name (extracted from profile)
            row['location'],                 # location
            row['description'],              # job_description
            row['requirements'],             # requirements
            row['salary_range'],             # salary_range
            row['employment_type'],          # employment_type
            row['required_experience'],      # experience_level
            datetime.now().date(),           # posted_date (we don't have actual dates)
            '',                              # application_url (not in dataset)
            row['company_profile'],          # company_profile
            row['fraudulent'],               # is_scam
            None,                            # scam_confidence
            None,                            # scam_reasons
            'susjobs_kaggle',                # data_source
            datetime.now(),                  # scraped_at
            row['has_salary'],               # has_salary
            row['has_company_logo'],         # has_company_logo
            row['telecommuting'],            # telecommuting
            row['has_company_profile'],      # has_company_profile
            row['has_questions'],            # has_questions
            row['fraudulent']                # fraudulent
        )
        records.append(record)
    
    # Batch insert
    insert_query = """
    INSERT INTO jobs (
        job_title, company_name, location, job_description, requirements,
        salary_range, employment_type, experience_level, posted_date,
        application_url, company_profile, is_scam, scam_confidence,
        scam_reasons, data_source, scraped_at, has_salary, has_company_logo,
        telecommuting, has_company_profile, has_questions, fraudulent
    ) VALUES %s
    ON CONFLICT DO NOTHING
    """
    
    try:
        execute_values(cursor, insert_query, records, page_size=1000)
        conn.commit()
        print(f"✓ Inserted {cursor.rowcount} records successfully")
    except Exception as e:
        print(f"❌ Insertion failed: {e}")
        conn.rollback()
        raise
    finally:
        cursor.close()

def verify_insertion(conn):
    """Verify data was inserted correctly"""
    print("\n🔍 Verifying data...")
    
    cursor = conn.cursor()
    
    # Total count
    cursor.execute("SELECT COUNT(*) FROM jobs")
    total = cursor.fetchone()[0]
    print(f"\n📊 Total jobs in database: {total}")
    
    # By data source
    cursor.execute("""
        SELECT data_source, COUNT(*) 
        FROM jobs 
        GROUP BY data_source
    """)
    print(f"\n📂 By data source:")
    for row in cursor.fetchall():
        print(f"  {row[0]}: {row[1]} records")
    
    # Scam vs legitimate
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN is_scam THEN 1 ELSE 0 END) as scam_jobs,
            SUM(CASE WHEN NOT is_scam THEN 1 ELSE 0 END) as legit_jobs
        FROM jobs
    """)
    result = cursor.fetchone()
    print(f"\n🎯 Job classification:")
    print(f"  Scam jobs: {result[0]}")
    print(f"  Legitimate jobs: {result[1]}")
    print(f"  Scam ratio: {result[0]/total*100:.2f}%")
    
    # Sample records
    cursor.execute("""
        SELECT job_title, company_name, location, is_scam 
        FROM jobs 
        LIMIT 5
    """)
    print(f"\n📝 Sample records:")
    for row in cursor.fetchall():
        scam_label = "🚨 SCAM" if row[3] else "✅ LEGIT"
        print(f"  {scam_label} - {row[0]} at {row[1]} ({row[2]})")
    
    cursor.close()

def main():
    print("=" * 70)
    print("🚀 SUSJOBS DATA LOADER")
    print("=" * 70)
    
    # 1. Connect to database
    conn = connect_db()
    
    # 2. Load CSV
    csv_path = "C:\\Rethika\\Amrita\\5th sem\\big data analytics\\part1\\data\\fake_job_postings.csv"
    
    df = load_csv_data(csv_path)
    
    # 3. Prepare data
    df = prepare_data(df)
    
    # 4. Confirm before insertion
    print(f"\n⚠️  About to insert {len(df)} records into PostgreSQL")
    confirm = input("Continue? (yes/no): ").strip().lower()
    
    if confirm != 'yes':
        print("❌ Cancelled")
        conn.close()
        exit(0)
    
    # 5. Insert data
    insert_to_postgres(conn, df)
    
    # 6. Verify
    verify_insertion(conn)
    
    # 7. Close connection
    conn.close()
    print("\n✅ Done! Database is ready.")
    print("\n💡 Next steps:")
    print("  1. Verify data: psql -U postgres -d scamjobs")
    print("  2. Run queries: SELECT * FROM jobs LIMIT 10;")
    print("  3. Move to Week 3-4: Kafka streaming setup")

if __name__ == "__main__":
    main()