import psycopg2
import pandas as pd

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin'  # UPDATE THIS
}

def run_analysis():
    """Run detailed analysis on loaded data"""
    conn = psycopg2.connect(**DB_CONFIG)
    
    queries = {
        "Total Records": "SELECT COUNT(*) FROM jobs",
        
        "Records by Source": """
            SELECT data_source, COUNT(*) as count 
            FROM jobs 
            GROUP BY data_source 
            ORDER BY count DESC
        """,
        
        "Scam Statistics": """
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN is_scam THEN 1 ELSE 0 END) as scams,
                SUM(CASE WHEN NOT is_scam THEN 1 ELSE 0 END) as legit,
                ROUND(100.0 * SUM(CASE WHEN is_scam THEN 1 ELSE 0 END) / COUNT(*), 2) as scam_percentage
            FROM jobs
        """,
        
        "Top 10 Companies": """
            SELECT company_name, COUNT(*) as job_count,
                   SUM(CASE WHEN is_scam THEN 1 ELSE 0 END) as scam_count
            FROM jobs
            WHERE company_name != ''
            GROUP BY company_name
            ORDER BY job_count DESC
            LIMIT 10
        """,
        
        "Jobs by Employment Type": """
            SELECT employment_type, COUNT(*) as count
            FROM jobs
            WHERE employment_type != ''
            GROUP BY employment_type
            ORDER BY count DESC
        """,
        
        "Jobs with Salary Info": """
            SELECT 
                SUM(CASE WHEN has_salary THEN 1 ELSE 0 END) as with_salary,
                SUM(CASE WHEN NOT has_salary THEN 1 ELSE 0 END) as without_salary
            FROM jobs
        """
    }
    
    print("=" * 70)
    print("📊 DATABASE ANALYSIS")
    print("=" * 70)
    
    for title, query in queries.items():
        print(f"\n{title}:")
        print("-" * 50)
        df = pd.read_sql_query(query, conn)
        print(df.to_string(index=False))
    
    conn.close()

if __name__ == "__main__":
    run_analysis()