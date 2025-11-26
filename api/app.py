# api/app.py - Flask REST API for Scam Job Detection

from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import psycopg2
from datetime import datetime, timedelta
import json

app = Flask(__name__)
CORS(app)

DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin'
}

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

@app.route('/')
def home():
    """Home page with simple dashboard"""
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get overall statistics"""
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Total jobs
    cursor.execute("SELECT COUNT(*) FROM jobs")
    total_jobs = cursor.fetchone()[0]
    
    # Scam vs legit
    cursor.execute("""
        SELECT 
            SUM(CASE WHEN fraudulent = TRUE THEN 1 ELSE 0 END) as scam_count,
            SUM(CASE WHEN fraudulent = FALSE THEN 1 ELSE 0 END) as legit_count
        FROM jobs
    """)
    scam_count, legit_count = cursor.fetchone()
    
    # By data source
    cursor.execute("""
        SELECT data_source, COUNT(*) as count
        FROM jobs
        GROUP BY data_source
        ORDER BY count DESC
    """)
    by_source = [{'source': row[0], 'count': row[1]} for row in cursor.fetchall()]
    
    # Recent activity (last 24 hours)
    cursor.execute("""
        SELECT COUNT(*) 
        FROM jobs 
        WHERE scraped_at > NOW() - INTERVAL '24 hours'
    """)
    recent_count = cursor.fetchone()[0]
    
    cursor.close()
    conn.close()
    
    return jsonify({
        'total_jobs': total_jobs,
        'scam_count': scam_count or 0,
        'legit_count': legit_count or 0,
        'scam_percentage': round((scam_count or 0) / max(total_jobs, 1) * 100, 2),
        'by_source': by_source,
        'recent_24h': recent_count
    })

@app.route('/api/search', methods=['POST'])
def search_jobs():
    """Search for jobs by company name or keywords"""
    data = request.json
    query = data.get('query', '')
    limit = data.get('limit', 20)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    search_query = """
        SELECT 
            job_id, job_title, company_name, location, 
            fraudulent, scam_confidence, scam_reasons,
            scraped_at, data_source
        FROM jobs
        WHERE 
            LOWER(company_name) LIKE %s 
            OR LOWER(job_title) LIKE %s
            OR LOWER(job_description) LIKE %s
        ORDER BY scraped_at DESC
        LIMIT %s
    """
    
    search_term = f'%{query.lower()}%'
    cursor.execute(search_query, (search_term, search_term, search_term, limit))
    
    results = []
    for row in cursor.fetchall():
        results.append({
            'job_id': row[0],
            'title': row[1],
            'company': row[2],
            'location': row[3],
            'is_scam': row[4],
            'confidence': float(row[5]) if row[5] else 0,
            'reasons': row[6] or [],
            'scraped_at': row[7].isoformat() if row[7] else None,
            'source': row[8]
        })
    
    cursor.close()
    conn.close()
    
    return jsonify({'results': results, 'count': len(results)})

@app.route('/api/recent-scams', methods=['GET'])
def get_recent_scams():
    """Get recent scam detections"""
    limit = request.args.get('limit', 10, type=int)
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            job_title, company_name, location, 
            scam_confidence, scam_reasons, scraped_at
        FROM jobs
        WHERE fraudulent = TRUE
        ORDER BY scraped_at DESC
        LIMIT %s
    """, (limit,))
    
    scams = []
    for row in cursor.fetchall():
        scams.append({
            'title': row[0],
            'company': row[1],
            'location': row[2],
            'confidence': float(row[3]) if row[3] else 0,
            'reasons': row[4] or [],
            'detected_at': row[5].isoformat() if row[5] else None
        })
    
    cursor.close()
    conn.close()
    
    return jsonify({'scams': scams})

@app.route('/api/check-company', methods=['POST'])
def check_company():
    """Check if a company has scam reports"""
    data = request.json
    company_name = data.get('company_name', '')
    
    conn = get_db_connection()
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT 
            COUNT(*) as total_jobs,
            SUM(CASE WHEN fraudulent = TRUE THEN 1 ELSE 0 END) as scam_count,
            AVG(scam_confidence) as avg_confidence
        FROM jobs
        WHERE LOWER(company_name) = LOWER(%s)
    """, (company_name,))
    
    result = cursor.fetchone()
    total_jobs, scam_count, avg_confidence = result
    
    cursor.close()
    conn.close()
    
    if total_jobs == 0:
        return jsonify({
            'found': False,
            'message': 'No jobs found for this company'
        })
    
    return jsonify({
        'found': True,
        'company': company_name,
        'total_jobs': total_jobs,
        'scam_count': scam_count or 0,
        'legit_count': total_jobs - (scam_count or 0),
        'avg_confidence': float(avg_confidence) if avg_confidence else 0,
        'risk_level': 'HIGH' if (scam_count or 0) > total_jobs * 0.5 else 'MEDIUM' if (scam_count or 0) > 0 else 'LOW'
    })

# Simple HTML Dashboard
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Internship Scam Detector Dashboard</title>
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 20px;
        }
        .container { max-width: 1200px; margin: 0 auto; }
        .header {
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            margin-bottom: 20px;
            text-align: center;
        }
        .header h1 { color: #667eea; margin-bottom: 10px; }
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 20px;
        }
        .stat-card {
            background: white;
            padding: 25px;
            border-radius: 15px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            text-align: center;
        }
        .stat-card h3 { color: #666; font-size: 14px; margin-bottom: 10px; }
        .stat-card .number { font-size: 36px; font-weight: bold; color: #667eea; }
        .search-section {
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            margin-bottom: 20px;
        }
        .search-box {
            display: flex;
            gap: 10px;
            margin-bottom: 20px;
        }
        .search-box input {
            flex: 1;
            padding: 15px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 16px;
        }
        .search-box button {
            padding: 15px 30px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 8px;
            cursor: pointer;
            font-size: 16px;
            font-weight: bold;
        }
        .search-box button:hover { background: #5568d3; }
        .results { margin-top: 20px; }
        .job-card {
            background: #f9f9f9;
            padding: 20px;
            border-radius: 10px;
            margin-bottom: 15px;
            border-left: 5px solid #ddd;
        }
        .job-card.scam { border-left-color: #ff4444; background: #fff5f5; }
        .job-card.safe { border-left-color: #00C851; background: #f0fff4; }
        .job-card h4 { margin-bottom: 10px; color: #333; }
        .job-card .meta { color: #666; font-size: 14px; margin-bottom: 10px; }
        .badge {
            display: inline-block;
            padding: 5px 15px;
            border-radius: 20px;
            font-size: 12px;
            font-weight: bold;
            margin-right: 10px;
        }
        .badge.scam { background: #ff4444; color: white; }
        .badge.safe { background: #00C851; color: white; }
        .reasons { 
            margin-top: 10px;
            padding: 10px;
            background: white;
            border-radius: 5px;
            font-size: 13px;
        }
        .reasons li { margin: 5px 0; color: #d32f2f; }
        .loader { 
            text-align: center; 
            padding: 20px;
            display: none;
        }
        .loader.active { display: block; }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1>🚨 Internship Scam Detector</h1>
            <p>Big Data Pipeline for Real-time Job Scam Detection</p>
        </div>

        <div class="stats-grid" id="stats">
            <div class="stat-card">
                <h3>Total Jobs Analyzed</h3>
                <div class="number" id="total-jobs">0</div>
            </div>
            <div class="stat-card">
                <h3>Scams Detected</h3>
                <div class="number" style="color: #ff4444" id="scam-count">0</div>
            </div>
            <div class="stat-card">
                <h3>Legitimate Jobs</h3>
                <div class="number" style="color: #00C851" id="legit-count">0</div>
            </div>
            <div class="stat-card">
                <h3>Scam Rate</h3>
                <div class="number" id="scam-rate">0%</div>
            </div>
        </div>

        <div class="search-section">
            <h2>Search Jobs</h2>
            <div class="search-box">
                <input type="text" id="search-input" placeholder="Enter company name or job title...">
                <button onclick="searchJobs()">🔍 Search</button>
            </div>
            
            <div class="loader" id="loader">Loading...</div>
            
            <div class="results" id="results"></div>
        </div>
    </div>

    <script>
        // Load stats on page load
        async function loadStats() {
            const response = await fetch('/api/stats');
            const data = await response.json();
            
            document.getElementById('total-jobs').textContent = data.total_jobs.toLocaleString();
            document.getElementById('scam-count').textContent = data.scam_count.toLocaleString();
            document.getElementById('legit-count').textContent = data.legit_count.toLocaleString();
            document.getElementById('scam-rate').textContent = data.scam_percentage + '%';
        }

        async function searchJobs() {
            const query = document.getElementById('search-input').value;
            if (!query) return;

            const loader = document.getElementById('loader');
            const results = document.getElementById('results');
            
            loader.classList.add('active');
            results.innerHTML = '';

            const response = await fetch('/api/search', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify({ query, limit: 20 })
            });

            const data = await response.json();
            loader.classList.remove('active');

            if (data.count === 0) {
                results.innerHTML = '<p>No jobs found matching your search.</p>';
                return;
            }

            data.results.forEach(job => {
                const card = document.createElement('div');
                card.className = `job-card ${job.is_scam ? 'scam' : 'safe'}`;
                
                let reasonsHtml = '';
                if (job.is_scam && job.reasons.length > 0) {
                    reasonsHtml = '<div class="reasons"><strong>Red Flags:</strong><ul>' +
                        job.reasons.map(r => `<li>${r}</li>`).join('') +
                        '</ul></div>';
                }
                
                card.innerHTML = `
                    <h4>${job.title}</h4>
                    <div class="meta">
                        <span class="badge ${job.is_scam ? 'scam' : 'safe'}">
                            ${job.is_scam ? '🚨 SCAM' : '✅ SAFE'}
                        </span>
                        <strong>${job.company}</strong> | ${job.location} | 
                        Confidence: ${(job.confidence * 100).toFixed(1)}%
                    </div>
                    ${reasonsHtml}
                `;
                
                results.appendChild(card);
            });
        }

        // Load stats on page load
        loadStats();
        
        // Refresh stats every 30 seconds
        setInterval(loadStats, 30000);

        // Enable Enter key for search
        document.getElementById('search-input').addEventListener('keypress', (e) => {
            if (e.key === 'Enter') searchJobs();
        });
    </script>
</body>
</html>
"""

if __name__ == '__main__':
    print("="*70)
    print("🌐 STARTING FLASK API SERVER")
    print("="*70)
    print("\n📊 Dashboard: http://localhost:5000")
    print("📡 API Endpoints:")
    print("   GET  /api/stats")
    print("   POST /api/search")
    print("   POST /api/check-company")
    print("   GET  /api/recent-scams")
    print("\nPress Ctrl+C to stop\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)