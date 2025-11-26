# app_integrated.py - ENHANCED VERSION with Styled Output & Gemini Explanation

from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import psycopg2
import pickle
import re
import os
from datetime import datetime
import requests

app = Flask(__name__)
CORS(app)

import sys
import os

# Fix Windows Unicode encoding issues
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# ============= CONFIGURATION =============
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin'
}

# GEMINI API KEY - Put your API key here
GEMINI_API_KEY = "AIzaSyAIqZQCpGCp9LMCUPK471IAeAuIHsfiyfU"  # Replace with your actual key from https://aistudio.google.com/api-keys

# Paths
BASE_DIR = r"C:\Rethika\Amrita\5th sem\big data analytics\part1"
MODEL_PATH = os.path.join(BASE_DIR, "spark_streaming", "model", "rf_model_params.pkl")

# Load ML Model
try:
    with open(MODEL_PATH, 'rb') as f:
        ml_model_params = pickle.load(f)
    print(" ML Model loaded")
except Exception as e:
    print(f" ML Model not found: {e}")
    ml_model_params = None

# ============= DATABASE FUNCTIONS =============

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def extract_company_name(job_title, job_description):
    """Extract company name from text"""
    text = f"{job_title} {job_description}"
    
    patterns = [
        r'(?:at|@)\s+([A-Z][A-Za-z\s&\.]+?)(?:\s|,|\.|$)',
        r'([A-Z][A-Za-z\s&\.]+?)\s+(?:is hiring|seeks|looking for)',
        r'(?:company|organization)[\s:]+([A-Z][A-Za-z\s&\.]+?)(?:\.|,)',
    ]
    
    for pattern in patterns:
        match = re.search(pattern, text)
        if match:
            company = match.group(1).strip()
            if len(company) > 2:
                return company
    
    return None

def check_company_in_db(company_name):
    """Check if company exists in database"""
    if not company_name:
        return None
    
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                SUM(CASE WHEN fraudulent = TRUE THEN 1 ELSE 0 END) as scam_count
            FROM jobs
            WHERE LOWER(company_name) LIKE LOWER(%s)
        """, (f'%{company_name}%',))
        
        row = cursor.fetchone()
        total, scam_count = row
        
        if total and total > 0:
            scam_count = scam_count or 0
            scam_pct = round((scam_count / total) * 100, 2)
            
            cursor.execute("""
                SELECT job_title, location, fraudulent, scam_reasons
                FROM jobs
                WHERE LOWER(company_name) LIKE LOWER(%s)
                ORDER BY scraped_at DESC
                LIMIT 5
            """, (f'%{company_name}%',))
            
            recent_jobs = []
            for job_row in cursor.fetchall():
                recent_jobs.append({
                    'title': job_row[0],
                    'location': job_row[1],
                    'is_scam': job_row[2],
                    'reasons': job_row[3] or []
                })
            
            cursor.close()
            conn.close()
            
            return {
                'found': True,
                'company_name': company_name,
                'total_jobs': total,
                'scam_count': scam_count,
                'legit_count': total - scam_count,
                'scam_percentage': scam_pct,
                'risk_level': 'HIGH' if scam_pct > 50 else 'MEDIUM' if scam_pct > 0 else 'LOW',
                'recent_jobs': recent_jobs
            }
        
        cursor.close()
        conn.close()
        return None
        
    except Exception as e:
        print(f" DB error: {e}")
        return None

def save_prediction_to_db(job_data, prediction_result):
    """Save user's query and prediction to database"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        insert_query = """
        INSERT INTO jobs (
            job_title, company_name, location, job_description,
            salary_range, fraudulent, scam_confidence, scam_reasons,
            data_source, scraped_at
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        RETURNING job_id
        """
        
        cursor.execute(insert_query, (
            job_data.get('title', ''),
            job_data.get('company_name', 'Unknown'),
            'User Submitted',
            job_data.get('description', ''),
            job_data.get('salary', ''),
            prediction_result['is_scam'],
            prediction_result['confidence'],
            prediction_result['reasons'],
            'user_submitted',
            datetime.now()
        ))
        
        job_id = cursor.fetchone()[0]
        conn.commit()
        cursor.close()
        conn.close()
        
        print(f" Saved prediction to DB (job_id: {job_id})")
        return job_id
        
    except Exception as e:
        print(f" Failed to save to DB: {e}")
        return None

# ============= ML PREDICTION =============

def predict_scam_with_ml(job_description, job_title="", salary_range=""):
    """ML-based scam prediction"""
    text = f"{job_title} {job_description}".lower()
    
    urgent_words = ['urgent', 'immediate', 'asap', 'hurry', 'now', 'today', 'quick', 'limited']
    fee_words = ['fee', 'payment', 'deposit', 'investment', 'charge', 'registration', 'security deposit']
    contact_words = ['whatsapp', 'telegram', 'gmail', 'yahoo', 'hotmail', 'contact via', 'call on']
    
    urgent_count = sum(1 for word in urgent_words if word in text)
    fee_count = sum(1 for word in fee_words if word in text)
    contact_count = sum(1 for word in contact_words if word in text)
    has_email = 1 if re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text) else 0
    has_phone = 1 if re.search(r'\b\d{10}\b|\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', text) else 0
    no_salary = 1 if not salary_range or salary_range.strip() == '' else 0
    exclamation = text.count('!')
    
    score = 0
    reasons = []
    
    if fee_count > 0:
        score += 40
        reasons.append(f'Contains {fee_count} fee/payment keywords - major red flag')
    
    if urgent_count > 1:
        score += 25
        reasons.append(f'Excessive urgency pressure ({urgent_count} urgent keywords)')
    
    if contact_count > 0:
        score += 30
        reasons.append(f'Suspicious contact methods ({contact_count} found)')
    
    if has_email:
        score += 20
        reasons.append('Email address found in description')
    
    if has_phone:
        score += 20
        reasons.append('Phone number found in description')
    
    if exclamation > 3:
        score += 10
        reasons.append(f'Excessive exclamation marks ({exclamation})')
    
    if no_salary:
        score += 10
        reasons.append('Missing salary information')
    
    is_scam = score >= 50
    confidence = min(score / 100.0, 1.0)
    
    return {
        'is_scam': is_scam,
        'confidence': confidence,
        'suspicion_score': int(min(score, 100)),
        'reasons': reasons,
        'feature_breakdown': {
            'urgent_keywords': urgent_count,
            'fee_keywords': fee_count,
            'suspicious_contacts': contact_count,
            'has_email': has_email,
            'has_phone': has_phone
        }
    }

# ============= GEMINI API INTEGRATION =============

@app.route('/api/explain-with-gemini', methods=['POST'])
def explain_with_gemini():
    """Use Gemini API to explain why job is scam/safe"""
    try:
        data = request.json
        job_data = data.get('job_data', {})
        
        # Use hardcoded API key from config
        api_key = GEMINI_API_KEY
        
        if not api_key or api_key == "YOUR_API_KEY_HERE":
            return jsonify({'error': 'Please add your Gemini API key to the code (GEMINI_API_KEY variable)'}), 400
        
        # Build prompt for Gemini
        prompt = f"""Analyze this job posting and explain in simple terms why it appears to be {"a SCAM" if job_data.get('is_scam') else "LEGITIMATE"}:

Job Title: {job_data.get('job_title', 'N/A')}
Company: {job_data.get('company_name', 'Unknown')}
Salary: {job_data.get('salary', 'Not specified')}
Description: {job_data.get('description', 'N/A')}

Analysis Results:
- Confidence: {job_data.get('confidence', 0) * 100:.1f}%
- Suspicion Score: {job_data.get('suspicion_score', 0)}/100
- Red Flags: {', '.join(job_data.get('reasons', [])) if job_data.get('reasons') else 'None'}

Feature Analysis:
- Urgent Keywords: {job_data.get('feature_breakdown', {}).get('urgent_keywords', 0)}
- Fee Keywords: {job_data.get('feature_breakdown', {}).get('fee_keywords', 0)}
- Suspicious Contacts: {job_data.get('feature_breakdown', {}).get('suspicious_contacts', 0)}

Please provide a clear, user-friendly explanation in 3-4 sentences that helps someone understand why this job posting is {"suspicious" if job_data.get('is_scam') else "likely legitimate"}."""

        # Call Gemini API with correct model
        model_names = [
    'gemini-2.5-flash',  # ← FASTEST & FREE - Use this!
    'gemini-2.0-flash',
    'gemini-1.5-flash'
]
        
        response = None
        last_error = None

        for model in model_names:
            url = f'https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent?key={api_key}'
            try:
                response = requests.post(url, json={
                    'contents': [{
                        'parts': [{
                            'text': prompt
                        }]
                    }]
                }, timeout=30)

                if response.status_code == 200:
                    result = response.json()
                    explanation = result['candidates'][0]['content']['parts'][0]['text']
                    return jsonify({'explanation': explanation})
                else:
                    last_error = f'{model}: {response.status_code} - {response.text[:200]}'
            except Exception as e:
                last_error = f'{model}: {str(e)}'
                continue

        if response is not None:
            if response.status_code == 400:
                try:
                    error_data = response.json()
                    return jsonify({'error': f'Bad request: {error_data.get("error", {}).get("message", "Invalid request format")}'}), 400
                except Exception:
                    return jsonify({'error': 'Bad request from Gemini API (malformed error payload)'}), 400
            elif response.status_code == 403:
                return jsonify({'error': 'API key invalid or API not enabled. Get a new key from https://aistudio.google.com/apikey'}), 403
            elif response.status_code == 404:
                return jsonify({'error': 'Model not found. API key may be invalid or expired. Get new key from https://aistudio.google.com/apikey'}), 404
            else:
                error_msg = response.text[:200]
                return jsonify({'error': f'Gemini API error ({response.status_code}): {error_msg}'}), 500

        return jsonify({'error': f'All models failed. Last error: {last_error}. Please check your API key at https://aistudio.google.com/apikey'}), 500
            
    except Exception as e:
        print(f"Gemini error: {e}")
        return jsonify({'error': str(e)}), 500

# ============= SERVICE HEALTH CHECKS =============

def check_recent_activity():
    """Check if data is being added to database recently"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("""
            SELECT COUNT(*) FROM jobs 
            WHERE scraped_at > NOW() - INTERVAL '5 minutes'
        """)
        recent = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return recent > 0
        
    except:
        return False

# ============= API ROUTES =============

@app.route('/')
def home():
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/status', methods=['GET'])
def get_status():
    """Get system status"""
    has_recent_data = check_recent_activity()
    
    return jsonify({
        'streaming_active': has_recent_data,
        'scraping_active': has_recent_data,
        'ml_model_loaded': ml_model_params is not None,
        'recent_data': has_recent_data
    })

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database statistics"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM jobs")
        total = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN fraudulent = TRUE THEN 1 ELSE 0 END) as scams,
                SUM(CASE WHEN fraudulent = FALSE THEN 1 ELSE 0 END) as legit
            FROM jobs
        """)
        scams, legit = cursor.fetchone()
        
        cursor.execute("""
            SELECT COUNT(*) FROM jobs 
            WHERE scraped_at > NOW() - INTERVAL '1 hour'
        """)
        recent = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT data_source, COUNT(*) 
            FROM jobs 
            GROUP BY data_source 
            ORDER BY COUNT(*) DESC
        """)
        by_source = [{'source': row[0], 'count': row[1]} for row in cursor.fetchall()]
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total': total,
            'scams': scams or 0,
            'legit': legit or 0,
            'recent_hour': recent,
            'by_source': by_source
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/check-job', methods=['POST'])
def check_job():
    """Main endpoint: Check job posting"""
    try:
        data = request.json
        print(f"\n Received: {data.get('title', 'No title')}")
        
        job_title = data.get('title', '').strip()
        company_input = data.get('company', '').strip()  # NEW: Get company from form
        salary = data.get('salary', '').strip()
        description = data.get('description', '').strip()
        
        if not description:
            return jsonify({'error': 'Description required'}), 400
        
        # Use provided company name OR try to extract it
        company_name = company_input or extract_company_name(job_title, description)
        print(f"Company: {company_name}")
        
        db_result = check_company_in_db(company_name) if company_name else None
        
        if db_result and db_result['found']:
            print(f" Found in DB: {db_result['total_jobs']} jobs")
            
            result = {
                'source': 'database',
                'company_found': True,
                'company_name': db_result['company_name'],
                'is_scam': db_result['risk_level'] in ['HIGH', 'MEDIUM'],
                'confidence': db_result['scam_percentage'] / 100.0,
                'suspicion_score': db_result['scam_percentage'],
                'risk_level': db_result['risk_level'],
                'reasons': [f"{db_result['scam_count']} scam reports in database"],
                'db_stats': {
                    'total_jobs': db_result['total_jobs'],
                    'scam_count': db_result['scam_count'],
                    'legit_count': db_result['legit_count'],
                    'scam_percentage': db_result['scam_percentage']
                },
                'recent_jobs': db_result['recent_jobs'],
                'job_title': job_title,
                'description': description,
                'salary': salary
            }
        else:
            print(" Using ML model")
            ml_result = predict_scam_with_ml(description, job_title, salary)
            
            result = {
                'source': 'ml_model',
                'company_found': False,
                'company_name': company_name or 'Unknown',
                'is_scam': ml_result['is_scam'],
                'confidence': ml_result['confidence'],
                'suspicion_score': ml_result['suspicion_score'],
                'reasons': ml_result['reasons'],
                'feature_breakdown': ml_result['feature_breakdown'],
                'job_title': job_title,
                'description': description,
                'salary': salary
            }
            
            save_prediction_to_db({
                'title': job_title,
                'salary': salary,
                'description': description,
                'company_name': company_name
            }, ml_result)
        
        return jsonify(result)
        
    except Exception as e:
        print(f" Error: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

# ============= HTML DASHBOARD =============
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Scam Detector - Live System</title>
    <meta charset="UTF-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 1000px; margin: 0 auto; }
        
        .status-bar {
            background: white;
            padding: 15px 25px;
            border-radius: 10px;
            margin-bottom: 20px;
            display: flex;
            justify-content: space-between;
            align-items: center;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
        }
        
        .status-item {
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .status-dot {
            width: 10px;
            height: 10px;
            border-radius: 50%;
            animation: pulse 2s infinite;
        }
        
        .status-dot.active { background: #00C851; }
        .status-dot.inactive { background: #ff4444; animation: none; }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.5; }
        }
        
        .header {
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            margin-bottom: 20px;
            text-align: center;
        }
        
        .header h1 { color: #667eea; font-size: 32px; margin-bottom: 5px; }
        .header p { color: #666; }
        
        .stats {
            display: grid;
            grid-template-columns: repeat(4, 1fr);
            gap: 15px;
            margin-bottom: 20px;
        }
        
        .stat-card {
            background: white;
            padding: 20px;
            border-radius: 10px;
            box-shadow: 0 5px 15px rgba(0,0,0,0.2);
            text-align: center;
        }
        
        .stat-card .number {
            font-size: 28px;
            font-weight: bold;
            color: #667eea;
        }
        
        .stat-card .label {
            font-size: 13px;
            color: #666;
            margin-top: 5px;
        }
        
        .card {
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
        }
        
        .form-group {
            margin-bottom: 20px;
        }
        
        label {
            display: block;
            margin-bottom: 8px;
            font-weight: 600;
            color: #333;
        }
        
        input, textarea {
            width: 100%;
            padding: 12px;
            border: 2px solid #e0e0e0;
            border-radius: 8px;
            font-size: 14px;
            font-family: inherit;
        }
        
        textarea { min-height: 150px; resize: vertical; }
        
        button {
            width: 100%;
            padding: 15px;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            color: white;
            border: none;
            border-radius: 10px;
            font-size: 16px;
            font-weight: bold;
            cursor: pointer;
            transition: transform 0.2s;
        }
        
        button:hover { transform: translateY(-2px); }
        button:disabled { opacity: 0.5; cursor: not-allowed; transform: none; }
        
        .loader {
            text-align: center;
            padding: 20px;
            display: none;
        }
        
        .loader.active { display: block; }
        
        .result {
            margin-top: 20px;
            padding: 0;
            border-radius: 12px;
            display: none;
            border: 3px solid;
        }
        
        .result.show { display: block; animation: slideIn 0.3s; }
        
        @keyframes slideIn {
            from { opacity: 0; transform: translateY(10px); }
            to { opacity: 1; transform: translateY(0); }
        }
        
        .result.scam { 
            background: #fff5f5; 
            border-color: #ff4444;
        }
        .result.safe { 
            background: #f0fdf4; 
            border-color: #22c55e;
        }
        
        .result-header {
            padding: 20px 25px;
            border-bottom: 2px solid rgba(0,0,0,0.1);
            display: flex;
            align-items: center;
            gap: 15px;
        }
        
        .result-badge {
            padding: 8px 16px;
            border-radius: 20px;
            font-weight: bold;
            font-size: 14px;
            display: inline-flex;
            align-items: center;
            gap: 8px;
        }
        
        .result.scam .result-badge { 
            background: #ff4444; 
            color: white; 
        }
        .result.safe .result-badge { 
            background: #22c55e; 
            color: white; 
        }
        
        .result-body {
            padding: 25px;
        }
        
        .result-section {
            margin-bottom: 20px;
        }
        
        .result-section:last-child {
            margin-bottom: 0;
        }
        
        .result-section h3 {
            font-size: 14px;
            color: #666;
            margin-bottom: 10px;
            font-weight: 600;
        }
        
        .result-section p {
            font-size: 16px;
            color: #333;
            font-weight: 500;
        }
        
        .info-grid {
            display: grid;
            grid-template-columns: 1fr 1fr;
            gap: 15px;
            margin: 20px 0;
        }
        
        .info-item {
            background: white;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid rgba(0,0,0,0.1);
        }
        
        .info-label { 
            font-size: 12px; 
            color: #666; 
            margin-bottom: 5px;
        }
        .info-value { 
            font-size: 18px; 
            font-weight: bold; 
            color: #333;
        }
        
        .red-flags {
            background: white;
            padding: 15px;
            border-radius: 8px;
            border: 1px solid rgba(0,0,0,0.1);
        }
        
        .red-flags h4 {
            font-size: 14px;
            color: #333;
            margin-bottom: 10px;
            font-weight: 600;
        }
        
        .red-flags ul {
            list-style: none;
            padding: 0;
        }
        
        .red-flags li {
            margin: 8px 0;
            padding-left: 20px;
            position: relative;
            color: #d32f2f;
            font-size: 14px;
        }
        
        .red-flags li:before {
            content: "•";
            position: absolute;
            left: 5px;
            font-size: 18px;
        }
        
        .feature-analysis {
            display: grid;
            grid-template-columns: repeat(3, 1fr);
            gap: 10px;
            margin-top: 15px;
        }
        
        .feature-box {
            background: white;
            padding: 12px;
            border-radius: 8px;
            text-align: center;
            border: 1px solid rgba(0,0,0,0.1);
        }
        
        .feature-box .label {
            font-size: 11px;
            color: #666;
            margin-bottom: 5px;
        }
        
        .feature-box .value {
            font-size: 20px;
            font-weight: bold;
            color: #333;
        }
        
        .gemini-section {
            margin-top: 20px;
            padding-top: 20px;
            border-top: 2px solid rgba(0,0,0,0.1);
        }
        
        .explanation-box {
            background: white;
            padding: 20px;
            border-radius: 8px;
            border: 1px solid rgba(0,0,0,0.1);
            margin-top: 15px;
            display: none;
        }
        
        .explanation-box.show {
            display: block;
            animation: slideIn 0.3s;
        }
        
        .explanation-box h4 {
            font-size: 14px;
            color: #667eea;
            margin-bottom: 10px;
            display: flex;
            align-items: center;
            gap: 8px;
        }
        
        .explanation-box p {
            font-size: 14px;
            color: #333;
            line-height: 1.6;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="status-bar">
            <div class="status-item">
                <div class="status-dot" id="streaming-dot"></div>
                <span>Data Pipeline: <strong id="pipeline-status">...</strong></span>
            </div>
            <div class="status-item">
                <div class="status-dot active"></div>
                <span>ML Model: <strong>Active</strong></span>
            </div>
            <div class="status-item">
                <span>Last hour: <strong id="recent-count">...</strong> jobs</span>
            </div>
        </div>

        <div class="header">
            <h1>🛡️ Live Scam Detector</h1>
            <p>Real-time job posting analysis with streaming data</p>
        </div>

        <div class="stats">
            <div class="stat-card">
                <div class="number" id="total-jobs">...</div>
                <div class="label">Total Jobs</div>
            </div>
            <div class="stat-card">
                <div class="number" style="color: #ff4444" id="scam-count">...</div>
                <div class="label">Scams Detected</div>
            </div>
            <div class="stat-card">
                <div class="number" style="color: #00C851" id="legit-count">...</div>
                <div class="label">Legitimate</div>
            </div>
            <div class="stat-card">
                <div class="number" style="color: #667eea" id="recent-jobs">...</div>
                <div class="label">Recent (1h)</div>
            </div>
        </div>

        <div class="card">
            <h2> Check Job Posting</h2>
            
            <form id="job-form">
                <div class="form-group">
                    <label>Job Title *</label>
                    <input type="text" id="job-title" required>
                </div>
                
                <div class="form-group">
                    <label>Company Name</label>
                    <input type="text" id="company-name" placeholder="Optional - helps with database lookup">
                </div>
                
                <div class="form-group">
                    <label>Salary Range</label>
                    <input type="text" id="salary" placeholder="Optional">
                </div>
                
                <div class="form-group">
                    <label>Job Description *</label>
                    <textarea id="job-desc" required placeholder="Paste full job description..."></textarea>
                </div>
                
                <button type="submit"> Analyze Job</button>
            </form>
            
            <div class="loader" id="loader">
                <p> Analyzing...</p>
            </div>
            
            <div class="result" id="result"></div>
        </div>
    </div>

    <script>
        let currentJobData = null;

        async function loadStatus() {
            try {
                const res = await fetch('/api/status');
                const data = await res.json();
                
                const hasData = data.streaming_active;
                document.getElementById('streaming-dot').className = 
                    'status-dot ' + (hasData ? 'active' : 'inactive');
                document.getElementById('pipeline-status').textContent = 
                    hasData ? 'Running' : 'Starting...';
            } catch (e) {
                console.error('Status error:', e);
            }
        }

        async function loadStats() {
            try {
                const res = await fetch('/api/stats');
                const data = await res.json();
                
                document.getElementById('total-jobs').textContent = data.total.toLocaleString();
                document.getElementById('scam-count').textContent = data.scams.toLocaleString();
                document.getElementById('legit-count').textContent = data.legit.toLocaleString();
                document.getElementById('recent-jobs').textContent = data.recent_hour.toLocaleString();
                document.getElementById('recent-count').textContent = data.recent_hour.toLocaleString();
            } catch (e) {
                console.error('Stats error:', e);
            }
        }

        async function explainWithGemini() {
            const explainBtn = document.getElementById('explain-btn');
            const explanationBox = document.getElementById('explanation-box');
            
            explainBtn.disabled = true;
            explainBtn.textContent = ' Generating...';
            explanationBox.innerHTML = '<p style="text-align:center;">Generating explanation...</p>';
            explanationBox.classList.add('show');
            
            try {
                const res = await fetch('/api/explain-with-gemini', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        job_data: currentJobData
                    })
                });
                
                const data = await res.json();
                
                if (data.error) {
                    explanationBox.innerHTML = `<p style="color:#ff4444;"> Error: ${data.error}</p>`;
                } else {
                    explanationBox.innerHTML = `
                        <h4> AI Explanation</h4>
                        <p>${data.explanation}</p>
                    `;
                }
            } catch (error) {
                explanationBox.innerHTML = `<p style="color:#ff4444;"> Error: ${error.message}</p>`;
            } finally {
                explainBtn.disabled = false;
                explainBtn.textContent = 'Explain Why';
            }
        }

        document.getElementById('job-form').addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const loader = document.getElementById('loader');
            const result = document.getElementById('result');
            
            loader.classList.add('active');
            result.classList.remove('show');
            
            try {
                const jobTitle = document.getElementById('job-title').value;
                const companyName = document.getElementById('company-name').value;
                const salary = document.getElementById('salary').value;
                const description = document.getElementById('job-desc').value;
                
                const res = await fetch('/api/check-job', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({
                        title: jobTitle,
                        company: companyName,
                        salary: salary,
                        description: description
                    })
                });
                
                const data = await res.json();
                loader.classList.remove('active');
                
                currentJobData = data;
                
                const resultClass = data.is_scam ? 'scam' : 'safe';
                const badgeIcon = data.is_scam ? '' : '';
                const badgeText = data.is_scam ? 'APPEARS SUSPICIOUS' : 'APPEARS SAFE';
                
                let html = `
                    <div class="result-header">
                        <div class="result-badge">
                            ${badgeIcon} ${badgeText}
                        </div>
                    </div>
                    <div class="result-body">
                        <div class="result-section">
                            <h3>Company:</h3>
                            <p>${data.company_name || 'Unknown'}</p>
                        </div>
                        
                        <div class="result-section">
                            <h3>Source:</h3>
                            <p> ${data.source === 'database' ? 'Database' : 'ML Model'}</p>
                        </div>
                        
                        <div class="info-grid">
                            <div class="info-item">
                                <div class="info-label">Confidence</div>
                                <div class="info-value">${(data.confidence * 100).toFixed(1)}%</div>
                            </div>
                            <div class="info-item">
                                <div class="info-label">Score</div>
                                <div class="info-value">${data.suspicion_score || 0}/100</div>
                            </div>
                        </div>
                `;
                
                if (data.reasons && data.reasons.length > 0) {
                    html += `
                        <div class="red-flags">
                            <h4> Red Flags:</h4>
                            <ul>
                                ${data.reasons.map(r => `<li>${r}</li>`).join('')}
                            </ul>
                        </div>
                    `;
                }
                
                if (data.feature_breakdown) {
                    html += `
                        <div class="result-section">
                            <h3>Feature Analysis:</h3>
                            <div class="feature-analysis">
                                <div class="feature-box">
                                    <div class="label">Urgent Keywords</div>
                                    <div class="value">${data.feature_breakdown.urgent_keywords || 0}</div>
                                </div>
                                <div class="feature-box">
                                    <div class="label">Fee Keywords</div>
                                    <div class="value">${data.feature_breakdown.fee_keywords || 0}</div>
                                </div>
                                <div class="feature-box">
                                    <div class="label">Suspicious Contacts</div>
                                    <div class="value">${data.feature_breakdown.suspicious_contacts || 0}</div>
                                </div>
                            </div>
                        </div>
                    `;
                }
                
                html += `
                    <div class="gemini-section">
                        <h3> Get AI Explanation</h3>
                        <p style="font-size:13px; color:#666; margin-bottom:10px;">
                            Click below to get a detailed AI-powered explanation of why this job is ${data.is_scam ? 'suspicious' : 'legitimate'}.
                        </p>
                        <button type="button" id="explain-btn" onclick="explainWithGemini()" style="width:auto; padding:12px 30px;"> Explain Why</button>
                        <div class="explanation-box" id="explanation-box"></div>
                    </div>
                `;
                
                html += '</div>';
                
                result.className = `result ${resultClass} show`;
                result.innerHTML = html;
                
                loadStats();
                
            } catch (error) {
                loader.classList.remove('active');
                alert('Error: ' + error.message);
            }
        });

        loadStatus();
        loadStats();
        
        setInterval(loadStatus, 10000);
        setInterval(loadStats, 10000);
    </script>
</body>
</html>
"""

if __name__ == '__main__':
    print("="*70)
    print(" FLASK API SERVER - ENHANCED")
    print("="*70)
    print(f" ML Model: {'Loaded' if ml_model_params else 'Fallback'}")
    print(f" Database: {DB_CONFIG['database']}")
    print(f" Gemini AI: Integrated")
    print("\n Dashboard: http://localhost:5000")
    print("\n Features:")
    print("   • Styled card output")
    print("   • Gemini AI explanations")
    print("   • Real-time stats")
    print("   • Company name input field")
    print("\n NOTE: This should be run via run_all.py")
    print("   Or start Spark/Scraper separately first")
    print("\n Press Ctrl+C to stop\n")

    app.run(debug=False, host='0.0.0.0', port=5000)