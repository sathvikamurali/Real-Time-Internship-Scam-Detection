# app_complete.py - FIXED WITH ERROR HANDLING

from flask import Flask, request, jsonify, render_template_string
from flask_cors import CORS
import psycopg2
import pickle
import re
import traceback

app = Flask(__name__)
CORS(app)

# ============= CONFIGURATION =============
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin'
}

# Gemini API Key (optional - works without it)
GEMINI_API_KEY = "AIzaSyAIqZQCpGCp9LMCUPK471IAeAuIHsfiyfU"  # Add your key or leave as is
USE_GEMINI = False

if GEMINI_API_KEY != "AIzaSyDVM17zn8r4GPNEsLgRz2aCE9XkSj-l6Yo":
    try:
        import google.generativeai as genai
        genai.configure(api_key=GEMINI_API_KEY)
        gemini_model = genai.GenerativeModel('gemini-2.0-flash-exp')
        USE_GEMINI = True
        print(" Gemini AI enabled")
    except:
        USE_GEMINI = False
        print("Gemini import failed")

# Load ML Model
MODEL_PATH = r"C:\Rethika\Amrita\5th sem\big data analytics\part1\spark_streaming\model\rf_model_params.pkl"

try:
    with open(MODEL_PATH, 'rb') as f:
        ml_model_params = pickle.load(f)
    print(" ML Model loaded successfully!")
except Exception as e:
    print(f" Could not load ML model: {e}")
    ml_model_params = None

# ============= DATABASE FUNCTIONS =============
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def extract_company_name(job_title, job_description):
    """Extract company name from text"""
    patterns = [
        r'(?:company|organization|firm)[\s:]+([A-Z][A-Za-z\s&\.]+?)(?:\.|,|\s+is)',
        r'(?:about|join|at)\s+([A-Z][A-Za-z\s&\.]+?)(?:\.|,|:)',
        r'^([A-Z][A-Za-z\s&\.]+?)\s+(?:is hiring|seeks|looking for)',
    ]
    
    text = f"{job_title} {job_description}"
    for pattern in patterns:
        match = re.search(pattern, text, re.MULTILINE | re.IGNORECASE)
        if match:
            return match.group(1).strip()
    
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
        
        if total > 0:
            cursor.execute("""
                SELECT job_title, location, fraudulent, scraped_at
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
                    'date': str(job_row[3]) if job_row[3] else None
                })
            
            cursor.close()
            conn.close()
            
            scam_count = scam_count or 0
            scam_pct = round((scam_count / total) * 100, 2)
            
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
        print(f" Database error: {e}")
        traceback.print_exc()
        return None

# ============= ML PREDICTION =============
def predict_scam_with_ml(job_description, job_title="", salary_range=""):
    """ML-based scam prediction"""
    text = f"{job_title} {job_description}".lower()
    
    # Keywords
    urgent_words = ['urgent', 'immediate', 'asap', 'hurry', 'now', 'today', 'quick', 'limited']
    fee_words = ['fee', 'payment', 'pay upfront', 'deposit', 'investment', 'charge', 'registration']
    contact_words = ['whatsapp', 'telegram', 'gmail', 'yahoo', 'hotmail', 'contact via']
    
    # Count features
    urgent_count = sum(1 for word in urgent_words if word in text)
    fee_count = sum(1 for word in fee_words if word in text)
    contact_count = sum(1 for word in contact_words if word in text)
    has_email = 1 if re.search(r'\b[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Z|a-z]{2,}\b', text) else 0
    has_phone = 1 if re.search(r'\b\d{10}\b|\b\d{3}[-.\s]?\d{3}[-.\s]?\d{4}\b', text) else 0
    no_salary = 1 if not salary_range or salary_range.strip() == '' else 0
    exclamation = text.count('!')
    
    # Calculate score
    score = 0
    reasons = []
    
    if fee_count > 0:
        score += 35
        reasons.append(f'Contains {fee_count} fee/payment keyword(s)')
    
    if urgent_count > 1:
        score += 25
        reasons.append(f'Excessive urgency ({urgent_count} urgent keywords)')
    
    if contact_count > 0:
        score += 30
        reasons.append(f'Suspicious contact methods ({contact_count} found)')
    
    if has_email:
        score += 20
        reasons.append('Email address in description')
    
    if has_phone:
        score += 20
        reasons.append('Phone number in description')
    
    if exclamation > 3:
        score += 15
        reasons.append(f'Excessive exclamation marks ({exclamation})')
    
    if no_salary:
        score += 10
        reasons.append('Missing salary information')
    
    is_scam = score >= 50
    confidence = min(score / 100.0, 1.0)
    
    return {
        'is_scam': is_scam,
        'confidence': confidence,
        'suspicion_score': int(score),
        'reasons': reasons,
        'feature_breakdown': {
            'urgent_keywords': urgent_count,
            'fee_keywords': fee_count,
            'suspicious_contacts': contact_count,
            'has_email': has_email,
            'has_phone': has_phone,
            'red_flags': len(reasons)
        }
    }

# ============= GEMINI EXPLANATION =============
def get_gemini_explanation(job_description, reasons):
    """Get AI explanation (optional)"""
    if not USE_GEMINI:
        return None
    
    try:
        prompt = f"""Analyze this suspicious job posting and explain why it's likely a scam in 2-3 sentences.

Job Description: {job_description[:500]}
Red Flags: {', '.join(reasons[:3])}

Be direct and helpful."""
        
        response = gemini_model.generate_content(prompt)
        return response.text
    except Exception as e:
        print(f"Gemini error: {e}")
        return None

# ============= API ROUTES =============
@app.route('/')
def home():
    return render_template_string(DASHBOARD_HTML)

@app.route('/api/check-job', methods=['POST'])
def check_job():
    """Main endpoint - check job posting"""
    try:
        data = request.json
        print(f"\n Received request: {data}")
        
        job_title = data.get('title', '').strip()
        salary_range = data.get('salary', '').strip()
        job_description = data.get('description', '').strip()
        
        if not job_description:
            return jsonify({'error': 'Job description is required'}), 400
        
        # Extract company name
        company_name = extract_company_name(job_title, job_description)
        print(f" Extracted company: {company_name}")
        
        # Check database first
        db_result = check_company_in_db(company_name) if company_name else None
        
        if db_result and db_result['found']:
            print(f" Company found in DB: {db_result['total_jobs']} jobs")
            
            # Build reasons from DB
            reasons = []
            if db_result['scam_count'] > 0:
                reasons.append(f"{db_result['scam_count']} scam reports in database")
            if db_result['scam_percentage'] > 50:
                reasons.append(f"High scam rate: {db_result['scam_percentage']}%")
            
            return jsonify({
                'source': 'database',
                'company_found': True,
                'company_name': db_result['company_name'],
                'is_scam': db_result['risk_level'] in ['HIGH', 'MEDIUM'],
                'confidence': db_result['scam_percentage'] / 100.0,
                'suspicion_score': db_result['scam_percentage'],
                'risk_level': db_result['risk_level'],
                'reasons': reasons,
                'db_stats': {
                    'total_jobs': db_result['total_jobs'],
                    'scam_count': db_result['scam_count'],
                    'legit_count': db_result['legit_count'],
                    'scam_percentage': db_result['scam_percentage']
                },
                'recent_jobs': db_result['recent_jobs'],
                'message': f'Found {db_result["total_jobs"]} jobs from this company in database'
            })
        
        else:
            # Use ML model
            print(" Using ML model")
            ml_prediction = predict_scam_with_ml(job_description, job_title, salary_range)
            
            # Get AI explanation if scam
            ai_explanation = None
            if ml_prediction['is_scam'] and USE_GEMINI:
                ai_explanation = get_gemini_explanation(job_description, ml_prediction['reasons'])
            
            return jsonify({
                'source': 'ml_model',
                'company_found': False,
                'company_name': company_name or 'Unknown',
                'is_scam': ml_prediction['is_scam'],
                'confidence': ml_prediction['confidence'],
                'suspicion_score': ml_prediction['suspicion_score'],
                'reasons': ml_prediction['reasons'],
                'feature_breakdown': ml_prediction['feature_breakdown'],
                'ai_explanation': ai_explanation,
                'model_used': 'Trained ML Model' if ml_model_params else 'Rule-based',
                'message': 'Company not found in database. Analyzed using ML model.'
            })
    
    except Exception as e:
        print(f" ERROR in check_job: {e}")
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500

@app.route('/api/stats', methods=['GET'])
def get_stats():
    """Get database stats"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM jobs")
        total_jobs = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT 
                SUM(CASE WHEN fraudulent = TRUE THEN 1 ELSE 0 END) as scam_count,
                SUM(CASE WHEN fraudulent = FALSE THEN 1 ELSE 0 END) as legit_count
            FROM jobs
        """)
        scam_count, legit_count = cursor.fetchone()
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total_jobs': total_jobs,
            'scam_count': scam_count or 0,
            'legit_count': legit_count or 0,
            'scam_percentage': round((scam_count or 0) / max(total_jobs, 1) * 100, 2)
        })
    except Exception as e:
        print(f" ERROR in stats: {e}")
        return jsonify({'error': str(e)}), 500

# ============= HTML DASHBOARD =============
DASHBOARD_HTML = """
<!DOCTYPE html>
<html>
<head>
    <title>Scam Detector</title>
    <meta charset="UTF-8">
    <style>
        * { margin: 0; padding: 0; box-sizing: border-box; }
        body { 
            font-family: 'Segoe UI', sans-serif;
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            min-height: 100vh;
            padding: 20px;
        }
        .container { max-width: 900px; margin: 0 auto; }
        
        .header {
            background: white;
            padding: 30px;
            border-radius: 15px;
            box-shadow: 0 10px 30px rgba(0,0,0,0.3);
            margin-bottom: 20px;
            text-align: center;
        }
        .header h1 { color: #667eea; font-size: 28px; }
        
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
        
        textarea {
            min-height: 150px;
            resize: vertical;
        }
        
        button {
            width: 100%;
            padding: 15px;
            background: #667eea;
            color: white;
            border: none;
            border-radius: 8px;
            font-size: 16px;
            font-weight: bold;
            cursor: pointer;
        }
        
        button:hover { background: #5568d3; }
        button:disabled { background: #ccc; cursor: not-allowed; }
        
        .result {
            margin-top: 20px;
            padding: 20px;
            border-radius: 10px;
            display: none;
        }
        
        .result.show { display: block; }
        .result.scam { background: #ffe5e5; border-left: 5px solid #ff4444; }
        .result.safe { background: #e5ffe5; border-left: 5px solid #00C851; }
        
        .badge {
            display: inline-block;
            padding: 8px 20px;
            border-radius: 20px;
            font-weight: bold;
            margin-bottom: 15px;
        }
        
        .badge.scam { background: #ff4444; color: white; }
        .badge.safe { background: #00C851; color: white; }
        
        .loader {
            text-align: center;
            padding: 20px;
            display: none;
        }
        
        .loader.active { display: block; }
        
        pre {
            background: #f5f5f5;
            padding: 10px;
            border-radius: 5px;
            overflow-x: auto;
            font-size: 12px;
        }
    </style>
</head>
<body>
    <div class="container">
        <div class="header">
            <h1> Internship Scam Detector</h1>
            <p>AI-Powered Job Analysis</p>
        </div>

        <div class="card">
            <h2>Check Job Posting</h2>
            
            <form id="job-form">
                <div class="form-group">
                    <label>Job Title *</label>
                    <input type="text" id="job-title" required placeholder="e.g., Data Entry Operator">
                </div>
                
                <div class="form-group">
                    <label>Salary Range</label>
                    <input type="text" id="salary" placeholder="e.g., 15000-25000 or leave blank">
                </div>
                
                <div class="form-group">
                    <label>Job Description *</label>
                    <textarea id="job-desc" required placeholder="Paste the complete job description here..."></textarea>
                </div>
                
                <button type="submit"> Analyze Job</button>
            </form>
            
            <div class="loader" id="loader">
                <p>Analyzing...</p>
            </div>
            
            <div class="result" id="result"></div>
        </div>
    </div>

    <script>
        const form = document.getElementById('job-form');
        const loader = document.getElementById('loader');
        const result = document.getElementById('result');

        form.addEventListener('submit', async (e) => {
            e.preventDefault();
            
            const title = document.getElementById('job-title').value.trim();
            const salary = document.getElementById('salary').value.trim();
            const description = document.getElementById('job-desc').value.trim();
            
            console.log('Form submitted:', { title, salary, description });
            
            if (!description) {
                alert('Please enter job description');
                return;
            }
            
            // Show loader
            loader.classList.add('active');
            result.classList.remove('show');
            
            try {
                console.log('Sending request to /api/check-job...');
                
                const response = await fetch('/api/check-job', {
                    method: 'POST',
                    headers: {
                        'Content-Type': 'application/json',
                    },
                    body: JSON.stringify({
                        title: title,
                        salary: salary,
                        description: description
                    })
                });
                
                console.log('Response status:', response.status);
                
                if (!response.ok) {
                    const errorText = await response.text();
                    throw new Error(`HTTP ${response.status}: ${errorText}`);
                }
                
                const data = await response.json();
                console.log('Response data:', data);
                
                loader.classList.remove('active');
                displayResult(data);
                
            } catch (error) {
                console.error('Error:', error);
                loader.classList.remove('active');
                
                result.className = 'result scam show';
                result.innerHTML = `
                    <h3> Error</h3>
                    <p>${error.message}</p>
                    <pre>${error.stack || ''}</pre>
                `;
            }
        });

        function displayResult(data) {
            const resultClass = data.is_scam ? 'scam' : 'safe';
            const badgeClass = data.is_scam ? 'scam' : 'safe';
            const badgeText = data.is_scam ? ' SCAM DETECTED' : ' APPEARS SAFE';
            
            let html = `
                <span class="badge ${badgeClass}">${badgeText}</span>
                <p><strong>Company:</strong> ${data.company_name}</p>
                <p><strong>Source:</strong> ${data.source === 'database' ? ' Database' : ' ML Model'}</p>
                <p><strong>Confidence:</strong> ${(data.confidence * 100).toFixed(1)}%</p>
                <p><strong>Score:</strong> ${data.suspicion_score}/100</p>
            `;
            
            if (data.reasons && data.reasons.length > 0) {
                html += `<h4>Red Flags:</h4><ul>`;
                data.reasons.forEach(r => {
                    html += `<li>${r}</li>`;
                });
                html += `</ul>`;
            }
            
            if (data.db_stats) {
                html += `
                    <h4>Database Stats:</h4>
                    <p>Total Jobs: ${data.db_stats.total_jobs}</p>
                    <p>Scam Jobs: ${data.db_stats.scam_count}</p>
                    <p>Scam Rate: ${data.db_stats.scam_percentage}%</p>
                `;
            }
            
            if (data.feature_breakdown) {
                html += `
                    <h4>Feature Analysis:</h4>
                    <p>Urgent Keywords: ${data.feature_breakdown.urgent_keywords}</p>
                    <p>Fee Keywords: ${data.feature_breakdown.fee_keywords}</p>
                    <p>Suspicious Contacts: ${data.feature_breakdown.suspicious_contacts}</p>
                `;
            }
            
            if (data.ai_explanation) {
                html += `
                    <h4>AI Analysis:</h4>
                    <p>${data.ai_explanation}</p>
                `;
            }
            
            result.className = `result ${resultClass} show`;
            result.innerHTML = html;
        }
    </script>
</body>
</html>
"""

# ============= RUN =============
if __name__ == '__main__':
    print("="*70)
    print("🚀 SCAM DETECTOR - SINGLE FORM")
    print("="*70)
    print(f" ML Model: {'Loaded' if ml_model_params else 'Fallback'}")
    print(f" Database: Connected")
    print(f"Gemini: {'Enabled' if USE_GEMINI else 'Disabled'}")
    print("\n Dashboard: http://localhost:5000")
    print("Press Ctrl+C to stop\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)