# analytics_dashboard.py - PROFESSIONAL ANALYTICS DASHBOARD (REAL DATA ONLY)
"""
Run this SEPARATELY from run_all.py

Terminal 1: python run_all.py (for job detection)
Terminal 2: python analytics_dashboard.py (for analytics dashboard)

Dashboard will be available at: http://localhost:5001

NOTE: This dashboard now EXCLUDES Kaggle static data and only shows:
- indeed_scrapped
- naukri_scrapped
- linkedin_scrapped
- user_submitted
"""

import sys
import os
from flask import Flask, jsonify
from flask_cors import CORS
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import datetime, timedelta
import json
import re

# Fix Windows Unicode encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# Flask app configuration
app = Flask(__name__)
app.config['SECRET_KEY'] = 'analytics_secret_key_2024'
CORS(app)

# Database configuration
DB_CONFIG = {
    'host': 'localhost',
    'port': 5432,
    'database': 'scamjobs',
    'user': 'postgres',
    'password': 'radmin'
}

# REAL DATA SOURCES (excluding Kaggle)
REAL_DATA_SOURCES = ['indeed_scraped', 'naukri_scraped', 'linkedin_scraped', 'user_submitted']

# ALL DATA SOURCES (including Kaggle) - for KPIs and Detection Status only
ALL_DATA_SOURCES = ['indeed_scraped', 'naukri_scraped', 'linkedin_scraped', 'user_submitted', 'susjobs_kaggle']

def get_db_connection():
    """Get PostgreSQL database connection"""
    return psycopg2.connect(**DB_CONFIG)

def get_data_filter(include_kaggle=False):
    """Get SQL WHERE clause to filter data sources"""
    sources = ALL_DATA_SOURCES if include_kaggle else REAL_DATA_SOURCES
    sources_list = "', '".join(sources)
    return f"data_source IN ('{sources_list}')"

# Professional HTML Template
HTML_TEMPLATE = '''<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Scam Internship Detection - Analytics Dashboard</title>
    <link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.3/dist/css/bootstrap.min.css" rel="stylesheet">
    <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/font-awesome/6.0.0/css/all.min.css">
    <script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.0/dist/chart.umd.min.js"></script>
    <style>
        :root {
            --primary-color: #2c3e50;
            --secondary-color: #3498db;
            --danger-color: #e74c3c;
            --success-color: #27ae60;
            --warning-color: #f39c12;
            --background: #ecf0f1;
            --card-shadow: 0 2px 8px rgba(0, 0, 0, 0.08);
            --card-hover-shadow: 0 4px 16px rgba(0, 0, 0, 0.12);
        }
        
        * { margin: 0; padding: 0; box-sizing: border-box; }
        
        body {
            background: var(--background);
            font-family: -apple-system, BlinkMacSystemFont, 'Inter', 'Segoe UI', sans-serif;
            color: var(--primary-color);
            line-height: 1.6;
        }
        
        .top-navbar {
            background: white;
            border-bottom: 1px solid #dee2e6;
            padding: 20px 0;
            box-shadow: 0 2px 4px rgba(0,0,0,0.05);
            position: sticky;
            top: 0;
            z-index: 1000;
        }
        
        .navbar-content {
            max-width: 1400px;
            margin: 0 auto;
            padding: 0 30px;
            display: flex;
            justify-content: space-between;
            align-items: center;
        }
        
        .navbar-title {
            display: flex;
            align-items: center;
            gap: 15px;
        }
        
        .navbar-title h1 {
            font-size: 1.5rem;
            font-weight: 600;
            margin: 0;
            color: var(--primary-color);
        }
        
        .navbar-title p {
            margin: 0;
            color: #6c757d;
            font-size: 0.9rem;
        }
        
        .navbar-status {
            display: flex;
            align-items: center;
            gap: 20px;
        }
        
        .live-badge {
            background: var(--success-color);
            color: white;
            padding: 6px 14px;
            border-radius: 6px;
            display: flex;
            align-items: center;
            gap: 8px;
            font-weight: 500;
            font-size: 0.85rem;
            animation: pulse 2s infinite;
        }
        
        .real-data-badge {
            background: var(--secondary-color);
            color: white;
            padding: 6px 14px;
            border-radius: 6px;
            font-weight: 500;
            font-size: 0.85rem;
        }
        
        @keyframes pulse {
            0%, 100% { opacity: 1; }
            50% { opacity: 0.8; }
        }
        
        .container-main {
            max-width: 1400px;
            margin: 30px auto;
            padding: 0 30px;
        }
        
        .stats-grid {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
            gap: 20px;
            margin-bottom: 30px;
        }
        
        .stat-card {
            background: white;
            border-radius: 8px;
            padding: 24px;
            box-shadow: var(--card-shadow);
            transition: all 0.3s ease;
            border: 1px solid #e9ecef;
        }
        
        .stat-card:hover {
            box-shadow: var(--card-hover-shadow);
            transform: translateY(-2px);
        }
        
        .stat-header {
            display: flex;
            justify-content: space-between;
            align-items: flex-start;
            margin-bottom: 12px;
        }
        
        .stat-icon {
            width: 48px;
            height: 48px;
            border-radius: 8px;
            display: flex;
            align-items: center;
            justify-content: center;
            font-size: 1.3rem;
        }
        
        .stat-icon.primary { background: rgba(52, 152, 219, 0.1); color: var(--secondary-color); }
        .stat-icon.danger { background: rgba(231, 76, 60, 0.1); color: var(--danger-color); }
        .stat-icon.success { background: rgba(39, 174, 96, 0.1); color: var(--success-color); }
        .stat-icon.warning { background: rgba(243, 156, 18, 0.1); color: var(--warning-color); }
        
        .stat-value {
            font-size: 2rem;
            font-weight: 700;
            margin: 8px 0;
            color: var(--primary-color);
        }
        
        .stat-label {
            color: #6c757d;
            font-size: 0.85rem;
            font-weight: 500;
        }
        
        .chart-card {
            background: white;
            border-radius: 8px;
            padding: 24px;
            box-shadow: var(--card-shadow);
            margin-bottom: 30px;
            border: 1px solid #e9ecef;
        }
        
        .chart-header {
            display: flex;
            justify-content: space-between;
            align-items: center;
            margin-bottom: 20px;
            padding-bottom: 12px;
            border-bottom: 1px solid #e9ecef;
        }
        
        .chart-title {
            font-size: 1.1rem;
            font-weight: 600;
            color: var(--primary-color);
            display: flex;
            align-items: center;
            gap: 10px;
        }
        
        .chart-wrapper {
            position: relative;
            height: 380px;
        }
        
        .grid-2 {
            display: grid;
            grid-template-columns: repeat(auto-fit, minmax(500px, 1fr));
            gap: 30px;
        }
        
        .activity-feed {
            max-height: 600px;
            overflow-y: auto;
            padding-right: 10px;
        }
        
        .activity-item {
            background: #f8f9fa;
            border-radius: 8px;
            padding: 18px;
            margin-bottom: 12px;
            border-left: 3px solid #dee2e6;
            transition: all 0.2s ease;
        }
        
        .activity-item:hover {
            background: #e9ecef;
            transform: translateX(4px);
        }
        
        .activity-item.suspicious {
            border-left-color: var(--danger-color);
            background: rgba(231, 76, 60, 0.03);
        }
        
        .activity-item.safe {
            border-left-color: var(--success-color);
            background: rgba(39, 174, 96, 0.03);
        }
        
        .job-title {
            font-weight: 600;
            font-size: 1rem;
            margin-bottom: 8px;
            color: var(--primary-color);
        }
        
        .job-meta {
            display: flex;
            flex-wrap: wrap;
            gap: 16px;
            color: #6c757d;
            font-size: 0.85rem;
        }
        
        .job-meta span {
            display: flex;
            align-items: center;
            gap: 6px;
        }
        
        .badge {
            padding: 4px 10px;
            border-radius: 4px;
            font-size: 0.75rem;
            font-weight: 600;
            display: inline-block;
        }
        
        .badge-danger {
            background: rgba(231, 76, 60, 0.1);
            color: var(--danger-color);
        }
        
        .badge-success {
            background: rgba(39, 174, 96, 0.1);
            color: var(--success-color);
        }
        
        .empty-state {
            text-align: center;
            padding: 60px 20px;
            color: #95a5a6;
        }
        
        .empty-state i {
            font-size: 3rem;
            margin-bottom: 16px;
            opacity: 0.4;
        }
        
        .update-indicator {
            position: fixed;
            bottom: 30px;
            right: 30px;
            background: white;
            padding: 12px 20px;
            border-radius: 8px;
            box-shadow: 0 4px 12px rgba(0,0,0,0.15);
            display: none;
            align-items: center;
            gap: 10px;
            z-index: 1000;
            border: 1px solid #e9ecef;
        }
        
        .update-indicator.active { display: flex; }
        
        .update-spinner {
            width: 16px;
            height: 16px;
            border: 2px solid #e9ecef;
            border-top-color: var(--secondary-color);
            border-radius: 50%;
            animation: spin 0.8s linear infinite;
        }
        
        @keyframes spin {
            to { transform: rotate(360deg); }
        }
        
        ::-webkit-scrollbar { width: 6px; }
        ::-webkit-scrollbar-track { background: #f1f1f1; }
        ::-webkit-scrollbar-thumb { background: #cbd5e0; border-radius: 3px; }
        ::-webkit-scrollbar-thumb:hover { background: #a0aec0; }
        
        @media (max-width: 768px) {
            .grid-2 { grid-template-columns: 1fr; }
            .stats-grid { grid-template-columns: 1fr; }
            .container-main { padding: 0 15px; }
        }
    </style>
</head>
<body>
    <div class="top-navbar">
        <div class="navbar-content">
            <div class="navbar-title">
                <div>
                    <h1><i class="fas fa-shield-alt"></i> Scam Internship Detection System</h1>
                    <p>Real-Time Analytics Dashboard - Live Scraped Data Only</p>
                </div>
            </div>
            <div class="navbar-status">
                <div class="real-data-badge">
                    <i class="fas fa-database"></i>
                    <span>REAL DATA</span>
                </div>
                <div class="live-badge">
                    <i class="fas fa-circle" style="font-size: 0.6rem;"></i>
                    <span>LIVE</span>
                </div>
            </div>
        </div>
    </div>

    <div class="container-main">
        <!-- KPI Cards -->
        <div class="stats-grid">
            <div class="stat-card">
                <div class="stat-header">
                    <div>
                        <div class="stat-label">Total Internships Analyzed</div>
                        <div class="stat-value" id="totalJobs">0</div>
                    </div>
                    <div class="stat-icon primary">
                        <i class="fas fa-briefcase"></i>
                    </div>
                </div>
            </div>
            
            <div class="stat-card">
                <div class="stat-header">
                    <div>
                        <div class="stat-label">Scam Detected</div>
                        <div class="stat-value" style="color: var(--danger-color);" id="scamJobs">0</div>
                    </div>
                    <div class="stat-icon danger">
                        <i class="fas fa-exclamation-triangle"></i>
                    </div>
                </div>
            </div>
            
            <div class="stat-card">
                <div class="stat-header">
                    <div>
                        <div class="stat-label">Safe Internships</div>
                        <div class="stat-value" style="color: var(--success-color);" id="safeJobs">0</div>
                    </div>
                    <div class="stat-icon success">
                        <i class="fas fa-check-circle"></i>
                    </div>
                </div>
            </div>
            
            <div class="stat-card">
                <div class="stat-header">
                    <div>
                        <div class="stat-label">Scam Rate</div>
                        <div class="stat-value" style="color: var(--warning-color);" id="scamRate">0%</div>
                    </div>
                    <div class="stat-icon warning">
                        <i class="fas fa-percentage"></i>
                    </div>
                </div>
            </div>
        </div>

        <!-- Charts Row 1 -->
        <div class="grid-2">
            <div class="chart-card">
                <div class="chart-header">
                    <div class="chart-title">
                        <i class="fas fa-briefcase" style="color: var(--secondary-color);"></i>
                        Top 10 Internship Roles
                    </div>
                </div>
                <div class="chart-wrapper">
                    <canvas id="jobTitlesChart"></canvas>
                </div>
            </div>
            
            <div class="chart-card">
                <div class="chart-header">
                    <div class="chart-title">
                        <i class="fas fa-map-marker-alt" style="color: var(--success-color);"></i>
                        Top 10 Internship Locations
                    </div>
                </div>
                <div class="chart-wrapper">
                    <canvas id="locationsChart"></canvas>
                </div>
            </div>
        </div>

        <!-- Charts Row 2 -->
        <div class="grid-2">
            <div class="chart-card">
                <div class="chart-header">
                    <div class="chart-title">
                        <i class="fas fa-database" style="color: var(--warning-color);"></i>
                        Data Sources (Scraped Only)
                    </div>
                </div>
                <div class="chart-wrapper">
                    <canvas id="sourceChart"></canvas>
                </div>
            </div>
            
            <div class="chart-card">
                <div class="chart-header">
                    <div class="chart-title">
                        <i class="fas fa-shield-alt" style="color: var(--danger-color);"></i>
                        Detection Status
                    </div>
                </div>
                <div class="chart-wrapper">
                    <canvas id="fraudChart"></canvas>
                </div>
            </div>
        </div>

        <!-- NEW: Scam Keywords Chart (Full Width) -->
        <div class="chart-card">
            <div class="chart-header">
                <div class="chart-title">
                    <i class="fas fa-exclamation-circle" style="color: var(--danger-color);"></i>
                    Top 15 Scam Keywords Detected
                </div>
            </div>
            <div class="chart-wrapper">
                <canvas id="scamKeywordsChart"></canvas>
            </div>
        </div>

        <!-- Hourly Trends -->
        <div class="chart-card">
            <div class="chart-header">
                <div class="chart-title">
                    <i class="fas fa-chart-line" style="color: var(--secondary-color);"></i>
                    Hourly Activity Trends (Last 24 Hours)
                </div>
            </div>
            <div class="chart-wrapper">
                <canvas id="hourlyChart"></canvas>
            </div>
        </div>

        <!-- Live Activity Feed -->
        <div class="chart-card">
            <div class="chart-header">
                <div class="chart-title">
                    <i class="fas fa-stream" style="color: var(--secondary-color);"></i>
                    Live Activity Feed (Real Scraped Data)
                </div>
            </div>
            <div class="activity-feed" id="activityFeed">
                <div class="empty-state">
                    <i class="fas fa-spinner fa-spin"></i>
                    <p>Loading recent activity...</p>
                </div>
            </div>
        </div>
    </div>

    <div class="update-indicator" id="updateIndicator">
        <div class="update-spinner"></div>
        <span style="font-size: 0.9rem; color: var(--primary-color);">Updating...</span>
    </div>

    <script>
        const charts = {};
        const colors = {
            primary: '#3498db',
            danger: '#e74c3c',
            success: '#27ae60',
            warning: '#f39c12',
            purple: '#9b59b6',
            gradient: ['#3498db', '#9b59b6', '#e74c3c', '#f39c12', '#27ae60', '#16a085', '#34495e', '#e67e22']
        };

        function initCharts() {
            // Job Titles Chart
            charts.jobTitles = new Chart(document.getElementById('jobTitlesChart'), {
                type: 'bar',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Number of Jobs',
                        data: [],
                        backgroundColor: colors.primary,
                        borderRadius: 4,
                        barThickness: 30
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            padding: 12,
                            titleFont: { size: 13 },
                            bodyFont: { size: 12 }
                        }
                    },
                    scales: {
                        y: { 
                            beginAtZero: true,
                            grid: { color: '#f0f0f0' },
                            ticks: { font: { size: 11 } }
                        },
                        x: { 
                            grid: { display: false },
                            ticks: { font: { size: 11 } }
                        }
                    }
                }
            });

            // Locations Chart
            charts.locations = new Chart(document.getElementById('locationsChart'), {
                type: 'bar',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Number of Jobs',
                        data: [],
                        backgroundColor: colors.success,
                        borderRadius: 4,
                        barThickness: 30
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            padding: 12,
                            titleFont: { size: 13 },
                            bodyFont: { size: 12 }
                        }
                    },
                    scales: {
                        y: { 
                            beginAtZero: true,
                            grid: { color: '#f0f0f0' },
                            ticks: { font: { size: 11 } }
                        },
                        x: { 
                            grid: { display: false },
                            ticks: { font: { size: 11 } }
                        }
                    }
                }
            });

            // Source Distribution
            charts.source = new Chart(document.getElementById('sourceChart'), {
                type: 'doughnut',
                data: {
                    labels: [],
                    datasets: [{
                        data: [],
                        backgroundColor: colors.gradient,
                        borderWidth: 2,
                        borderColor: '#fff'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { 
                            position: 'right',
                            labels: { 
                                padding: 15,
                                font: { size: 11 }
                            }
                        }
                    }
                }
            });

            // Fraud Detection
            charts.fraud = new Chart(document.getElementById('fraudChart'), {
                type: 'pie',
                data: {
                    labels: ['Safe Jobs', 'Scam Jobs'],
                    datasets: [{
                        data: [0, 0],
                        backgroundColor: [colors.success, colors.danger],
                        borderWidth: 2,
                        borderColor: '#fff'
                    }]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { 
                            position: 'right',
                            labels: { 
                                padding: 15,
                                font: { size: 11 }
                            }
                        }
                    }
                }
            });

            // NEW: Scam Keywords Chart (Horizontal Bar)
            charts.scamKeywords = new Chart(document.getElementById('scamKeywordsChart'), {
                type: 'bar',
                data: {
                    labels: [],
                    datasets: [{
                        label: 'Frequency Count',
                        data: [],
                        backgroundColor: colors.danger,
                        borderRadius: 4,
                        barThickness: 20
                    }]
                },
                options: {
                    indexAxis: 'y',
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { display: false },
                        tooltip: {
                            backgroundColor: 'rgba(0, 0, 0, 0.8)',
                            padding: 12,
                            titleFont: { size: 13 },
                            bodyFont: { size: 12 },
                            callbacks: {
                                label: function(context) {
                                    return 'Detected: ' + context.parsed.x + ' times';
                                }
                            }
                        }
                    },
                    scales: {
                        x: { 
                            beginAtZero: true,
                            grid: { color: '#f0f0f0' },
                            ticks: { 
                                font: { size: 11 },
                                precision: 0
                            },
                            title: {
                                display: true,
                                text: 'Frequency',
                                font: { size: 12, weight: 'bold' }
                            }
                        },
                        y: { 
                            grid: { display: false },
                            ticks: { 
                                font: { size: 11 },
                                autoSkip: false
                            }
                        }
                    }
                }
            });

            // Hourly Trends
            charts.hourly = new Chart(document.getElementById('hourlyChart'), {
                type: 'line',
                data: {
                    labels: [],
                    datasets: [
                        {
                            label: 'Total Jobs',
                            data: [],
                            borderColor: colors.primary,
                            backgroundColor: 'rgba(52, 152, 219, 0.1)',
                            fill: true,
                            tension: 0.4,
                            borderWidth: 2,
                            pointRadius: 3,
                            pointHoverRadius: 5
                        },
                        {
                            label: 'Scam Jobs',
                            data: [],
                            borderColor: colors.danger,
                            backgroundColor: 'rgba(231, 76, 60, 0.1)',
                            fill: true,
                            tension: 0.4,
                            borderWidth: 2,
                            pointRadius: 3,
                            pointHoverRadius: 5
                        }
                    ]
                },
                options: {
                    responsive: true,
                    maintainAspectRatio: false,
                    plugins: {
                        legend: { 
                            display: true,
                            labels: { font: { size: 11 } }
                        }
                    },
                    scales: {
                        y: { 
                            beginAtZero: true,
                            grid: { color: '#f0f0f0' }
                        },
                        x: { 
                            grid: { display: false }
                        }
                    }
                }
            });
        }

        async function updateStats() {
            const data = await fetch('/api/stats/overview').then(r => r.json());
            document.getElementById('totalJobs').textContent = data.total_jobs.toLocaleString();
            document.getElementById('scamJobs').textContent = data.scam_jobs.toLocaleString();
            document.getElementById('safeJobs').textContent = data.safe_jobs.toLocaleString();
            document.getElementById('scamRate').textContent = data.scam_rate + '%';
        }

        async function updateCharts() {
            const [titles, locations, source, fraud, keywords, hourly] = await Promise.all([
                fetch('/api/charts/job_titles').then(r => r.json()),
                fetch('/api/charts/locations').then(r => r.json()),
                fetch('/api/charts/source_distribution').then(r => r.json()),
                fetch('/api/charts/fraud_distribution').then(r => r.json()),
                fetch('/api/charts/scam_keywords').then(r => r.json()),
                fetch('/api/charts/hourly_trends').then(r => r.json())
            ]);

            charts.jobTitles.data.labels = titles.labels;
            charts.jobTitles.data.datasets[0].data = titles.counts;
            charts.jobTitles.update();

            charts.locations.data.labels = locations.labels;
            charts.locations.data.datasets[0].data = locations.counts;
            charts.locations.update();

            charts.source.data.labels = source.labels;
            charts.source.data.datasets[0].data = source.counts;
            charts.source.update();

            charts.fraud.data.datasets[0].data = [fraud.safe, fraud.scam];
            charts.fraud.update();

            // NEW: Update Scam Keywords Chart
            charts.scamKeywords.data.labels = keywords.labels;
            charts.scamKeywords.data.datasets[0].data = keywords.counts;
            charts.scamKeywords.update();

            charts.hourly.data.labels = hourly.labels;
            charts.hourly.data.datasets[0].data = hourly.total;
            charts.hourly.data.datasets[1].data = hourly.scams;
            charts.hourly.update();
        }

        async function updateFeed() {
            const jobs = await fetch('/api/activity/recent').then(r => r.json());
            const feed = document.getElementById('activityFeed');
            
            if (jobs.length === 0) {
                feed.innerHTML = '<div class="empty-state"><i class="fas fa-inbox"></i><p>No recent activity from scraped sources</p></div>';
                return;
            }
            
            feed.innerHTML = jobs.map(job => {
                const timeAgo = (() => {
                    const s = Math.floor((new Date() - new Date(job.time)) / 1000);
                    if (s < 60) return 'Just now';
                    if (s < 3600) return Math.floor(s / 60) + 'm ago';
                    if (s < 86400) return Math.floor(s / 3600) + 'h ago';
                    return Math.floor(s / 86400) + 'd ago';
                })();
                
                const confidencePercent = job.fraudulent 
                    ? (job.confidence * 100).toFixed(0) 
                    : (100 - job.confidence * 100).toFixed(0);
                
                return `
                    <div class="activity-item ${job.fraudulent ? 'suspicious' : 'safe'}">
                        <div class="d-flex justify-content-between align-items-start">
                            <div class="flex-grow-1">
                                <div class="job-title">${job.title}</div>
                                <div class="job-meta">
                                    <span><i class="fas fa-building"></i> ${job.company}</span>
                                    <span><i class="fas fa-map-marker-alt"></i> ${job.location || 'N/A'}</span>
                                    <span><i class="fas fa-database"></i> ${job.source}</span>
                                    <span><i class="fas fa-clock"></i> ${timeAgo}</span>
                                </div>
                            </div>
                            <div>
                                <span class="badge ${job.fraudulent ? 'badge-danger' : 'badge-success'}">
                                    ${job.fraudulent ? 'SCAM' : 'SAFE'} ${confidencePercent}%
                                </span>
                            </div>
                        </div>
                    </div>
                `;
            }).join('');
        }

        async function updateDashboard() {
            const indicator = document.getElementById('updateIndicator');
            indicator.classList.add('active');
            try {
                await Promise.all([updateStats(), updateCharts(), updateFeed()]);
            } finally {
                setTimeout(() => indicator.classList.remove('active'), 500);
            }
        }

        document.addEventListener('DOMContentLoaded', () => {
            initCharts();
            updateDashboard();
            setInterval(updateDashboard, 5000);
        });
    </script>
</body>
</html>'''

@app.route('/')
def index():
    """Main analytics dashboard page"""
    return HTML_TEMPLATE

@app.route('/api/stats/overview')
def stats_overview():
    """Get overall statistics (INCLUDING KAGGLE DATA)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Include Kaggle data for KPIs
        data_filter = get_data_filter(include_kaggle=True)
        
        # Total jobs that have been analyzed (excluding NULL fraudulent values)
        cursor.execute(f"SELECT COUNT(*) FROM jobs WHERE {data_filter} AND fraudulent IS NOT NULL")
        total_jobs = cursor.fetchone()[0]
        
        # Scam jobs
        cursor.execute(f"SELECT COUNT(*) FROM jobs WHERE {data_filter} AND fraudulent = true")
        scam_jobs = cursor.fetchone()[0]
        
        # Safe jobs
        cursor.execute(f"SELECT COUNT(*) FROM jobs WHERE {data_filter} AND fraudulent = false")
        safe_jobs = cursor.fetchone()[0]
        
        # Scam rate
        scam_rate = (scam_jobs / total_jobs * 100) if total_jobs > 0 else 0
        
        cursor.close()
        conn.close()
        
        return jsonify({
            'total_jobs': total_jobs,
            'scam_jobs': scam_jobs,
            'safe_jobs': safe_jobs,
            'scam_rate': round(scam_rate, 2)
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/charts/source_distribution')
def chart_source_distribution():
    """Get data source distribution (REAL SCRAPED DATA ONLY - NO KAGGLE)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Exclude Kaggle for this chart
        data_filter = get_data_filter(include_kaggle=False)
        
        cursor.execute(f"""
            SELECT data_source, COUNT(*) as count
            FROM jobs 
            WHERE {data_filter}
            GROUP BY data_source
            ORDER BY count DESC
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            'labels': [row[0] for row in results],
            'counts': [row[1] for row in results]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/charts/fraud_distribution')
def chart_fraud_distribution():
    """Get fraud vs safe distribution (INCLUDING KAGGLE DATA)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Include Kaggle data for Detection Status chart
        data_filter = get_data_filter(include_kaggle=True)
        
        # Only count jobs that have been analyzed (fraudulent IS NOT NULL)
        cursor.execute(f"""
            SELECT fraudulent, COUNT(*) 
            FROM jobs 
            WHERE {data_filter}
              AND fraudulent IS NOT NULL
            GROUP BY fraudulent
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        data = {'safe': 0, 'scam': 0}
        for row in results:
            if row[0]:
                data['scam'] = row[1]
            else:
                data['safe'] = row[1]
        
        return jsonify(data)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/charts/scam_keywords')
def chart_scam_keywords():
    """Get top 15 most frequent scam keywords (REAL SCRAPED DATA ONLY - NO KAGGLE)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Exclude Kaggle for scam keywords
        data_filter = get_data_filter(include_kaggle=False)
        
        # Get all scam reasons from fraudulent jobs
        # scam_reasons is a PostgreSQL ARRAY column
        cursor.execute(f"""
            SELECT scam_reasons
            FROM jobs 
            WHERE {data_filter}
              AND fraudulent = true
              AND scam_reasons IS NOT NULL
              AND array_length(scam_reasons, 1) > 0
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Count keywords from the array column
        keyword_counts = {}
        for row in results:
            scam_reasons_array = row[0]
            if not scam_reasons_array:
                continue
            
            # scam_reasons is already a Python list from psycopg2
            for reason in scam_reasons_array:
                if reason and str(reason).strip() and str(reason).lower() not in ['none', 'null', '']:
                    clean_reason = str(reason).strip()
                    keyword_counts[clean_reason] = keyword_counts.get(clean_reason, 0) + 1
        
        # If no keywords found, return empty data
        if not keyword_counts:
            return jsonify({
                'labels': ['No scam reasons detected yet'],
                'counts': [0]
            })
        
        # Sort by frequency and get top 15
        sorted_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        
        return jsonify({
            'labels': [kw[0] for kw in sorted_keywords],
            'counts': [kw[1] for kw in sorted_keywords]
        })
    except Exception as e:
        print(f"Error in scam_keywords endpoint: {e}")
        return jsonify({'error': str(e), 'labels': ['Error loading data'], 'counts': [0]}), 200
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        # Parse and count keywords
        keyword_counts = {}
        for row in results:
            keywords_raw = row[0]
            if not keywords_raw:
                continue
                
            # Parse the keywords (handle different formats)
            try:
                # Try JSON parsing first
                if keywords_raw.startswith('['):
                    keywords = json.loads(keywords_raw)
                else:
                    # Handle comma-separated or other formats
                    keywords = [k.strip() for k in keywords_raw.split(',')]
                
                # Clean and count each keyword
                for keyword in keywords:
                    # Remove quotes, brackets, extra whitespace
                    clean_keyword = re.sub(r'[\[\]\'"{}]', '', str(keyword)).strip()
                    if clean_keyword and clean_keyword.lower() != 'none':
                        keyword_counts[clean_keyword] = keyword_counts.get(clean_keyword, 0) + 1
            except:
                # If parsing fails, try simple split
                keywords = [k.strip() for k in str(keywords_raw).replace('[', '').replace(']', '').replace('"', '').replace("'", '').split(',')]
                for keyword in keywords:
                    clean_keyword = keyword.strip()
                    if clean_keyword and clean_keyword.lower() != 'none':
                        keyword_counts[clean_keyword] = keyword_counts.get(clean_keyword, 0) + 1
        
        # Sort by frequency and get top 15
        sorted_keywords = sorted(keyword_counts.items(), key=lambda x: x[1], reverse=True)[:15]
        
        return jsonify({
            'labels': [kw[0] for kw in sorted_keywords],
            'counts': [kw[1] for kw in sorted_keywords]
        })
    except Exception as e:
        print(f"Error in scam_keywords endpoint: {e}")
        return jsonify({'error': str(e), 'labels': [], 'counts': []}), 500

@app.route('/api/charts/hourly_trends')
def chart_hourly_trends():
    """Get hourly job posting trends (last 24 hours) - REAL SCRAPED DATA ONLY"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Exclude Kaggle for hourly trends
        data_filter = get_data_filter(include_kaggle=False)
        
        # Only count jobs that have been analyzed (fraudulent IS NOT NULL)
        cursor.execute(f"""
            SELECT 
                DATE_TRUNC('hour', scraped_at) as hour,
                COUNT(*) as total,
                SUM(CASE WHEN fraudulent = true THEN 1 ELSE 0 END) as scams
            FROM jobs 
            WHERE scraped_at >= NOW() - INTERVAL '24 hours'
              AND {data_filter}
              AND fraudulent IS NOT NULL
            GROUP BY DATE_TRUNC('hour', scraped_at)
            ORDER BY hour
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            'labels': [row[0].strftime('%H:%M') if row[0] else 'N/A' for row in results],
            'total': [row[1] for row in results],
            'scams': [row[2] for row in results]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/charts/job_titles')
def chart_job_titles():
    """Get top 10 job titles (REAL SCRAPED DATA ONLY - NO KAGGLE)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Exclude Kaggle for job titles chart
        data_filter = get_data_filter(include_kaggle=False)
        
        cursor.execute(f"""
            SELECT job_title, COUNT(*) as count
            FROM jobs 
            WHERE job_title IS NOT NULL 
              AND job_title != 'Unknown'
              AND job_title != ''
              AND {data_filter}
            GROUP BY job_title
            ORDER BY count DESC
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        labels = []
        counts = []
        for row in results:
            # Truncate long titles
            title = row[0][:50] + '...' if len(row[0]) > 50 else row[0]
            labels.append(title)
            counts.append(row[1])
        
        return jsonify({'labels': labels, 'counts': counts})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/charts/locations')
def chart_locations():
    """Get top 10 job locations (REAL SCRAPED DATA ONLY - NO KAGGLE)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Exclude Kaggle for locations chart
        data_filter = get_data_filter(include_kaggle=False)
        
        cursor.execute(f"""
            SELECT location, COUNT(*) as count
            FROM jobs 
            WHERE location IS NOT NULL 
              AND location != ''
              AND {data_filter}
            GROUP BY location
            ORDER BY count DESC
            LIMIT 10
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        return jsonify({
            'labels': [row[0] for row in results],
            'counts': [row[1] for row in results]
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/activity/recent')
def activity_recent():
    """Get recent job activity (REAL SCRAPED DATA ONLY - NO KAGGLE)"""
    try:
        conn = get_db_connection()
        cursor = conn.cursor(cursor_factory=RealDictCursor)
        
        # Exclude Kaggle for activity feed
        data_filter = get_data_filter(include_kaggle=False)
        
        cursor.execute(f"""
            SELECT 
                job_title,
                company_name,
                location,
                data_source,
                fraudulent,
                scam_confidence,
                scraped_at
            FROM jobs
            WHERE {data_filter}
            ORDER BY scraped_at DESC
            LIMIT 20
        """)
        
        results = cursor.fetchall()
        cursor.close()
        conn.close()
        
        jobs = []
        for row in results:
            jobs.append({
                'title': row['job_title'],
                'company': row['company_name'],
                'location': row['location'],
                'source': row['data_source'],
                'fraudulent': row['fraudulent'],
                'confidence': float(row['scam_confidence']) if row['scam_confidence'] else 0,
                'time': row['scraped_at'].isoformat()
            })
        
        return jsonify(jobs)
    except Exception as e:
        return jsonify({'error': str(e)}), 500

def print_banner():
    print("=" * 80)
    print("  SCAM INTERNSHIP DETECTION SYSTEM - ANALYTICS DASHBOARD")
    print("  HYBRID DATA MODE")
    print("=" * 80)
    print(f"  Dashboard URL: http://localhost:5001")
    print(f"  Database: PostgreSQL (scamjobs)")
    print(f"  Auto-refresh: Every 5 seconds")
    print("=" * 80)
    print("\n   KPI Cards & Detection Status Chart:")
    print("  ✓ indeed_scraped")
    print("  ✓ naukri_scraped")
    print("  ✓ linkedin_scraped")
    print("  ✓ user_submitted")
    print("  ✓ susjobs_kaggle (INCLUDED)")
    print("\n   All Other Charts (Job Titles, Locations, Keywords, etc):")
    print("  ✓ indeed_scraped")
    print("  ✓ naukri_scraped")
    print("  ✓ linkedin_scraped")
    print("  ✓ user_submitted")
    print("  ✗ susjobs_kaggle (EXCLUDED)")
    print("=" * 80)
    print("\n  Features:")
    print("  ✓ Clean & Professional UI Design")
    print("  ✓ Real-time KPI Monitoring (with Kaggle)")
    print("  ✓ Detection Status Chart (with Kaggle)")
    print("  ✓ Top 10 Job Roles Analysis (scraped only)")
    print("  ✓ Geographic Distribution (scraped only)")
    print("  ✓ Data Source Tracking (scraped only)")
    print("  ✓ Top 15 Scam Keywords (scraped only)")
    print("  ✓ 24-Hour Activity Trends (scraped only)")
    print("  ✓ Live Activity Feed (scraped only)")
    print("=" * 80)
    print("\n  Press Ctrl+C to stop\n")

if __name__ == '__main__':
    print_banner()
    
    # Test database connection
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        
        # Show stats with Kaggle (for KPIs)
        data_filter_with_kaggle = get_data_filter(include_kaggle=True)
        cursor.execute(f"SELECT COUNT(*) FROM jobs WHERE {data_filter_with_kaggle}")
        count_with_kaggle = cursor.fetchone()[0]
        cursor.execute(f"SELECT COUNT(*) FROM jobs WHERE {data_filter_with_kaggle} AND fraudulent = true")
        scam_with_kaggle = cursor.fetchone()[0]
        
        # Show stats without Kaggle (for other charts)
        data_filter_no_kaggle = get_data_filter(include_kaggle=False)
        cursor.execute(f"SELECT COUNT(*) FROM jobs WHERE {data_filter_no_kaggle}")
        count_no_kaggle = cursor.fetchone()[0]
        
        print(f"   Database connected successfully!")
        print(f"\n    KPI Stats (WITH Kaggle):")
        print(f"   Total Internships: {count_with_kaggle}")
        print(f"   Scam Internships: {scam_with_kaggle}")
        
        print(f"\n    Chart Stats (WITHOUT Kaggle):")
        print(f"   Scraped Internships: {count_no_kaggle}")
        
        # Show breakdown by source (with Kaggle)
        cursor.execute(f"""
            SELECT data_source, COUNT(*) 
            FROM jobs 
            WHERE {data_filter_with_kaggle}
            GROUP BY data_source
            ORDER BY data_source
        """)
        sources = cursor.fetchall()
        if sources:
            print("\n   Breakdown by source:")
            for source, source_count in sources:
                kaggle_marker = " (for KPIs only)" if source == "susjobs_kaggle" else ""
                print(f"   - {source}: {source_count} internships{kaggle_marker}")
        
        print()
        cursor.close()
        conn.close()
    except Exception as e:
        print(f"   Database connection failed: {e}")
        print("  Make sure PostgreSQL is running and database 'scamjobs' exists\n")
        sys.exit(1)
    
    app.run(debug=True, host='0.0.0.0', port=5001, use_reloader=False)