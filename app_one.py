# run_all.py - SINGLE COMMAND TO RUN EVERYTHING (INCLUDING KAFKA CHECK)
"""
Run this file to start ALL services with ONE command:
    python run_all.py

It will:
1. Check if Kafka is running (and tell you to start it if not)
2. Start Spark Streaming (processes Kafka messages)
3. Start LinkedIn/Indeed Scraper (sends jobs to Kafka)
4. Start Flask API (web interface)

Press Ctrl+C to stop everything
"""

import subprocess
import sys
import os
import time
import signal
import threading
from pathlib import Path

# Fix Windows Unicode encoding
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# ============= CONFIGURATION =============
BASE_DIR = Path(__file__).parent.absolute()
PYTHON_PATH = sys.executable

# Process tracking
processes = []
running = True

def print_banner():
    print("=" * 80)
    print("SCAM DETECTOR - ALL-IN-ONE LAUNCHER")
    print("=" * 80)
    print(f" Base Directory: {BASE_DIR}")
    print(f" Python: {PYTHON_PATH}")
    print("=" * 80)
    print()

def check_kafka_running():
    """Check if Kafka is actually running"""
    print(" Checking if Kafka is running...")
    
    try:
        from kafka import KafkaProducer
        from kafka.errors import NoBrokersAvailable
        
        producer = KafkaProducer(
            bootstrap_servers=['localhost:9092'],
            request_timeout_ms=5000
        )
        producer.close()
        print(" Kafka is running on localhost:9092")
        return True
        
    except NoBrokersAvailable:
        print("\n" + "="*80)
        print(" KAFKA IS NOT RUNNING!")
        print("="*80)
        print("\n  You MUST start Kafka before running this script.")
        print("\n Quick Start Guide:")
        print("\n1. Open TWO new Command Prompts:")
        print("\n   Terminal 1 (Zookeeper):")
        print("   cd C:\\kafka_2.13-3.7.0")
        print("   bin\\windows\\zookeeper-server-start.bat config\\zookeeper.properties")
        print("\n   Terminal 2 (Kafka - wait 10 seconds after starting Zookeeper):")
        print("   cd C:\\kafka_2.13-3.7.0")
        print("   bin\\windows\\kafka-server-start.bat config\\server.properties")
        print("\n2. Wait 15 seconds for Kafka to fully start")
        print("\n3. Run this script again: python run_all.py")
        print("\n" + "="*80)
        return False
        
    except ImportError:
        print(" kafka-python not installed. Run: pip install kafka-python")
        return False
        
    except Exception as e:
        print(f" Error checking Kafka: {e}")
        return False

def cleanup_processes():
    """Kill all child processes"""
    global running
    running = False
    print("\n Shutting down all processes...")
    
    for name, proc in processes:
        try:
            print(f"   Stopping {name}...")
            if sys.platform == 'win32':
                # Windows: Kill process tree
                subprocess.run(['taskkill', '/F', '/T', '/PID', str(proc.pid)], 
                             capture_output=True)
            else:
                proc.terminate()
                proc.wait(timeout=5)
        except subprocess.TimeoutExpired:
            proc.kill()
        except Exception as e:
            print(f"   Error stopping {name}: {e}")
    
    print(" All processes stopped")
    sys.exit(0)

def signal_handler(sig, frame):
    """Handle Ctrl+C"""
    cleanup_processes()

def stream_output(process, name, color_code):
    """Stream process output with colored prefix"""
    try:
        for line in iter(process.stdout.readline, ''):
            if not running:
                break
            if line and line.strip():
                print(f"\033[{color_code}m[{name}]\033[0m {line.strip()}")
    except Exception as e:
        if running:
            print(f" {name} output stream ended: {e}")

def start_process(name, command, color_code, wait_time=0, show_output=True):
    """Start a subprocess and track it"""
    print(f" Starting {name}...")
    
    try:
        if wait_time > 0:
            print(f"   Waiting {wait_time}s before starting...")
            time.sleep(wait_time)
        
        # Start process
        proc = subprocess.Popen(
            command,
            cwd=str(BASE_DIR),
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            stdin=subprocess.DEVNULL,
            text=True,
            bufsize=1,
            universal_newlines=True,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == 'win32' else 0
        )
        
        processes.append((name, proc))
        print(f" {name} started (PID: {proc.pid})")
        
        # Start output streaming thread ONLY if show_output is True
        if show_output:
            thread = threading.Thread(
                target=stream_output,
                args=(proc, name, color_code),
                daemon=True
            )
            thread.start()
        
        return proc
    
    except Exception as e:
        print(f" Failed to start {name}: {e}")
        return None

def check_prerequisites():
    """Check if required files exist"""
    print(" Checking prerequisites...\n")
    
    required_files = [
        "spark_streaming/streaming_processor.py",
        "kafka_pipeline/producer/linkedin_scrapper_producer.py",
        "app_integrated.py"
    ]
    
    missing = []
    for file in required_files:
        path = BASE_DIR / file
        if path.exists():
            print(f"    {file}")
        else:
            print(f"    {file} - NOT FOUND")
            missing.append(file)
    
    if missing:
        print(f"\n Missing files: {missing}")
        print("Please ensure all files are in the correct location.")
        return False
    
    print()
    return True

def monitor_processes():
    """Monitor processes and restart if they die unexpectedly"""
    restart_attempts = {}
    max_restarts = 3
    
    while running:
        time.sleep(2)
        
        for name, proc in processes[:]:  # Copy list to avoid modification during iteration
            if proc.poll() is not None:  # Process ended
                exit_code = proc.returncode
                
                # Track restart attempts
                if name not in restart_attempts:
                    restart_attempts[name] = 0
                
                if exit_code == 0:
                    # Normal exit (e.g., user stopped it)
                    continue
                else:
                    # Abnormal exit
                    restart_attempts[name] += 1
                    
                    if restart_attempts[name] <= max_restarts:
                        print(f"\n  {name} stopped unexpectedly (exit code: {exit_code})")
                        print(f"   Restart attempt {restart_attempts[name]}/{max_restarts}...")
                        
                        # Remove dead process
                        processes.remove((name, proc))
                        
                        # Restart based on service type
                        if "SPARK" in name:
                            start_process(
                                name="SPARK STREAMING",
                                command=[PYTHON_PATH, "-m", "spark_streaming.streaming_processor"],
                                color_code="94",
                                wait_time=2
                            )
                        elif "SCRAPER" in name:
                            start_process(
                                name="SCRAPER",
                                command=[PYTHON_PATH, "-m", "kafka_pipeline.producer.linkedin_scrapper_producer"],
                                color_code="92",
                                wait_time=2
                            )
                        elif "FLASK" in name:
                            start_process(
                                name="FLASK API",
                                command=[PYTHON_PATH, "app_integrated.py"],
                                color_code="93",
                                wait_time=2
                            )
                    else:
                        print(f"\n {name} failed {max_restarts} times. Not restarting.")

def main():
    global running
    
    print_banner()
    
    # Step 1: Check Kafka
    if not check_kafka_running():
        sys.exit(1)
    
    print()
    
    # Step 2: Check prerequisites
    if not check_prerequisites():
        sys.exit(1)
    
    # Set up signal handler for Ctrl+C
    signal.signal(signal.SIGINT, signal_handler)
    if sys.platform != 'win32':
        signal.signal(signal.SIGTERM, signal_handler)
    
    print(" Starting all services...\n")
    
    # Step 3: Start Spark Streaming (processes Kafka messages)
    spark_proc = start_process(
        name="SPARK STREAMING",
        command=[PYTHON_PATH, "-m", "spark_streaming.streaming_processor"],
        color_code="94",  # Blue
        wait_time=0
    )
    
    if not spark_proc:
        print(" Failed to start Spark Streaming. Exiting.")
        sys.exit(1)
    
    # Step 4: Wait for Spark to initialize
    print("\n Waiting for Spark to initialize (15 seconds)...\n")
    time.sleep(15)
    
    # Step 5: Start Scraper (sends jobs to Kafka)
    # Try LinkedIn scraper first, fall back to Indeed
    scraper_started = False
    
    linkedin_scraper = BASE_DIR / "kafka_pipeline/producer/linkedin_scrapper_producer.py"
    indeed_scraper = BASE_DIR / "kafka_pipeline/producer/indeed_scraper_producer.py"
    
    if linkedin_scraper.exists():
        print(" Using LinkedIn Scraper")
        scraper_proc = start_process(
            name="SCRAPER (LinkedIn)",
            command=[PYTHON_PATH, "-m", "kafka_pipeline.producer.linkedin_scrapper_producer"],
            color_code="92",  # Green
            wait_time=0,
            show_output=True  # ← SHOW scraper output
        )
        scraper_started = scraper_proc is not None
    
    if not scraper_started and indeed_scraper.exists():
        print(" Using Indeed Scraper")
        scraper_proc = start_process(
            name="SCRAPER (Indeed)",
            command=[PYTHON_PATH, "-m", "kafka_pipeline.producer.indeed_scraper_producer"],
            color_code="92",  # Green
            wait_time=0,
            show_output=True  # ← SHOW scraper output
        )
        scraper_started = scraper_proc is not None
    
    if not scraper_started:
        print("  No scraper found, but continuing with Spark and Flask...")
        print("    You can still test the system manually via the dashboard")
    
    # Step 6: Wait a bit, then start Flask
    print("\n Waiting for scraper to start (5 seconds)...\n")
    time.sleep(5)
    
    # Step 7: Start Flask API (web interface) - HIDDEN OUTPUT
    flask_proc = start_process(
        name="FLASK API",
        command=[PYTHON_PATH, "app_integrated.py"],
        color_code="93",  # Yellow
        wait_time=0,
        show_output=False  # ← HIDE Flask logs
    )
    
    print("\n" + "=" * 80)
    print(" ALL SERVICES RUNNING")
    print("=" * 80)
    print("\n Dashboard: http://localhost:5000")
    print("\n What's happening:")
    print("   1.  Spark Streaming is processing Kafka messages → Database")
    print("   2.  Scraper is sending jobs → Kafka")
    print("   3.  Flask API is running in background (logs hidden)")
    print("\n Console shows:")
    print("   🔵 [SPARK STREAMING] - Processing jobs")
    print("   🟢 [SCRAPER (LinkedIn)] - Scraping jobs")
    print("\n  Press Ctrl+C to stop all services\n")
    print("=" * 80)
    
    # Keep main thread alive and monitor processes
    try:
        monitor_processes()
    
    except KeyboardInterrupt:
        cleanup_processes()
    
    # If we get here, something went wrong
    cleanup_processes()

if __name__ == "__main__":
    main()