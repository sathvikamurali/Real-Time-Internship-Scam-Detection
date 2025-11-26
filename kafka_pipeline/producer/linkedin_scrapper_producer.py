import time
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from kafka import KafkaProducer
import json
import hashlib
from datetime import datetime
import sys
import os


# Fix Windows Unicode encoding issues
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')
    sys.stderr.reconfigure(encoding='utf-8')
    os.environ['PYTHONIOENCODING'] = 'utf-8'

# ---------------- CONFIGURATION ----------------
INDEED_SEARCH_URL = "https://in.indeed.com/jobs?q=internship+fresher+trainee&l=India&fromage=1"
NAUKRI_SEARCH_URL = "https://www.naukri.com/internship-fresher-trainee-jobs-in-india?experience=0"
LINKEDIN_SEARCH_URL = "https://www.linkedin.com/jobs/search/?keywords=internship%20fresher%20trainee&location=India&f_TPR=r86400&f_E=1%2C2"

SCAM_KEYWORDS = [
    'pay fee', 'registration fee', 'deposit', 'training kit',
    'urgent hiring', 'immediate joining', 'whatsapp', 'telegram',
    'no experience needed', 'earn money', 'work from home guaranteed'
]

KAFKA_BROKER = 'localhost:9092'
KAFKA_TOPIC = 'job-postings'
CSV_FILE = "all_jobs_log.csv"
SEEN_JOBS_FILE = "seen_all_jobs.txt"

# Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def load_seen_jobs():
    try:
        with open(SEEN_JOBS_FILE, 'r', encoding='utf-8') as f:
            return set(line.strip() for line in f)
    except FileNotFoundError:
        return set()

def save_seen_job(job_id):
    with open(SEEN_JOBS_FILE, 'a', encoding='utf-8') as f:
        f.write(f"{job_id}\n")

def generate_job_id(title, company):
    return hashlib.md5(f"{title}|{company}".lower().encode()).hexdigest()

# ============ INDEED SCRAPER ============
def scrape_indeed_jobs(driver, url, keywords, seen_jobs):
    print(f"\n{'='*80}")
    print("[INDEED] Starting scrape...")
    print(f"{'='*80}")
    
    try:
        driver.get(url)
        time.sleep(5)
        
        all_postings = []
        scraped_count = 0
        skipped_count = 0
        
        # Close popup if exists
        try:
            close_button = driver.find_element(By.CSS_SELECTOR, 'button[aria-label="Close"]')
            close_button.click()
            time.sleep(1)
        except:
            pass
        
        # Find job cards
        job_cards = driver.find_elements(By.CSS_SELECTOR, 'div.job_seen_beacon')
        print(f"[INDEED] Found {len(job_cards)} job cards")
        
        for idx, card in enumerate(job_cards, 1):
            try:
                title = card.find_element(By.CSS_SELECTOR, 'h2.jobTitle span').text
                company = card.find_element(By.CSS_SELECTOR, 'span[data-testid="company-name"]').text
                location = card.find_element(By.CSS_SELECTOR, 'div[data-testid="text-location"]').text
                
                job_id = generate_job_id(title, company)
                
                if job_id in seen_jobs:
                    skipped_count += 1
                    continue
                
                snippet = ""
                try:
                    snippet = card.find_element(By.CSS_SELECTOR, 'div.job-snippet').text
                except:
                    try:
                        snippet = card.find_element(By.CSS_SELECTOR, 'div[class*="snippet"]').text
                    except:
                        snippet = "No description available"
                
                snippet = snippet.strip() if snippet else "No description available"
                
                full_text = f"{title} {company} {snippet}".lower()
                found_keywords = [kw for kw in keywords if kw in full_text]
                
                posting = {
                    'job_id': job_id,
                    'job_title': title,
                    'company_name': company,
                    'location': location,
                    'job_description': snippet,
                    'experience_required': 'Not specified',
                    'salary': 'Not disclosed',
                    'detected_keywords': found_keywords,
                    'is_suspicious': len(found_keywords) > 0,
                    'suspicion_score': min(len(found_keywords) * 10, 100),
                    'data_source': 'indeed_scraped',
                    'scraped_at': datetime.now().isoformat(),
                    'posted_date': datetime.now().strftime('%Y-%m-%d')
                }
                
                all_postings.append(posting)
                scraped_count += 1
                producer.send(KAFKA_TOPIC, posting)
                seen_jobs.add(job_id)
                save_seen_job(job_id)
                
                flag = "[SUSPICIOUS]" if posting['is_suspicious'] else "[SAFE]"
                print(f"[INDEED Job {idx}] {flag} {title} @ {company}")
                
            except Exception as e:
                continue
        
        print(f"[INDEED] Scraped: {scraped_count} | Skipped: {skipped_count}")
        return all_postings
        
    except Exception as e:
        print(f"[INDEED] Error: {e}")
        return []

# ============ NAUKRI SCRAPER ============
def scrape_naukri_jobs(driver, url, keywords, seen_jobs):
    print(f"\n{'='*80}")
    print("[NAUKRI] Starting scrape...")
    print(f"{'='*80}")
    
    try:
        driver.get(url)
        time.sleep(5)
        
        all_postings = []
        scraped_count = 0
        skipped_count = 0
        
        # Close popups
        try:
            close_buttons = driver.find_elements(By.CSS_SELECTOR, 'button.crossIcon, div.close')
            for btn in close_buttons:
                try:
                    btn.click()
                    time.sleep(1)
                except:
                    pass
        except:
            pass
        
        # Scroll to load more
        for i in range(3):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        job_cards = driver.find_elements(By.CSS_SELECTOR, 'article.jobTuple, div.jobTuple, div.srp-jobtuple-wrapper')
        print(f"[NAUKRI] Found {len(job_cards)} job cards")
        
        for idx, card in enumerate(job_cards, 1):
            try:
                title_elem = card.find_element(By.CSS_SELECTOR, 'a.title, a.jobTuple-title')
                title = title_elem.text.strip()
                
                if not title:
                    continue
                
                try:
                    company = card.find_element(By.CSS_SELECTOR, 'a.comp-name, div.companyInfo a').text.strip()
                except:
                    company = "Unknown Company"
                
                try:
                    location = card.find_element(By.CSS_SELECTOR, 'span.location, li.location').text.strip()
                except:
                    location = "India"
                
                job_id = generate_job_id(title, company)
                
                if job_id in seen_jobs:
                    skipped_count += 1
                    continue
                
                try:
                    snippet = card.find_element(By.CSS_SELECTOR, 'div.job-description, ul.job-desc').text.strip()
                except:
                    snippet = "No description available"
                
                try:
                    experience = card.find_element(By.CSS_SELECTOR, 'span.experience, li.experience').text.strip()
                except:
                    experience = "Not specified"
                
                try:
                    salary = card.find_element(By.CSS_SELECTOR, 'span.salary, li.salary').text.strip()
                except:
                    salary = "Not disclosed"
                
                full_text = f"{title} {company} {snippet} {experience}".lower()
                found_keywords = [kw for kw in keywords if kw in full_text]
                
                posting = {
                    'job_id': job_id,
                    'job_title': title,
                    'company_name': company,
                    'location': location,
                    'job_description': snippet,
                    'experience_required': experience,
                    'salary': salary,
                    'detected_keywords': found_keywords,
                    'is_suspicious': len(found_keywords) > 0,
                    'suspicion_score': min(len(found_keywords) * 10, 100),
                    'data_source': 'naukri_scraped',
                    'scraped_at': datetime.now().isoformat(),
                    'posted_date': datetime.now().strftime('%Y-%m-%d')
                }
                
                all_postings.append(posting)
                scraped_count += 1
                producer.send(KAFKA_TOPIC, posting)
                seen_jobs.add(job_id)
                save_seen_job(job_id)
                
                flag = "[SUSPICIOUS]" if posting['is_suspicious'] else "[SAFE]"
                print(f"[NAUKRI Job {idx}] {flag} {title} @ {company}")
                
            except Exception as e:
                continue
        
        print(f"[NAUKRI] Scraped: {scraped_count} | Skipped: {skipped_count}")
        return all_postings
        
    except Exception as e:
        print(f"[NAUKRI] Error: {e}")
        return []

# ============ LINKEDIN SCRAPER ============
def scrape_linkedin_jobs(driver, url, keywords, seen_jobs):
    print(f"\n{'='*80}")
    print("[LINKEDIN] Starting scrape...")
    print(f"{'='*80}")
    
    try:
        driver.get(url)
        time.sleep(5)
        
        all_postings = []
        scraped_count = 0
        skipped_count = 0
        
        # Close modals
        try:
            close_buttons = driver.find_elements(By.CSS_SELECTOR, 'button[aria-label="Dismiss"]')
            for btn in close_buttons:
                try:
                    btn.click()
                    time.sleep(1)
                except:
                    pass
        except:
            pass
        
        # Scroll to load more
        for i in range(5):
            driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
            time.sleep(2)
        
        job_cards = driver.find_elements(By.CSS_SELECTOR, 'li.jobs-search-results__list-item, div.job-card-container')
        print(f"[LINKEDIN] Found {len(job_cards)} job cards")
        
        for idx, card in enumerate(job_cards, 1):
            try:
                title_elem = card.find_element(By.CSS_SELECTOR, 'a.job-card-list__title, h3.job-card-list__title')
                title = title_elem.text.strip()
                
                if not title:
                    continue
                
                try:
                    company = card.find_element(By.CSS_SELECTOR, 'a.job-card-container__company-name, h4.job-card-container__company-name').text.strip()
                except:
                    company = "Unknown Company"
                
                try:
                    location = card.find_element(By.CSS_SELECTOR, 'span.job-card-container__metadata-item').text.strip()
                except:
                    location = "India"
                
                job_id = generate_job_id(title, company)
                
                if job_id in seen_jobs:
                    skipped_count += 1
                    continue
                
                snippet = ""
                experience = "Not specified"
                salary = "Not disclosed"
                
                try:
                    title_elem.click()
                    time.sleep(2)
                    
                    try:
                        desc_elem = driver.find_element(By.CSS_SELECTOR, 'div.jobs-description-content__text, div.jobs-box__html-content')
                        snippet = desc_elem.text.strip()[:500]
                    except:
                        snippet = "No description available"
                except:
                    snippet = "No description available"
                
                full_text = f"{title} {company} {snippet}".lower()
                found_keywords = [kw for kw in keywords if kw in full_text]
                
                posting = {
                    'job_id': job_id,
                    'job_title': title,
                    'company_name': company,
                    'location': location,
                    'job_description': snippet,
                    'experience_required': experience,
                    'salary': salary,
                    'detected_keywords': found_keywords,
                    'is_suspicious': len(found_keywords) > 0,
                    'suspicion_score': min(len(found_keywords) * 10, 100),
                    'data_source': 'linkedin_scraped',
                    'scraped_at': datetime.now().isoformat(),
                    'posted_date': datetime.now().strftime('%Y-%m-%d')
                }
                
                all_postings.append(posting)
                scraped_count += 1
                producer.send(KAFKA_TOPIC, posting)
                seen_jobs.add(job_id)
                save_seen_job(job_id)
                
                flag = "[SUSPICIOUS]" if posting['is_suspicious'] else "[SAFE]"
                print(f"[LINKEDIN Job {idx}] {flag} {title} @ {company}")
                
            except Exception as e:
                continue
        
        print(f"[LINKEDIN] Scraped: {scraped_count} | Skipped: {skipped_count}")
        return all_postings
        
    except Exception as e:
        print(f"[LINKEDIN] Error: {e}")
        return []

# ============ MAIN EXECUTION ============
if __name__ == "__main__":
    print("[*] Starting undetected Chrome browser...")
    
    options = uc.ChromeOptions()
    options.add_argument("--disable-gpu")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    
    driver = uc.Chrome(options=options, version_main=141)
    
    try:
        seen_jobs = load_seen_jobs()
        print(f"[*] Loaded {len(seen_jobs)} previously seen jobs")
        
        print("\n" + "="*80)
        print("[*] MANUAL LOGIN REQUIRED FOR ALL PLATFORMS")
        print("="*80)
        print("You have 60 seconds total. Please:")
        print("1. First, log in to Indeed (if needed)")
        print("2. Then, log in to Naukri (if needed)")
        print("3. Finally, log in to LinkedIn (if needed)")
        print("="*80)
        
        # Open all three sites in tabs for login
        driver.get("https://in.indeed.com/")
        time.sleep(2)
        driver.execute_script("window.open('https://www.naukri.com/', '_blank');")
        time.sleep(2)
        driver.execute_script("window.open('https://www.linkedin.com/', '_blank');")
        
        print("\n[*] Waiting 60 seconds for you to log in to all platforms...")
        print("[*] Switch between tabs to log in to each site")
        for i in range(60, 0, -10):
            print(f"[*] {i} seconds remaining...")
            time.sleep(10)
        
        print("\n[+] Starting scraping from all platforms...\n")
        
        while True:
            all_jobs = []
            
            # Scrape Indeed
            indeed_jobs = scrape_indeed_jobs(driver, INDEED_SEARCH_URL, SCAM_KEYWORDS, seen_jobs)
            all_jobs.extend(indeed_jobs)
            
            # Scrape Naukri
            naukri_jobs = scrape_naukri_jobs(driver, NAUKRI_SEARCH_URL, SCAM_KEYWORDS, seen_jobs)
            all_jobs.extend(naukri_jobs)
            
            # Scrape LinkedIn
            linkedin_jobs = scrape_linkedin_jobs(driver, LINKEDIN_SEARCH_URL, SCAM_KEYWORDS, seen_jobs)
            all_jobs.extend(linkedin_jobs)
            
            # Save all to CSV
            if all_jobs:
                df = pd.DataFrame(all_jobs)
                df.to_csv(CSV_FILE, mode='a', header=not pd.io.common.file_exists(CSV_FILE), index=False, encoding='utf-8')
                print(f"\n[+] Total jobs saved: {len(all_jobs)} (Indeed: {len(indeed_jobs)}, Naukri: {len(naukri_jobs)}, LinkedIn: {len(linkedin_jobs)})")
            else:
                print("\n[i] No new jobs found in this iteration")
            
            producer.flush()
            
            print("\n" + "="*80)
            print("[*] Waiting 1 minute before next scrape cycle...")
            print("="*80)
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n\n[!] Scraper stopped by user")
    finally:
        driver.quit()
        producer.close()
        print("[+] Cleanup complete")