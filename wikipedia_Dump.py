!pip install wikipedia --quiet

import wikipedia
import re
import threading
import time
import os
from datetime import datetime
from google.colab import drive
from wikipedia.exceptions import RedirectError, DisambiguationError, PageError, WikipediaException

# Mount Google Drive
drive.mount('/content/drive', force_remount=True)

# === OPTIMIZED CONFIGURATION ===
NUM_ARTICLES = 800000000000 # Increased target for Marathi Wikipedia
NUM_WORKER_THREADS = 8  # Safe for Colab and Wikipedia API
FILE_SIZE_PRINT_INTERVAL = 60  # seconds
BATCH_SIZE = 20  # Max supported by Wikipedia API
MAX_RETRIES = 4  # For API error recovery
RETRY_DELAY = 10  # Seconds between retries
CONSECUTIVE_EMPTY_THRESHOLD = 100  # Stop when no new articles found after this many batches

# File paths
DRIVE_FILE_PATH = "/content/drive/MyDrive/marathi_clean_corpus.txt"
SEEN_TITLES_FILE = "/content/drive/MyDrive/marathi_seen_titles.txt"

# Ensure directories exist
os.makedirs(os.path.dirname(DRIVE_FILE_PATH), exist_ok=True)
os.makedirs(os.path.dirname(SEEN_TITLES_FILE), exist_ok=True)

# Initialize Marathi Wikipedia with proper user agent
wikipedia.set_lang("mr")
wikipedia.set_user_agent("MarathiCorpusBot/1.0 (https://example.com; your_email@example.com)")

# Global state with synchronization
seen_titles = set()
total_articles_written = 0
consecutive_empty_batches = 0  # Track batches with no new articles
lock = threading.Lock()

# Load existing seen titles
if os.path.exists(SEEN_TITLES_FILE):
    with open(SEEN_TITLES_FILE, 'r', encoding='utf-8') as f:
        for line in f:
            title = line.strip()
            if title:  # Skip empty lines
                seen_titles.add(title)
    
    # Filter out invalid titles
    print(f"Preloaded {len(seen_titles)} titles from disk")
    print("Filtering invalid titles...")
    initial_count = len(seen_titles)
    
    # Patterns indicating non-article pages
    invalid_patterns = ["(", ")", "विकिपीडिया", "disambiguation", "मदत", "साचा", "चर्चा"]
    filtered_titles = [t for t in seen_titles if not any(patt in t for patt in invalid_patterns)]
    
    seen_titles = set(filtered_titles)
    total_articles_written = len(seen_titles)
    print(f"Filtered {initial_count - total_articles_written} invalid titles")
    print(f"Valid articles: {total_articles_written}")
else:
    print("No seen titles file. Starting fresh.")

# === CLEANING FUNCTION ===
def clean_text(text):
    text = re.sub(r'[^\u0900-\u097F\s।,!?०१२३४५६७८९]+', ' ', text)
    text = re.sub(r'\s+', ' ', text)
    return text.strip()

# === WORKER FUNCTION WITH RETRY LOGIC ===
def worker(thread_id):
    global total_articles_written, consecutive_empty_batches
    
    while True:
        # Check stopping conditions
        with lock:
            if total_articles_written >= NUM_ARTICLES:
                print(f"[Thread-{thread_id}] 🛑 Stopping (reached target)", flush=True)
                return
            if consecutive_empty_batches >= CONSECUTIVE_EMPTY_THRESHOLD:
                print(f"[Thread-{thread_id}] 🛑 Stopping (no new articles)", flush=True)
                return
        
        # Fetch batch with retry mechanism
        titles_batch = []
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                titles_batch = wikipedia.random(pages=BATCH_SIZE)
                break  # Exit loop on success
            except Exception as e:
                error_msg = str(e)
                if "HTTP" in error_msg or "timed out" in error_msg:
                    print(f"[Thread-{thread_id}] ⚠️ API error (attempt {attempt}/{MAX_RETRIES}): {error_msg}", flush=True)
                    time.sleep(RETRY_DELAY * attempt)
                else:
                    print(f"[Thread-{thread_id}] ❌ Critical error: {error_msg}", flush=True)
                    break
        else:
            print(f"[Thread-{thread_id}] ❌ Failed after {MAX_RETRIES} attempts. Skipping batch.", flush=True)
            time.sleep(30)  # Longer pause before next try
            continue
        
        # Process new titles
        new_titles = []
        with lock:
            for title in titles_batch:
                if title not in seen_titles:
                    seen_titles.add(title)
                    new_titles.append(title)
            
            if not new_titles:
                consecutive_empty_batches += 1
                print(f"[Thread-{thread_id}] ⏩ No new titles in batch | Consecutive empty: {consecutive_empty_batches}/{CONSECUTIVE_EMPTY_THRESHOLD}", flush=True)
                continue
            else:
                consecutive_empty_batches = 0  # Reset counter
                
            # Persist new titles immediately
            try:
                with open(SEEN_TITLES_FILE, "a", encoding="utf-8") as f_seen:
                    for title in new_titles:
                        f_seen.write(title + "\n")
                    f_seen.flush()
                print(f"[Thread-{thread_id}] 💾 Saved {len(new_titles)} new titles to disk", flush=True)
            except Exception as e:
                print(f"[Thread-{thread_id}] ❌ Seen file error: {str(e)}", flush=True)
        
        # Process each article in batch
        articles_written_in_batch = 0
        for title in new_titles:
            with lock:
                if total_articles_written >= NUM_ARTICLES:
                    break
            
            try:
                # Fetch article content with error handling
                page = wikipedia.page(title, auto_suggest=False, redirect=False)
                content = clean_text(page.content)
                word_count = len(content.split())
                
                # Skip short articles
                if word_count <= 50:
                    print(f"[Thread-{thread_id}] ⚠️ Skipped short: {title} ({word_count} words)", flush=True)
                    continue
                
                # Write to main file
                with open(DRIVE_FILE_PATH, "a", encoding="utf-8") as f_out:
                    f_out.write(content + "\n")
                    f_out.flush()
                
                # Update count
                with lock:
                    total_articles_written += 1
                articles_written_in_batch += 1
                print(f"[Thread-{thread_id}] ✅ Written: {title} ({word_count} words) [Total: {total_articles_written}]", flush=True)
                
            except (RedirectError, DisambiguationError, PageError) as e:
                print(f"[Thread-{thread_id}] ⚠️ Skipped special: {title} ({type(e).__name__})", flush=True)
            except WikipediaException as e:
                print(f"[Thread-{thread_id}] ❌ Wikipedia error: {title} - {str(e)}", flush=True)
            except Exception as e:
                print(f"[Thread-{thread_id}] ❌ Unexpected error: {title} - {str(e)}", flush=True)
        
        # Update empty batch counter if no articles were written
        if articles_written_in_batch == 0:
            with lock:
                consecutive_empty_batches += 1
                print(f"[Thread-{thread_id}] ⚠️ Batch had no valid articles | Consecutive empty: {consecutive_empty_batches}/{CONSECUTIVE_EMPTY_THRESHOLD}", flush=True)

# === MONITOR FUNCTION ===
def monitor():
    last_size = 0
    last_count = 0
    start_time = time.time()
    
    while True:
        with lock:
            if total_articles_written >= NUM_ARTICLES:
                print("🛑 Monitoring stopped (target reached)", flush=True)
                return
            if consecutive_empty_batches >= CONSECUTIVE_EMPTY_THRESHOLD:
                print("🛑 Monitoring stopped (no new articles)", flush=True)
                return
            current_count = total_articles_written
        
        try:
            current_size = os.path.getsize(DRIVE_FILE_PATH) / (1024 ** 2)  # MB
            size_growth = current_size - last_size
            count_growth = current_count - last_count
            elapsed_hours = (time.time() - start_time) / 3600
            
            # Calculate collection rate
            if elapsed_hours > 0:
                articles_per_hour = current_count / elapsed_hours
            else:
                articles_per_hour = 0
            
            last_size = current_size
            last_count = current_count
            
            print(f"[{datetime.now().strftime('%H:%M:%S')}] 📊 File: {current_size:.2f} MB | " +
                  f"Articles: {current_count}/{NUM_ARTICLES} | " +
                  f"Rate: {articles_per_hour:.1f}/hour | " +
                  f"Growth: +{count_growth} articles, +{size_growth:.2f} MB | " +
                  f"Empty batches: {consecutive_empty_batches}/{CONSECUTIVE_EMPTY_THRESHOLD}", flush=True)
        except Exception as e:
            print(f"❌ Monitor error: {str(e)}", flush=True)
        
        time.sleep(FILE_SIZE_PRINT_INTERVAL)

# === MAIN EXECUTION ===
# Write session header
with open(DRIVE_FILE_PATH, "a", encoding="utf-8") as f:
    f.write(f"\n\n# === New session started at {datetime.now().isoformat()} ===\n")
    f.write(f"# Target articles: {NUM_ARTICLES}\n")
    f.write(f"# Existing valid articles: {total_articles_written}\n")
    f.flush()

print(f"🚀 Starting collection with {NUM_WORKER_THREADS} threads")
print(f"📈 Target: {NUM_ARTICLES} articles | Current: {total_articles_written} valid articles")

# Start monitoring thread
monitor_thread = threading.Thread(target=monitor, daemon=True)
monitor_thread.start()

# Start worker threads
threads = []
for i in range(NUM_WORKER_THREADS):
    t = threading.Thread(target=worker, args=(i,))
    t.start()
    threads.append(t)
    print(f"🚀 Started worker thread {i}", flush=True)

# Wait for completion
for t in threads:
    t.join()

# Final report
final_size = os.path.getsize(DRIVE_FILE_PATH)/(1024**2)
print(f"\n{'='*50}")
print(f"✅ Completed! Total articles written: {total_articles_written}")
print(f"💾 Main output: {DRIVE_FILE_PATH} ({final_size:.2f} MB)")
print(f"📝 Seen titles: {SEEN_TITLES_FILE}")
print(f"📊 Final status: {'Target reached' if total_articles_written >= NUM_ARTICLES else 'No new articles found'}")
print(f"{'='*50}")
