import os
import sys
import json
import argparse
import subprocess
import shutil
import time
import datetime
import concurrent.futures
import requests
from pathlib import Path

# Supported audio extensions for audiobooks
# Valid audio extensions
AUDIO_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.aac', '.ogg', '.wav'}

# Log Directory Setup
SCRIPT_DIR = Path(__file__).parent.resolve()
LOG_DIR = SCRIPT_DIR / "logs"
LOG_DIR.mkdir(exist_ok=True)

def check_dependencies():
    """Checks if 'gemini' CLI is installed."""
    if not shutil.which("gemini"):
        print("Error: 'gemini' CLI not found in PATH.")
        print("Please install the Gemini CLI and ensure it is in your PATH.")
        print("Run 'gemini \"test\"' to verify it works.")
        sys.exit(1)

def generate_metadata_prompt(filename, folder_name):
    return f"""
    1. Identify the language of the audiobook based on the filename: "{filename}" and folder: "{folder_name}".
    2. If the book appears to be French, search **Audible.fr** or **Audible.ca**.
    3. Otherwise, search **Audible.com** or **Audible.ca**.
    4. If the book is not found on Audible, fallback to using general Google Search data or your internal knowledge to find the correct metadata.
    
    Extract the metadata and provide it in this specific JSON format for Audiobookshelf. 
    Ensure the description and narrators are accurate.

    JSON Schema:
    {{
      "title": "String",
      "authors": ["List", "of", "Authors"],
      "narrators": ["List", "of", "Narrators"],
      "description": "Full description from Audible",
      "publisher": "String",
      "publishedYear": "String (YYYY)",
      "series": [
        {{
          "series": "Series Name",
          "sequence": "Sequence Number (e.g. '1')"
        }}
      ],
      "genres": ["List", "of", "Genres"],
      "language": "en",
      "confidence": 0.0,
      "confidence_reason": "String"
    }}
    
    IMPORTANT:
    - If you cannot find a specific match for this EXACT book/author, set "confidence" to 0.1 and provide the reason.
    - If you are guessing based on the filename only, set "confidence" to 0.4.
    - Do NOT make up data.
    
    Output ONLY the JSON block. Do not include markdown formatting like ```json ... ``` if possible, or I will filter it out.
    """

def call_gemini_cli(prompt, model=None):
    """Calls Gemini CLI with the prompt and returns the output text."""
    try:
        # We use the CLI via subprocess
        cmd = ["gemini", "prompt", prompt]
        if model and model.lower() != "default":
            cmd.extend(["--model", model])
            
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            encoding='utf-8',
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"Error calling Gemini CLI: {e.stderr}")
        return None
    except Exception as e:
        print(f"  Subprocess Error: {e}")
        return None

def extract_json(text):
    """Extracts JSON object from a potentially chatty response."""
    if not text:
        return None
        
    # strip markdown code blocks if present
    text = text.strip()
    if text.startswith("```json"):
        text = text[7:]
    if text.startswith("```"):
        text = text[3:]
    if text.endswith("```"):
        text = text[:-3]
        
    text = text.strip()
    
    # Try to find the first { and last }
    start = text.find('{')
    end = text.rfind('}')
    
    if start != -1 and end != -1:
        json_str = text[start:end+1]
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            return None
    return None

def fetch_abs_library_map(url, token):
    """Fetches all library items and maps absolute folder path to Item ID."""
    print(f"Connecting to Audiobookshelf at {url}...")
    headers = {"Authorization": f"Bearer {token}"}
    mapping = {}
    
    try:
        # 1. Get libraries
        libs_resp = requests.get(f"{url}/api/libraries", headers=headers)
        libs_resp.raise_for_status()
        libraries = libs_resp.json().get('libraries', [])
        
        for lib in libraries:
            lib_id = lib['id']
            # 2. Get items for library (simplified, might need pagination for huge libs but usually one big fetch works)
            items_resp = requests.get(f"{url}/api/libraries/{lib_id}/items", headers=headers)
            items_resp.raise_for_status()
            items = items_resp.json().get('results', [])
            
            for item in items:
                # ABS stores path. We need to normalize it to match local script usage.
                # Assuming script runs on same filesystem as ABS or mounted same way.
                # item['path'] is the folder path
                if 'path' in item:
                    # Normalize: resolve symlinks/absolute
                    # ABS server path might differ from local mount. 
                    # Use Basename as Key for robust matching
                    abs_path_obj = Path(item['path'])
                    folder_name = abs_path_obj.name
                    mapping[folder_name] = item 
                    
        print(f"Mapped {len(mapping)} Audiobookshelf items.")
        if mapping:
            sample_key = next(iter(mapping))
            print(f"DEBUG: Sample ABS path mapping (basename): '{sample_key}'")
        return mapping
    except Exception as e:
        print(f"Error fetching ABS library: {e}")
        return {}

def trigger_abs_scan(url, token, item_id):
    """Triggers a scan for a specific item ID."""
    try:
        requests.post(f"{url}/api/items/{item_id}/scan", headers={"Authorization": f"Bearer {token}"}, timeout=5)
        # print(f"  [API] Triggered scan for item {item_id}")
        return True
    except Exception as e:
        print(f"  [API] Failed to trigger scan: {e}")
        return False

def process_file(file_path, dry_run=False, abs_config=None, model=None, force=False):
    # abs_config is a dict: {'url': str, 'token': str, 'map': dict}
    path = Path(file_path)
    directory = path.parent
    
    # Debug info for ABS mapping
    if abs_config:
        dir_abs = str(directory.resolve())
        # print(f"DEBUG: Checking map for '{dir_abs}'")
    metadata_path = directory / "metadata.json"
    
    if metadata_path.exists() and not dry_run and not force:
        print(f"Skipping (metadata exists): {path.name}")
        return False

    print(f"Processing: {path.name}...")
    
    prompt = generate_metadata_prompt(path.name, directory.name)
    
    start_time = time.time()
    raw_output = call_gemini_cli(prompt, model=model)
    duration = time.time() - start_time
    
    # We will log to specific files based on outcome later.
    print(f"  Gemini response received in {duration:.2f}s")
    
    metadata = extract_json(raw_output)
    
    if not metadata:
        print(f"  Failed to extract valid JSON from Gemini output for {path.name}")
        try:
             with open(LOG_DIR / "other_errors.log", "a", encoding="utf-8") as err_log:
                err_log.write(f"{datetime.datetime.now()} | {path.name} | JSON Extraction Failed | Raw output length: {len(raw_output)}\n")
        except: pass
        if raw_output:
            print(f"  Raw Output Preview: {raw_output[:200]}...")
        return False
        
    # Check Confidence
    confidence = metadata.get('confidence', 0.5) # Default to 0.5 if missing (legacy/fallback)
    confidence = float(confidence)
    reason = metadata.get('confidence_reason', 'No reason provided')
    
    print(f"  Confidence: {confidence} | Reason: {reason}")
    
    if confidence < 0.60:
        print(f"  [SKIP] Confidence too low ({confidence}). Reason: {reason}")
        try:
             with open(LOG_DIR / "failed_to_match.log", "a", encoding="utf-8") as skip_log:
                skip_log.write(f"{datetime.datetime.now()} | {path.name} | Confidence: {confidence} | Reason: {reason}\n")
        except: pass
        return False

    if dry_run:
        print(f"  [DRY RUN] Generated metadata for {path.name}:")
        print(json.dumps(metadata, indent=2, ensure_ascii=False))
        # Log dry run success to processed log
        try:
            with open(LOG_DIR / "processed.log", "a", encoding="utf-8") as proc_log:
                proc_log.write(f"{datetime.datetime.now()} | {path.name} | DRY RUN | Confidence: {confidence}\n")
        except: pass
    else:
        # Log success
        try:
            with open(LOG_DIR / "processed.log", "a", encoding="utf-8") as proc_log:
                proc_log.write(f"{datetime.datetime.now()} | {path.name} | SAVED | Confidence: {confidence}\n")
        except: pass
        
        # Remove confidence fields before saving to file (not standard ABS format)
        meta_to_save = metadata.copy()
        meta_to_save.pop('confidence', None)
        meta_to_save.pop('confidence_reason', None)
        
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(meta_to_save, f, indent=2, ensure_ascii=False)
        print(f"  Saved metadata.json")
        
        # Trigger ABS Scan if configured
        if abs_config:
            folder_name = directory.name
            if folder_name in abs_config['map']:
                item = abs_config['map'][folder_name]
                item_id = item['id']
                
                # Check current state
                current_meta = item.get('media', {}).get('metadata', {})
                cur_title = current_meta.get('title', '[Unknown]')
                cur_author = current_meta.get('author', '[Unknown]')
                
                new_title = metadata.get('title', '[Unknown]')
                # Assuming authors is a list, join them for comparison
                new_auth_list = metadata.get('authors', [])
                new_author = ", ".join(new_auth_list) if isinstance(new_auth_list, list) else str(new_auth_list)
                
                print(f"  [ABS Comparison] ID: {item_id}")
                
                comparison_msg = (
                    f"--- COMPARISON {datetime.datetime.now()} ---\n"
                    f"Book: {folder_name}\n"
                    f"Current ABS: Title='{cur_title}' | Author='{cur_author}'\n"
                    f"New Gemini:  Title='{new_title}' | Author='{new_author}'\n"
                    f"Confidence: {confidence}\n"
                    f"------------------------------------------\n"
                )
                
                # Write to generic comparison report
                try:
                    with open(LOG_DIR / "comparison_report.txt", "a", encoding="utf-8") as rep:
                        rep.write(comparison_msg)
                except Exception as e:
                    print(f"Warning: Failed to write comparison report: {e}")

                trigger_abs_scan(abs_config['url'], abs_config['token'], item_id)
                print("  [API] Scan triggered")
            else:
                print(f"  [API Warning] Could not find folder '{folder_name}' in ABS library map.")
        
    return True

def main():
    parser = argparse.ArgumentParser(description="Generate Audiobookshelf metadata using Gemini CLI.")
    parser.add_argument("directory", nargs="?", default=".", help="Root directory to scan (default: current dir)")
    parser.add_argument("--dry-run", action="store_true", help="Print metadata to console instead of writing files")
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of books to process (0 for no limit)")
    parser.add_argument("--abs-url", help="Audiobookshelf URL (e.g. http://localhost:13378)")
    parser.add_argument("--abs-token", help="Audiobookshelf API Token")
    parser.add_argument("--model", default="default", help="Gemini model to use (default: CLI default)")
    parser.add_argument("--force", action="store_true", help="Overwrite existing metadata.json files")
    
    args = parser.parse_args()
    
    check_dependencies()
    
    root_dir = Path(args.directory).resolve()
    print(f"Scanning directory: {root_dir}")
    
    abs_config = None
    if args.abs_url and args.abs_token:
        # Normalize URL (remove trailing slash)
        clean_url = args.abs_url.rstrip('/')
        item_map = fetch_abs_library_map(clean_url, args.abs_token)
        abs_config = {
            'url': clean_url, 
            'token': args.abs_token,
            'map': item_map
        }
    
    if args.dry_run:
        print("Running in DRY RUN mode. No files will be modified.")
    if args.limit > 0:
        print(f"Limit set to {args.limit} books.")

    items_checked = 0
    
    # Collect all audio files first
    tasks = []
    for root, dirs, files in os.walk(root_dir):
        audio_files = [f for f in files if Path(f).suffix.lower() in AUDIO_EXTENSIONS]
        
        if not audio_files:
            continue
            
        items_checked += 1
        
        # Determine if this folder contains a single book (split into parts) or multiple different books
        if len(audio_files) > 1:
            common_prefix = os.path.commonprefix(audio_files)
            
            # Heuristic: 
            # 1. Common prefix > 3 chars
            # 2. Files appear to be tracks (numeric start, "Track", etc.)
            # 3. Folder name is in filenames
            
            is_single_book = False
            if len(common_prefix) > 3:
                is_single_book = True
            elif all(f[0].isdigit() for f in audio_files): 
                 is_single_book = True
            elif all(f.lower().startswith('track') for f in audio_files):
                 is_single_book = True
            elif all(Path(root).name.lower() in f.lower() for f in audio_files):
                 is_single_book = True
                
            if not is_single_book:
                msg = f"Skipping (mixed content?): {Path(root).name} contains {len(audio_files)} files with no common prefix."
                print(msg)
                try:
                     with open(LOG_DIR / "skipped_mixed_content.log", "a", encoding="utf-8") as log_file:
                        log_file.write(f"--- {datetime.datetime.now()} | {Path(root).name} | SKIPPED (Mixed content) ---\n")
                except: pass
                
                # Check limit including skipped items
                if args.limit > 0 and items_checked >= args.limit:
                    print(f"Limit of {args.limit} books checked (processed or skipped). Stopping scan.")
                    break
                continue
        
        first_audio = audio_files[0]
        full_path = Path(root) / first_audio
        tasks.append(full_path)
        
        if args.limit > 0 and items_checked >= args.limit:
             print(f"Limit of {args.limit} books checked (processed or skipped). Stopping scan.")
             break
        
    print(f"Found {len(tasks)} valid books to process.")
    
    # Process in parallel
    if tasks:
        processed_final = 0
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
            # Submit all tasks
            future_to_file = {executor.submit(process_file, f, args.dry_run, abs_config, args.model, args.force): f for f in tasks}
            
            for future in concurrent.futures.as_completed(future_to_file):
                f = future_to_file[future]
                try:
                    success = future.result()
                    if success:
                        processed_final += 1
                except Exception as exc:
                    print(f"Generated an exception for {f}: {exc}")

if __name__ == "__main__":
    main()
