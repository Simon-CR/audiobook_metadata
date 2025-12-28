import os
import sys
import json
import argparse
import subprocess
import shutil
import time
import datetime
import concurrent.futures
from pathlib import Path

# Supported audio extensions for audiobooks
AUDIO_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg', '.wav', '.wma', '.aac'}

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
      "language": "en"
    }}
    
    Output ONLY the JSON block. Do not include markdown formatting like ```json ... ``` if possible, or I will filter it out.
    """

def call_gemini_cli(prompt):
    """Calls the gemini CLI with the given prompt and returns the stdout."""
    try:
        # We use strict mode or just simple prompt. 
        # Quoting the prompt is handled by subprocess list args
        # Note: If the CLI requires specific flags for better output, add them here.
        result = subprocess.run(
            ["gemini", prompt],
            capture_output=True,
            text=True,
            check=True
        )
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"  Gemini CLI Error: {e.stderr}")
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

def process_file(file_path, dry_run=False):
    path = Path(file_path)
    directory = path.parent
    metadata_path = directory / "metadata.json"
    
    if metadata_path.exists() and not dry_run:
        print(f"Skipping (metadata exists): {path.name}")
        return False

    print(f"Processing: {path.name}...")
    
    prompt = generate_metadata_prompt(path.name, directory.name)
    
    start_time = time.time()
    raw_output = call_gemini_cli(prompt)
    duration = time.time() - start_time
    
    # Log the interaction
    log_entry = f"--- {datetime.datetime.now()} | {path.name} | took {duration:.2f}s ---\nPrompt: ...\nOutput:\n{raw_output}\n\n"
    try:
        with open("processing.log", "a", encoding="utf-8") as log_file:
            log_file.write(log_entry)
    except Exception as e:
        print(f"  Warning: Failed to write log: {e}")

    print(f"  Gemini response received in {duration:.2f}s")
    
    metadata = extract_json(raw_output)
    
    if not metadata:
        print(f"  Failed to extract valid JSON from Gemini output for {path.name}")
        if raw_output:
            print(f"  Raw Output Preview: {raw_output[:200]}...")
        return False

    if dry_run:
        print(f"  [DRY RUN] Generated metadata for {path.name}:")
        print(json.dumps(metadata, indent=2, ensure_ascii=False))
    else:
        with open(metadata_path, 'w', encoding='utf-8') as f:
            json.dump(metadata, f, indent=2, ensure_ascii=False)
        print(f"  Saved metadata.json")
        
    return True

def main():
    parser = argparse.ArgumentParser(description="Generate Audiobookshelf metadata using Gemini CLI.")
    parser.add_argument("directory", nargs="?", default=".", help="Root directory to scan (default: current dir)")
    parser.add_argument("--dry-run", action="store_true", help="Print metadata to console instead of writing files")
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of books to process (0 for no limit)")
    
    args = parser.parse_args()
    
    check_dependencies()
    
    root_dir = Path(args.directory).resolve()
    print(f"Scanning directory: {root_dir}")
    
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
                     with open("processing.log", "a", encoding="utf-8") as log_file:
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
            future_to_file = {executor.submit(process_file, f, args.dry_run): f for f in tasks}
            
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
