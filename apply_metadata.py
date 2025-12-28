import os
import sys
import json
import argparse
import subprocess
import shutil
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
    Search Audible.com for the audiobook with this filename and folder name.
    
    Filename: "{filename}"
    Folder: "{folder_name}"
    
    Extract the metadata and provide it in this specific JSON format for Audiobookshelf. 
    Ensure the description and narrators are accurate from the Audible listing.

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
    raw_output = call_gemini_cli(prompt)
    
    metadata = extract_json(raw_output)
    
    if not metadata:
        print(f"  Failed to extract valid JSON from Gemini output for {path.name}")
        if raw_output:
            print(f"  Raw Output Preview: {raw_output[:200]}...")
        return False

    if dry_run:
        print(f"  [DRY RUN] Generated metadata for {path.name}:")
        print(json.dumps(metadata, indent=2))
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

    processed_count = 0
    
    # Walk through the directory
    for root, dirs, files in os.walk(root_dir):
        audio_files = [f for f in files if Path(f).suffix.lower() in AUDIO_EXTENSIONS]
        
        if not audio_files:
            continue
            
        first_audio = audio_files[0]
        full_path = Path(root) / first_audio
        
        if process_file(full_path, dry_run=args.dry_run):
            processed_count += 1
            if args.limit > 0 and processed_count >= args.limit:
                print(f"Limit of {args.limit} books reached. Stopping.")
                break

if __name__ == "__main__":
    main()
