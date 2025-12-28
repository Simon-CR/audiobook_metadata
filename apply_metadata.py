import os
import sys
import json
import argparse
import time
from pathlib import Path
from google import genai
from google.genai import types

# Supported audio extensions for audiobooks
AUDIO_EXTENSIONS = {'.m4b', '.mp3', '.m4a', '.flac', '.ogg'}

def get_client():
    api_key = os.environ.get("GEMINI_API_KEY")
    if not api_key:
        print("Error: GEMINI_API_KEY environment variable not set.")
        print("Please export GEMINI_API_KEY='your_api_key'")
        sys.exit(1)
    return genai.Client(api_key=api_key)

def generate_metadata_prompt(filename, folder_name):
    return f"""
    I have an audiobook file. Please generate the metadata for it in JSON format compatible with Audiobookshelf.
    
    Filename: "{filename}"
    Folder: "{folder_name}"
    
    The JSON should strictly follow this schema:
    {{
      "title": "String",
      "authors": ["List", "of", "Authors"],
      "narrators": ["List", "of", "Narrators"],
      "description": "A brief description of the book",
      "publisher": "String",
      "publishedYear": "String (YYYY)",
      "series": [
        {{
          "series": "Series Name",
          "sequence": "Sequence Number (e.g. '1' or '1.5')"
        }}
      ],
      "genres": ["List", "of", "Genres"],
      "language": "en"
    }}
    
    If you cannot determine a field, leave it as null or an empty list. 
    Infer the Series and Sequence from the title/filename if possible.
    Output ONLY the JSON.
    """

def process_file(client, file_path, dry_run=False):
    path = Path(file_path)
    directory = path.parent
    metadata_path = directory / "metadata.json"
    
    if metadata_path.exists() and not dry_run:
        print(f"Skipping (metadata exists): {path.name}")
        return False

    print(f"Processing: {path.name}...")
    
    try:
        response = client.models.generate_content(
            model='gemini-2.0-flash-exp',
            contents=generate_metadata_prompt(path.name, directory.name),
            config=types.GenerateContentConfig(
                response_mime_type='application/json'
            )
        )
        
        metadata = response.parsed
        
        if not metadata:
             print(f"  Failed to parse JSON response for {path.name}")
             return False

        if dry_run:
            print(f"  [DRY RUN] Generated metadata for {path.name}:")
            print(json.dumps(metadata, indent=2))
        else:
            with open(metadata_path, 'w', encoding='utf-8') as f:
                json.dump(metadata, f, indent=2, ensure_ascii=False)
            print(f"  Saved metadata.json")
            
        # polite rate limiting
        time.sleep(1) 
        return True

    except Exception as e:
        print(f"  Error processing {path.name}: {e}")
        return False

def main():
    parser = argparse.ArgumentParser(description="Generate Audiobookshelf metadata using Gemini API.")
    parser.add_argument("directory", nargs="?", default=".", help="Root directory to scan (default: current dir)")
    parser.add_argument("--dry-run", action="store_true", help="Print metadata to console instead of writing files")
    parser.add_argument("--limit", type=int, default=0, help="Limit the number of books to process (0 for no limit)")
    
    args = parser.parse_args()
    
    root_dir = Path(args.directory).resolve()
    print(f"Scanning directory: {root_dir}")
    
    if args.dry_run:
        print("Running in DRY RUN mode. No files will be modified.")

    if args.dry_run:
        print("Running in DRY RUN mode. No files will be modified.")
    if args.limit > 0:
        print(f"Limit set to {args.limit} books.")

    client = get_client()
    
    processed_count = 0
    
    # Walk through the directory
    for root, dirs, files in os.walk(root_dir):
        # We only want to process one metadata file per folder if it contains audiobooks.
        # Usually audiobooks are 1 book per folder (multi-file) or single file.
        # Strategy: Look for audio files. If found, generate metadata for the BOOK.
        # Audiobookshelf expects metadata.json to be for the *book*, which might be the folder or a specific file.
        # If it's a multi-file book (Chapter 1.mp3, Chapter 2.mp3), we should use the folder name or the common prefix.
        # Simpler approach for this task: process the *first* audio file found in a directory to determine book metadata,
        # assuming the directory represents the book.
        
        audio_files = [f for f in files if Path(f).suffix.lower() in AUDIO_EXTENSIONS]
        
        if not audio_files:
            continue
            
        # Heuristic: If we are in a directory with audio files, we generate ONE metadata.json for the directory.
        # We use the first audio file and the folder name as context.
        # Exception: If all files are loose in the root, this might be messy, but standard audiobook structure is Author/Book/files.
        
        first_audio = audio_files[0]
        full_path = Path(root) / first_audio
        
        if process_file(client, full_path, dry_run=args.dry_run):
            processed_count += 1
            if args.limit > 0 and processed_count >= args.limit:
                print(f"Limit of {args.limit} books reached. Stopping.")
                break

if __name__ == "__main__":
    main()
