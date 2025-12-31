import os
import bz2
import mwxml
import mwparserfromhell
import re
import json
import zstandard as zstd
from pathlib import Path
from multiprocessing import Pool, cpu_count

# CONFIGURATION
DUMP_PATH = "enwiki-latest-pages-articles.xml.bz2"
# OUTPUT_DIR = Path("./wikipedia1_articles_json")
OUTPUT_DIR = Path("./wikipedia_zstd_batches")
# MAX_ARTICLES = None
BATCH_SIZE = 500
NUM_WORKERS = max(1, cpu_count() - 1)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def sanitize_filename(title):
    filename = re.sub(r'[\\/*?:"<>|]', "_", title)
    filename = re.sub(r'\s+', '_', filename)
    return filename[:200]

def extract_categories(parsed_wikitext):
    categories = []
    for link in parsed_wikitext.filter_wikilinks():
        print(link)
        target = str(link.title).strip()
        if target.startswith("Category:"):
            categories.append(target[len("Category:"):])
    return categories

def clean_and_write_article(data):
    """Worker: clean text, extract categories, write JSON."""
    page_id, title, raw_text = data
    try:
        parsed = mwparserfromhell.parse(raw_text)
        # parsed.filter_templates
        # clean_text = parsed.strip_code().strip()

            # Remove sections by heading
        sections_to_remove = [
            'See also', 'Further reading', 'Notes', 
            'References', 'External links', 'Bibliography'
        ]
        
        for section in parsed.get_sections(include_headings=True):
            for heading in section.filter_headings():
                heading_text = heading.title.strip()
                if heading_text in sections_to_remove:
                    # Remove this entire section
                    parsed.remove(section)
                    break
        
        # Remove category links
        for wikilink in parsed.filter_wikilinks():
            if str(wikilink.title).startswith("Category:"):
                parsed.remove(wikilink)
        
        clean_text = str(parsed.strip_code()).strip()
        # print(clean_text)
         # Remove escaped quotes
        # Remove \n characters
        # 
        clean_text = clean_text.replace('\n', ' ')
        clean_text = clean_text.replace('"', '')

        if not clean_text:
            return None
        article_url = f"https://en.wikipedia.org/wiki?curid={page_id}"

        article = {
            "id": page_id,
            "title": title,
            "text": clean_text,
            "url": article_url
        }
        # print(article_url)
        # filename = sanitize_filename(title) + ".json"
        # filepath = OUTPUT_DIR / filename

        # with open(filepath, "w", encoding="utf-8") as f:
        #     json.dump(article, f, ensure_ascii=False, indent=2)

        # return True
        return article
    except Exception as e:
        return None

def save_zstd_batch(batch_data, batch_number):
    """Saves a batch of articles into a .zstd file."""
    file_path = OUTPUT_DIR / f"wikipedia_batch_{batch_number:04d}.jsonl.zst"
    
    # Prepare the JSON Lines data
    # jsonl_content = "\n".join(json.dumps(art, ensure_ascii=False) for art in batch_data)
    jsonl_content = "\n".join(
        json.dumps(art, ensure_ascii=False, separators=(',', ':')) 
        for art in batch_data
    )
    binary_data = jsonl_content.encode('utf-8')
    
    params = zstd.ZstdCompressionParameters.from_level(
        3, 
        enable_ldm=True, 
        threads=-1 
    )

    # 3. Initialize Compressor with params and multi-threading
    cctx = zstd.ZstdCompressor(compression_params=params)
    with open(file_path, "wb") as f:
        f.write(cctx.compress(binary_data))
        
def main():
    pool = Pool(NUM_WORKERS)
    current_batch = []
    batch_count = 0

    with bz2.open(DUMP_PATH, "rb") as f:
        dump = mwxml.Dump.from_file(f)

        def article_generator():
            for page in dump:

                if page.namespace != 0 or page.redirect is not None:
                    continue
                rev = None
                for r in page:
                    rev = r  # keep the last revision
                if rev is None or not rev.text:
                    continue
                yield (page.id, page.title, rev.text)

        for result in pool.imap_unordered(clean_and_write_article, article_generator(), chunksize=10):
            if result:
                current_batch.append(result)

                if len(current_batch) >= BATCH_SIZE:
                    save_zstd_batch(current_batch, batch_count)
                    print(f"Compressed batch {batch_count} (500 articles) to .zstd")
                    batch_count += 1
                    current_batch = []
        
        if current_batch:
            save_zstd_batch(current_batch, batch_count)

    pool.close()
    pool.join()

if __name__ == "__main__":
    main()