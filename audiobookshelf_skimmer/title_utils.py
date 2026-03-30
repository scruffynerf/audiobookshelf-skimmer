import re
from typing import Optional
from .author_utils import normalize_author

def normalize_title(text: str, author_name: Optional[str] = None) -> str:
    """Advanced title normalization:
    1. Remove content in brackets/parentheses
    2. Replace & with 'and'
    3. Remove versioning (v2, v3, etc)
    4. Remove encoding and format labels
    5. Strip author name if provided
    6. Strip to pure alpha (a-z)
    """
    if not text:
        return ""
    
    t = str(text).lower()
    
    # 1. Handlers for symbols, HTML entities, and versioning
    t = t.replace("&", " and ")
    t = re.sub(r'\bamp\b', ' and ', t)
    t = t.replace("quot", " ")
    
    # Strip trailing version suffixes like " v2", " v3", "v 2"
    t = re.sub(r'\s+v\s*\d+\s*$', ' ', t)
    
    # 2. Remove anything in (), [], {}
    t = re.sub(r'[\(\[\{].*?[\)\]\}]', ' ', t)
    
    # 3. Remove encoding (64k, 128kbps, etc) and common format/media terms
    t = re.sub(r'\b\d{2,3}\s*k(b|bps)?\b', ' ', t)
    
    # Common format/media terms to ignore
    terms = [
        'mp3', 'm4b', 'm4a', 'flac', 'aac', 'unabridged', 'dramatized', 
        'adaptation', 'boxed set', 'collection', 'part', 'book', 'vol', 
        'volume', 'edition', 'annotated', 'sessions', 'cd', 'audio', 'read by'
    ]
    term_pattern = r'\b(' + '|'.join(terms) + r')\b'
    t = re.sub(term_pattern, ' ', t)
    
    # 4. If author name provided, strip it from the title
    if author_name:
        author_words = normalize_author(author_name)
        for word in author_words:
            t = re.sub(r'\b' + re.escape(word) + r'\b', ' ', t)
    
    # 5. Final: keep only a-z
    return re.sub(r'[^a-z]', '', t)
