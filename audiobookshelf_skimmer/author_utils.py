import re
from typing import Set

def normalize_author(text: str) -> Set[str]:
    """Returns a set of unique lowercase alpha words from author name(s)."""
    if not text:
        return set()
    # 1. Lowercase and replace non-alpha with spaces
    t = str(text).lower()
    t = re.sub(r'[^a-z]', ' ', t)
    
    # 2. Split into words and filter out common filler words and honorifics
    filler_words = {
        'read', 'by', 'the', 'and', 'with', 'narrated', 'presented', 'adaption', 'adaptation',
        'dr', 'md', 'ph', 'd', 'phd', 'prof', 'professor', 'mr', 'mrs', 'ms', 'sir', 'st', 'jr', 'sr', 'iii', 'iv'
    }
    words = t.split()
    return {w for w in words if w not in filler_words}
