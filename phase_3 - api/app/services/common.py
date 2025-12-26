import re
from typing import Optional, Tuple, Dict, Any, Union
from app.textual_manipulation import apply_confusables, strip_diacritics
from app.config import YEAR_RE
from app.services.languages import LANGUAGE_MAP 
from app.services.translations import DATE_NOTES_TRANSLATIONS


def calculate_richness(entity: Dict[str, Any]) -> int:
    """
    Calculates a score based on how much data is in the entity's card.
    Used for deduplication logic.
    """
    score = 0
    card = entity.get("card", {})
    for key, value in card.items():
        if value and value != "<em>None</em>":
            if isinstance(value, list):
                score += len(value)
            else:
                score += 1
    return score

def translate_date_notes(note: Optional[str]) -> Optional[str]:
    """
    Translates specific keywords within date-related note strings using regex
    for case-insensitive, whole-word matching.
    """
    if not note:
        return note
    
    translated_note = note
    for key, value in DATE_NOTES_TRANSLATIONS.items():
        # Use regex for case-insensitive replacement of whole words to avoid partial matches
        pattern = re.compile(r'\b' + re.escape(key) + r'\b', re.IGNORECASE)
        translated_note = pattern.sub(value, translated_note)
        
    return translated_note

def normalize_language(code: str) -> str:
    """
    Converts ISO codes (grc, lat) to full names (Ancient Greek, Latin).
    Also handles casing issues (Grc -> Ancient Greek).
    """
    if not code:
        return "Unknown"
    
    # 1. Strip whitespace and convert to lowercase (Fixes "Grc", " grc ")
    clean_code = code.strip().lower()
    
    # 2. Return the mapped name, or the original code if not found
    return LANGUAGE_MAP.get(clean_code, code)



def roman_to_int(s: str) -> Optional[int]:
    """Converts a Roman numeral string to an integer."""
    s = s.upper()
    roman_map = {'I': 1, 'V': 5, 'X': 10, 'L': 50, 'C': 100, 'D': 500, 'M': 1000}
    if not all(c in roman_map for c in s):
        return None
    
    result = 0
    for i in range(len(s)):
        if i > 0 and roman_map[s[i]] > roman_map[s[i-1]]:
            result += roman_map[s[i]] - 2 * roman_map[s[i-1]]
        else:
            result += roman_map[s[i]]
    return result

def parse_single_date_token(token: str) -> Tuple[Optional[int], Optional[int]]:
    if not token:
        return None, None
    token = token.strip().lower()
    if token.isdigit():
        year = int(token)
        return year, year
    century_match = re.match(r'^(.*?)(?:st|nd|rd|th)?(?:\s*century)?$', token)
    if century_match:
        century_val_str = century_match.group(1).strip()
        century_num = None
        try:
            century_num = int(century_val_str)
        except ValueError:
            century_num = roman_to_int(century_val_str)
        if century_num is not None and 0 < century_num < 40:
            return (century_num - 1) * 100 + 1, century_num * 100
    roman_val = roman_to_int(token)
    if roman_val is not None:
        if roman_val < 40: return (roman_val - 1) * 100 + 1, roman_val * 100
        else: return roman_val, roman_val
    return None, None

def parse_date_to_range(date_str: Optional[str]) -> Tuple[Optional[int], Optional[int]]:
    if not date_str:
        return None, None
    parts = re.split(r'\s*-\s*|–', date_str, 1)
    if len(parts) == 2:
        start_part, end_part = parts
        start_year, _ = parse_single_date_token(start_part)
        _, end_year = parse_single_date_token(end_part)
        return start_year, end_year or start_year
    else:
        return parse_single_date_token(date_str)

def extract_label(item: Dict[str, Any]) -> str:
    for k in ("label", "description", "title", "name"):
        if k in item and item[k]:
            return str(item[k])
    return "Unknown Title"

def extract_human_readable_id(item: Dict[str, Any], prefix: Union[str, Tuple[str, ...]]) -> Optional[str]:
    """
    Extracts the description as a human_readable_id if it starts with the given prefix(es).
    """
    description = item.get("description")
    if description and description.startswith(prefix):
        return description
    return None

def get_contributor_details(entity_id: str, context: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Helper to format contributor details for expressions/manifestations."""
    id_to_collection = context.get("id_to_collection", {})
    person_to_name = context.get("person_to_name", {})
    institution_to_name = context.get("institution_to_name", {})
    id2label = context.get("id2label", {})
    
    coll = id_to_collection.get(entity_id)
    if coll == "person":
        return {
            "id": entity_id, 
            "name": person_to_name.get(entity_id, id2label.get(entity_id, "Unknown")), 
            "type": "person",
            "birth_date": context.get("person_to_birth_date", {}).get(entity_id),
            "birth_date_notes": context.get("person_to_birth_date_notes", {}).get(entity_id),
            "death_date": context.get("person_to_death_date", {}).get(entity_id),
            "death_date_notes": context.get("person_to_death_date_notes", {}).get(entity_id)
        }
    elif coll == "institution":
        return {
            "id": entity_id, 
            "name": institution_to_name.get(entity_id, id2label.get(entity_id, "Unknown")), 
            "type": "institution",
            "founding_date": context.get("institution_to_founding_date", {}).get(entity_id),
            "dissolution_date": context.get("institution_to_dissolution_date", {}).get(entity_id)
        }
    return None