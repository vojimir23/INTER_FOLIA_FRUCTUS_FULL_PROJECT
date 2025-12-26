# era_iff_api/app/textual_manipulation.py

import unicodedata

# This map normalizes visually similar characters (homoglyphs) from Greek and
# Cyrillic scripts to their Latin counterparts. This is crucial for a search
# that should work across these scripts. For example, searching for "Toxotes"
# should match "Τοξότης".
CONFUSABLES_MAP = {
    # --- Greek to Latin ---
    'α': 'a', 'Α': 'A', 'β': 'b', 'Β': 'B', 'γ': 'g', 'Γ': 'G',
    'δ': 'd', 'Δ': 'D', 'ε': 'e', 'Ε': 'E', 'ζ': 'z', 'Ζ': 'Z',
    'η': 'e', 'Η': 'H', 'θ': 'th', 'Θ': 'Th', 'ι': 'i', 'Ι': 'I',
    'κ': 'k', 'Κ': 'K', 'λ': 'l', 'Λ': 'L', 'μ': 'm', 'Μ': 'M',
    'ν': 'n', 'Ν': 'N', 'ξ': 'x', 'Ξ': 'X', 'ο': 'o', 'Ο': 'O',
    'π': 'p', 'Π': 'P', 'ρ': 'r', 'Ρ': 'P', 'σ': 's', 'Σ': 'S',
    'ς': 's', 'τ': 't', 'Τ': 'T', 'υ': 'y', 'Υ': 'Y', 'φ': 'ph',
    'Φ': 'Ph', 'χ': 'ch', 'Χ': 'Ch', 'ψ': 'ps', 'Ψ': 'Ps', 'ω': 'o', 'Ω': 'O','ἰ':'i',

    # --- Cyrillic to Latin ---
    'а': 'a', 'А': 'A', 'б': 'b', 'Б': 'B', 'в': 'v', 'В': 'V',
    'г': 'g', 'Г': 'G', 'д': 'd', 'Д': 'D', 'е': 'e', 'Е': 'E',
    'ё': 'e', 'Ё': 'E', 'ж': 'zh', 'Ж': 'Zh', 'з': 'z', 'З': 'Z',
    'и': 'i', 'И': 'I', 'й': 'y', 'Й': 'Y', 'к': 'k', 'К': 'K',
    'л': 'l', 'Л': 'L', 'м': 'm', 'М': 'M', 'н': 'n', 'Н': 'N',
    'о': 'o', 'О': 'O', 'п': 'p', 'П': 'P', 'р': 'r', 'Р': 'R',
    'с': 's', 'С': 'S', 'т': 't', 'Т': 'T', 'у': 'u', 'У': 'U',
    'ф': 'f', 'Ф': 'F', 'х': 'kh', 'Х': 'Kh', 'ц': 'ts', 'Ц': 'Ts',
    'ч': 'ch', 'Ч': 'Ch', 'ш': 'sh', 'Ш': 'Sh', 'щ': 'shch', 'Щ': 'Shch',
    'ъ': '', 'Ъ': '', 'ы': 'y', 'Ы': 'Y', 'ь': '', 'Ь': '',
    'э': 'e', 'Э': 'E', 'ю': 'yu', 'Ю': 'Yu', 'я': 'ya', 'Я': 'Ya',

    # --- Other common substitutions ---
    'æ': 'ae', 'Æ': 'AE', 'œ': 'oe', 'Œ': 'OE', 'ß': 'ss',
    'ð': 'd', 'Ð': 'D', 'þ': 'th', 'Þ': 'Th', 'ł': 'l', 'Ł': 'L',

    'a': 'α', 'b': 'β', 'e': 'ε', 'k': 'κ', 'o': 'ο',
    'p': 'ρ', 't': 'τ', 'u': 'υ', 'x': 'χ', 'y': 'γ',
    'A': 'Α', 'B': 'Β', 'E': 'Ε', 'K': 'Κ', 'O': 'Ο',
    'P': 'Ρ', 'T': 'Τ', 'U': 'Υ', 'X': 'Χ', 'Y': 'Γ',


}

def apply_confusables(text: str) -> str:
    """
    Replaces confusable characters in a string based on the CONFUSABLES_MAP,
    normalizing them to a common (usually Latin) base.
    """
    if not isinstance(text, str):
        return text
    return "".join(CONFUSABLES_MAP.get(char, char) for char in text)

def strip_diacritics(text: str) -> str:
    """
    Removes diacritics from a string, supporting a wide range of languages
    by normalizing Unicode characters.
    """
    if not isinstance(text, str):
        return text
    # Decompose the string into base characters and combining marks (e.g., accents)
    nfkd_form = unicodedata.normalize('NFKD', text)
    # Filter out the combining marks, leaving only the base characters
    return "".join([c for c in nfkd_form if not unicodedata.combining(c)])