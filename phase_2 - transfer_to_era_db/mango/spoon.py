import re
import itertools
import numpy as np

def process_field(field, delimiter, lower=False, pattern=None):
    """
    Processes a field by ensuring it's a list of strings and then splitting
    by a delimiter if one is provided.
    
    Includes specific logic to handle IDs that contain delimiters internally.
    """
    # 1. Ensure we have a list to work with.
    if not isinstance(field, list):
        field = [field]

    # 2. Convert all items to strings.
    string_field = [str(item) for item in field]

    # 3. Apply the splitting logic.
    if delimiter is not None:
        processed_items = set()
        
        # DEFINITION OF PREFIXES
        # This list controls what counts as an "ID string" vs "Regular Text".
        prefixes = [
            "p_", "w_", "ex_", "m_", "m_vol_", "i_", "PO_", "PO_PAG_",
            "PAG_", "VO_", "e_", "ac_", "inst_", "loc_"
        ]
        
        # Critical: Sort by length descending. 
        # This ensures 'm_vol_' is matched before 'm_', avoiding partial matches.
        prefixes.sort(key=len, reverse=True)
        
        # Build a regex that matches the delimiter ONLY if followed by a known prefix.
        prefix_pattern = "|".join(map(re.escape, prefixes))
        
        # FIX: Added 'r' before the f-string (rf"...") to fix the SyntaxWarning.
        # This treats '\s' as a literal regex character rather than a failed python escape.
        smart_split_regex = rf"{re.escape(delimiter)}\s*(?=(?:{prefix_pattern}))"

        for item in string_field:
            clean_item = item.strip()
            if not clean_item:
                continue

            # CHECK: Does this specific item START with one of your ID prefixes?
            is_id_string = False
            for p in prefixes:
                if clean_item.startswith(p):
                    is_id_string = True
                    break
            
            if is_id_string:
                # CASE: ID STRING (e.g. "m_code(1;2)")
                # Only split if the delimiter is followed by another valid prefix (like "m_" or "p_")
                # This protects internal semicolons.
                parts = re.split(smart_split_regex, clean_item)
            else:
                # CASE: REGULAR TEXT (e.g. "milk; bread")
                # Split at every delimiter regardless of what follows.
                parts = clean_item.split(delimiter)

            # Clean and collect results
            for part in parts:
                final_part = part.strip()
                if lower:
                    final_part = final_part.lower()
                
                if final_part:
                    processed_items.add(final_part)
                    
        return np.array(list(processed_items))
    
    # If no delimiter is specified, just return the stringified items.
    return np.array(string_field)