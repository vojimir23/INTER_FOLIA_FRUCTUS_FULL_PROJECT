import re
import itertools
import numpy as np

def process_field(field, delimiter, lower=False, pattern=None):
    """
    Processes a field by ensuring it's a list of strings and then splitting
    by a delimiter if one is provided.
    """
    # 1. Ensure we have a list to work with. This wraps single items (like strings,
    # floats, or ints) in a list so they can be processed uniformly.
    if not isinstance(field, list):
        field = [field]

    # 2. Convert all items in the list to strings. This is the key fix that
    # prevents errors when pandas provides numbers (floats/ints) instead of text.
    string_field = [str(item) for item in field]

    # 3. Apply the splitting logic to the cleaned list of strings.
    if delimiter is not None:
        # Use a list comprehension to split each string item by the delimiter.
        # Then, use itertools.chain.from_iterable to flatten the list of lists.
        # Finally, use map to strip whitespace from each resulting element.
        processed_items = set(
            map(
                lambda x: x.strip(),
                itertools.chain.from_iterable(
                    [item.lower().split(delimiter) if lower else item.split(delimiter) for item in string_field]
                )
            )
        )
        return np.array(list(processed_items))
    
    # If no delimiter is specified, just return the stringified items.
    return np.array(string_field)