# app/services/entities/hypothesis.py
from typing import List, Dict, Any, Tuple

def process_hypotheses(raw_hypotheses: List[Dict[str, Any]], context: Dict[str, Any]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    hypothesis_details_cache = {}
    id2projects = context["id2projects"]
    id2label = context["id2label"]

    for hypothesis in raw_hypotheses:
        hypothesis_id = hypothesis["_id"]
        hypothesis_details_cache[hypothesis_id] = {
            "hypothesis_title": id2label.get(hypothesis_id, "Unknown Hypothesis"), 
            "relationships": [], 
            "projects": id2projects.get(hypothesis_id, ["Unknown Project"])
        }
    
    return [], hypothesis_details_cache # Hypotheses not searchable in main list