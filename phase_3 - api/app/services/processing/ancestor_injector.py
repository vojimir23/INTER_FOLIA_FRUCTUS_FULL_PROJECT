from typing import Dict, Any, List
from app.services.processing.cache_utils import add_relationship

def apply_all_ancestors(
    data_maps: Dict[str, Dict[str, Any]],
    details_caches: Dict[str, Dict[str, Any]],
    context: Dict[str, Any]
) -> None:
    """
    Injects hierarchical ancestors into entity caches (Step 6.5 and 7 logic).
    """
    
    # Unpack maps
    works_data_map = data_maps["works"]
    expressions_data_map = data_maps["expressions"]
    manifestations_data_map = data_maps["manifestations"]
    manifestation_volumes_data_map = data_maps["manifestation_volumes"]
    items_data_map = data_maps["items"]
    physical_objects_data_map = data_maps["physical_objects"]
    pages_data_map = data_maps["pages"]

    # Unpack caches
    expression_details_cache = details_caches["expression"]
    manifestation_details_cache = details_caches["manifestation"]
    manifestation_volume_details_cache = details_caches["manifestation_volume"]
    item_details_cache = details_caches["item"]
    physical_object_details_cache = details_caches["physical_object"]
    page_details_cache = details_caches["page"]
    visual_object_details_cache = details_caches.get("visual_object")

    def inject_ancestor(entity_cache, ancestor_id, ancestor_type, ancestor_label, ancestor_card):
        if not entity_cache or not ancestor_id: return
        
        rel_data = {
            "type": f"is_part_of_{ancestor_type}",
            "direction": "outgoing",
            "group": "parent",
            "target_id": ancestor_id,
            "target_type": ancestor_type,
            "target_label": ancestor_label,
            "target_card": ancestor_card
        }
        add_relationship(entity_cache, rel_data)

    # 1. Expressions (Add Work)
    for exp in data_maps["expressions_list"]:
        cache = expression_details_cache.get(exp['expression_id'])
        if w_id := exp.get('work_id'):
            if w_data := works_data_map.get(w_id):
                inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])

    # 2. Manifestations (Add Work, Expression)
    for man in data_maps["manifestations_list"]:
        cache = manifestation_details_cache.get(man['manifestation_id'])
        if w_id := man.get('work_id'):
            if w_data := works_data_map.get(w_id):
                inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
        if e_id := man.get('expression_id'):
            if e_data := expressions_data_map.get(e_id):
                inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])

    # 3. Manifestation Volumes (Add Work, Expression, Manifestation)
    for vol in data_maps["manifestation_volumes_list"]:
        cache = manifestation_volume_details_cache.get(vol['manifestation_volume_id'])
        if w_id := vol.get('work_id'):
            if w_data := works_data_map.get(w_id):
                inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
        if e_id := vol.get('expression_id'):
            if e_data := expressions_data_map.get(e_id):
                inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
        if m_id := vol.get('parent_manifestation_id'):
            if m_data := manifestations_data_map.get(m_id):
                inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])

    # 4. Items (Add Work, Expression, Manifestation)
    for item in data_maps["items_list"]:
        cache = item_details_cache.get(item['item_id'])
        if w_id := item.get('work_id'):
            if w_data := works_data_map.get(w_id):
                inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
        if e_id := item.get('expression_id'):
            if e_data := expressions_data_map.get(e_id):
                inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
        if m_id := item.get('manifestation_id'):
            if m_data := manifestations_data_map.get(m_id):
                inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])

    # --- PREPARE PO -> ITEM MAP ---
    po_to_item_map = {po_id: item_id for item_id, po_ids in context["item_to_physical_object_ids"].items() for po_id in po_ids}
    # --- PREPARE PO -> PAGE MAP ---
    page_to_physical_object_ids = context.get("page_to_physical_object_ids", {})
    po_to_page_map = {po_id: page_id for page_id, po_ids in page_to_physical_object_ids.items() for po_id in po_ids}
    # --- PREPARE PAGE -> PO PARENT MAP ---
    page_to_po_page_parent_map = context.get("page_to_po_page_parent_map", {})

    # 5. Physical Objects (Add Item and its ancestors)
    for po in data_maps["physical_objects_list"]:
        po_id = po['physical_object_id']
        cache = physical_object_details_cache.get(po_id)
        hr_id = po.get('human_readable_id', '')

        # --- PO_PAG_PO_ Ancestor Injection ---
        if hr_id.startswith("PO_PAG_PO_"):
            # 1. Inject Page
            if parent_page_id := po_to_page_map.get(po_id):
                if p_data := pages_data_map.get(parent_page_id):
                    inject_ancestor(cache, parent_page_id, 'page', p_data['page_title'], p_data['card'])
                    
                    # 2. Inject Parent PO (Grandparent of current PO)
                    if grandparent_po_id := page_to_po_page_parent_map.get(parent_page_id):
                        if gp_data := physical_objects_data_map.get(grandparent_po_id):
                            inject_ancestor(cache, grandparent_po_id, 'physical_object', gp_data['physical_object_name'], gp_data['card'])
                            
                            # 3. Inject Item (via Grandparent PO)
                            if item_id := po_to_item_map.get(grandparent_po_id):
                                if i_data := items_data_map.get(item_id):
                                    inject_ancestor(cache, item_id, 'item', i_data['item_label'], i_data['card'])
                                    # Inject Item's ancestors
                                    if w_id := i_data.get('work_id'):
                                        if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                                    if e_id := i_data.get('expression_id'):
                                        if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                                    if m_id := i_data.get('manifestation_id'):
                                        if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])
        
        # Standard PO Logic (Direct Item link)
        elif i_id := po_to_item_map.get(po_id):
            if i_data := items_data_map.get(i_id):
                inject_ancestor(cache, i_id, 'item', i_data['item_label'], i_data['card'])
                # Inject Item's ancestors
                if w_id := i_data.get('work_id'):
                    if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                if e_id := i_data.get('expression_id'):
                    if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                if m_id := i_data.get('manifestation_id'):
                    if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])

    # 6. Pages (Add ancestors based on human_readable_id)
    page_to_item_map = {page_id: item_id for item_id, page_ids in context["item_to_page_ids"].items() for page_id in page_ids}
    page_to_manifestation_map = context.get("page_to_manifestation_map", {})
    page_to_volume_map = context.get("page_to_manifestation_volume_map", {})
    
    for page in data_maps["pages_list"]:
        p_id = page['page_id']
        cache = page_details_cache.get(p_id)
        hr_id = page.get('human_readable_id', '')

        # Case a) Page is part of a PO which is part of another PO which is part of an Item
        if hr_id.startswith("PAG_PO_PAG_PO"):
            # The defining parent is the Physical Object. Always add this first.
            if po_id := page_to_po_page_parent_map.get(p_id):
                po_name = context["physical_object_to_name"].get(po_id, "Unknown PO")
                po_card = physical_objects_data_map.get(po_id, {}).get('card', {})
                inject_ancestor(cache, po_id, 'physical_object', po_name, po_card)
                
                # Now, find the Item and its ancestors.
                item_id_found = po_to_item_map.get(po_id)
                    
                if item_id_found:
                    if i_data := items_data_map.get(item_id_found):
                        inject_ancestor(cache, item_id_found, 'item', i_data['item_label'], i_data['card'])
                        if w_id := i_data.get('work_id'):
                            if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                        if e_id := i_data.get('expression_id'):
                            if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                        if m_id := i_data.get('manifestation_id'):
                            if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])

        # Case b) Page is part of a Physical Object which is part of an Item
        elif hr_id.startswith("PAG_PO_PAG_") or hr_id.startswith("PAG_PO_"):
            # The defining parent is the Physical Object. Always add this first.
            if po_id := page_to_po_page_parent_map.get(p_id):
                po_name = context["physical_object_to_name"].get(po_id, "Unknown PO")
                po_card = physical_objects_data_map.get(po_id, {}).get('card', {})
                inject_ancestor(cache, po_id, 'physical_object', po_name, po_card)
                
                # Now, find the Item and its ancestors.
                item_id_found = po_to_item_map.get(po_id)
                
                if not item_id_found:
                    item_id_found = page_to_item_map.get(p_id) # Fallback to direct link
                    
                if item_id_found:
                    if i_data := items_data_map.get(item_id_found):
                        inject_ancestor(cache, item_id_found, 'item', i_data['item_label'], i_data['card'])
                        if w_id := i_data.get('work_id'):
                            if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                        if e_id := i_data.get('expression_id'):
                            if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                        if m_id := i_data.get('manifestation_id'):
                            if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])

        # Case c/d) Page is part of a Manifestation or Volume
        elif hr_id.startswith("PAG_M_"): # This covers PAG_M_VOL_ as well
            # Prioritize Volume parent
            if v_id := page_to_volume_map.get(p_id):
                if v_data := manifestation_volumes_data_map.get(v_id):
                    inject_ancestor(cache, v_id, 'manifestation_volume', v_data['manifestation_volume_title'], v_data['card'])
                    if w_id := v_data.get('work_id'):
                        if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                    if e_id := v_data.get('expression_id'):
                        if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                    if m_id := v_data.get('parent_manifestation_id'):
                        if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])
            # Fallback to Manifestation parent
            elif m_id := page_to_manifestation_map.get(p_id):
                if m_data := manifestations_data_map.get(m_id):
                    inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])
                    if w_id := m_data.get('work_id'):
                        if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                    if e_id := m_data.get('expression_id'):
                        if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
        
        # Case e) Page is part of an Item directly
        elif hr_id.startswith("PAG_"):
            if i_id := page_to_item_map.get(p_id):
                if i_data := items_data_map.get(i_id):
                    inject_ancestor(cache, i_id, 'item', i_data['item_label'], i_data['card'])
                    if w_id := i_data.get('work_id'):
                        if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                    if e_id := i_data.get('expression_id'):
                        if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                    if m_id := i_data.get('manifestation_id'):
                        if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])

    # 7. Visual Objects (Add ancestors via Page)
    # Prepare VO -> Page Map
    vo_to_page_map = {vo_id: page_id for page_id, vo_ids in context["page_to_visual_object_ids"].items() for vo_id in vo_ids}

    if visual_object_details_cache:
        for vo in data_maps["visual_objects_list"]:
            vo_id = vo['visual_object_id']
            cache = visual_object_details_cache.get(vo_id)
            
            # 1. Find Page
            if p_id := vo_to_page_map.get(vo_id):
                if p_data := pages_data_map.get(p_id):
                    inject_ancestor(cache, p_id, 'page', p_data['page_title'], p_data['card'])
                    
                    # Path A: Page -> Item
                    item_id = page_to_item_map.get(p_id)
                    if not item_id:
                        if parent_po_id := page_to_po_page_parent_map.get(p_id):
                            item_id = po_to_item_map.get(parent_po_id)
                    
                    if item_id:
                        if i_data := items_data_map.get(item_id):
                            inject_ancestor(cache, item_id, 'item', i_data['item_label'], i_data['card'])
                            if w_id := i_data.get('work_id'):
                                if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                            if e_id := i_data.get('expression_id'):
                                if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                            if m_id := i_data.get('manifestation_id'):
                                if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])
                    
                    # Path B: Page -> Manifestation Volume
                    elif v_id := page_to_volume_map.get(p_id):
                        if v_data := manifestation_volumes_data_map.get(v_id):
                            inject_ancestor(cache, v_id, 'manifestation_volume', v_data['manifestation_volume_title'], v_data['card'])
                            if w_id := v_data.get('work_id'):
                                if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                            if e_id := v_data.get('expression_id'):
                                if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])
                            if m_id := v_data.get('parent_manifestation_id'):
                                if m_data := manifestations_data_map.get(m_id): inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])

                    # Path C: Page -> Manifestation (Direct)
                    elif m_id := page_to_manifestation_map.get(p_id):
                        if m_data := manifestations_data_map.get(m_id):
                            inject_ancestor(cache, m_id, 'manifestation', m_data['manifestation_title'], m_data['card'])
                            if w_id := m_data.get('work_id'):
                                if w_data := works_data_map.get(w_id): inject_ancestor(cache, w_id, 'work', w_data['work_title'], w_data['card'])
                            if e_id := m_data.get('expression_id'):
                                if e_data := expressions_data_map.get(e_id): inject_ancestor(cache, e_id, 'expression', e_data['expression_title'], e_data['card'])