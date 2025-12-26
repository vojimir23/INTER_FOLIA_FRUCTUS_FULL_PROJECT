from enum import Enum
from typing import List, Optional, Union
from pydantic import BaseModel, Field, model_validator

# -ENUMS for validation and type safety
class EntityType(str, Enum):
    WORK = "work"
    EXPRESSION = "expression"
    MANIFESTATION = "manifestation"
    ITEM = "item"
    PAGE = "page"
    PERSON = "person"
    GRAPH = "graph" 
    VISUAL_OBJECT = "visual_object"
    PHYSICAL_OBJECT = "physical_object"
    INSTITUTION = "institution"
    EVENT = "event"
    ABSTRACT_CHARACTER = "abstract_character"

class Logic(str, Enum):
    AND = "and"
    OR = "or"
    NOT = "not"
    GTE = "gte"  # Greater than or equal to
    LTE = "lte"  # Less than or equal to

class Operator(str, Enum):
    EQUALS = "equals"
    CONTAINS = "contains"
    ALL_WORDS = "all_words"
    PHRASE = "phrase"
    ANY_WORD = "any_word"

class ProximityLogic(str, Enum):
    AND = "and"
    OR = "or"
    NOT = "not"

class ProximityOperator(str, Enum):
    NEAR = "near"
    BEFORE = "before"
    AFTER = "after"

# Renamed to avoid conflict with pydantic.Field
class FilterableField(str, Enum):
    AUTHOR = "author"
    CLASSIFICATION = "classification"
    TYPE_OF_EXPRESSION = "type_of_expression"
    LANGUAGE = "language"
    ROLE_OF_PERSON_OR_INSTITUTION = "role_of_person_or_institution"
    PLACE = "place"
    PUBLICATION_DATE = "publication_date"
    PUBLISHER = "publisher"
    EDITOR = "editor"
    CORRECTOR = "corrector"
    SPONSOR = "sponsor"
    PRESERVATION_STATUS = "preservation_status"
    OWNER = "owner"
    MATERIAL = "material"
    TYPE_OF_ITEM = "type_of_item"
    VISUAL_OBJECT_OWNER = "visual_object_owner"
    VISUAL_OBJECT_INSCRIBER = "visual_object_inscriber"
    VISUAL_OBJECT_SENDER = "visual_object_sender"
    VISUAL_OBJECT_RECIPIENT = "visual_object_recipient"
    WORK_TITLE = "work_title"
    PERSON_NAME = "person_name"
    PERSON_ROLE = "person_role" 
    PERSON_GENDER = "person_gender"
    PERSON_BIRTH_DATE = "person_birth_date"
    PERSON_DEATH_DATE = "person_death_date"
    VISUAL_OBJECT_TRANSCRIPTION = "visual_object_transcription"
    PROXIMITY_TEXT_SEARCH = "proximity_text_search"
    INSTITUTION_NAME = "institution_name"
    INSTITUTION_PLACE = "institution_place"
    INSTITUTION_ROLE = "institution_role" 
    EVENT_NAME = "event_name"
    EVENT_DATE = "event_date" 
    TRANSLATOR = "translator"
    EXPRESSION_EDITOR = "expression_editor"
    SCRIPTWRITER = "scriptwriter"
    COMPOSITOR = "compositor"
    REVIEWER = "reviewer"
    OTHER_SECONDARY_ROLE = "other_secondary_role"
    
    # --- MODIFIED KEYS ---
    SEARCH_FOR_ROLES_IN_EXPRESSION = "search_for_roles_in_expression"
    SEARCH_FOR_ROLES_IN_MANIFESTATION = "search_for_roles_in_manifestation"
    # ---------------------

    ROLES_RELATED_TO_VISUAL_OBJECT = "roles_related_to_visual_object"
    PERSON_OR_INSTITUTION = "person_or_institution"
    TYPE_OF_VISUAL_OBJECT = "type_of_visual_object"
    TYPE_OF_PHYSICAL_OBJECT = "type_of_physical_object"
    ROLES_RELATED_TO_PHYSICAL_OBJECT = "roles_related_to_physical_object"
    
    # --- NEW FILTER ---
    DIGITALIZATION = "digitalization"

    # --- NEW FILTERS FOR VISUAL OBJECT ---
    VISUAL_OBJECT_FUNCTION = "visual_object_function"
    VISUAL_OBJECT_LANGUAGE = "visual_object_language"
    VISUAL_OBJECT_INSTRUMENT = "visual_object_instrument"
    VISUAL_OBJECT_COLOUR = "visual_object_colour"
    # ------------------------------------
    
    # --- NEW FILTERS FOR PHYSICAL OBJECT ---
    PHYSICAL_OBJECT_PLACE = "physical_object_place"
    PHYSICAL_OBJECT_DATE = "physical_object_date"
    # -------------------------------------
    
    # --- NEW FILTERS FOR ABSTRACT CHARACTER ---
    ABSTRACT_CHARACTER_NAME = "abstract_character_name"
    ABSTRACT_CHARACTER_MENTIONED_IN = "abstract_character_mentioned_in"
    # ----------------------------------------


class OrderByField(str, Enum):
    WORK_TITLE = "work_title"
    EXPRESSION_TITLE = "expression_title"
    MANIFESTATION_TITLE = "manifestation_title"
    ITEM_LABEL = "item_label"
    PAGE_TITLE = "page_title"
    PROJECT = "project"
    PERSON_NAME = "person_name"
    VISUAL_OBJECT_NAME = "visual_object_name"
    PHYSICAL_OBJECT_NAME = "physical_object_name"
    INSTITUTION_NAME = "institution_name"
    EVENT_NAME = "event_name"
    ABSTRACT_CHARACTER_NAME = "ac_name"

class Era(str, Enum):
    AD = "AD"
    BC = "BC"

# --- PYDANTIC MODELS for API requests 

class ProximityTerm(BaseModel):
    text: str
    logic: Optional[ProximityLogic] = None
    proximity: Optional[ProximityOperator] = None

class ProximityQuery(BaseModel):
    terms: List[ProximityTerm] = Field(..., min_length=1, max_length=3)
    distance: int = Field(5, ge=1)
    case_sensitive: bool = False
    diacritics_sensitive: bool = False
    exact_match: bool = False

class FilterRule(BaseModel):
    field: FilterableField
    logic: Logic
    values: Optional[List[str]] = None
    op: Optional[Operator] = Operator.EQUALS
    era: Optional[Era] = None
    case_sensitive: Optional[bool] = False
    diacritics_sensitive: Optional[bool] = False
    proximity_query: Optional[ProximityQuery] = None

    @model_validator(mode='before')
    @classmethod
    def check_rule_type(cls, values):
        field = values.get('field')
        proximity_query = values.get('proximity_query')
        value_list = values.get('values')

        if field == FilterableField.PROXIMITY_TEXT_SEARCH:
            if not proximity_query:
                raise ValueError("A 'proximity_query' must be provided for proximity text search.")
            if value_list is not None:
                values['values'] = None 
        else:
            if proximity_query is not None:
                 raise ValueError("'proximity_query' is only allowed for proximity text search.")
            if value_list is None:
                raise ValueError("'values' must be provided for non-proximity search fields.")
        return values


class SearchQuery(BaseModel):
    projects: Optional[List[str]] = None
    entity: EntityType = EntityType.WORK
    rules: List[FilterRule]
    limit: int = Field(500, ge=1, le=10000)
    offset: int = Field(0, ge=0)
    order_by: Optional[OrderByField] = None
    fields: Optional[List[str]] = None
    summary: bool = Field(False, description="If true, returns a minimal shape for each entity.")

# --- MODIFICATION START ---
class GraphType(str, Enum):
    GENERAL = "general"
    MENTIONS = "mentions"
    PERSON_AUTHORSHIP_OWNERSHIP = "person_authorship_ownership"

class GraphGeneralFilter(BaseModel):
    entity_types: List[str] = Field(..., description="List of entity types to include in the graph (e.g., 'work', 'expression').")
    relationships: List[str] = Field(..., description="List of relationship names to trace (e.g., 'is_expression_of_work').")

class MentionsGraphFilter(BaseModel):
    entity_types: List[str] = Field(..., description="List of entity types to include in the mentions graph.")
    mention_directions: List[str] = Field(..., description="Directions of mentions to include ('Mentioning', 'Mentioned by').")

class PersonAuthorshipOwnershipGraphFilter(BaseModel):
    person_names: Optional[List[str]] = Field(None, description="Optional list of person names to filter the graph.")
    entity_types: List[str] = Field(..., description="List of entity types to include in the graph.")
    relationships: List[str] = Field(..., description="List of relationship names to trace.")

class GraphSearchQuery(BaseModel):
    projects: Optional[List[str]] = None
    graph_type: GraphType = Field(GraphType.GENERAL, description="The type of graph to generate.")
    general_filters: Optional[GraphGeneralFilter] = None
    mentions_filters: Optional[MentionsGraphFilter] = None
    person_authorship_ownership_filters: Optional[PersonAuthorshipOwnershipGraphFilter] = None

    @model_validator(mode='before')
    @classmethod
    def check_filters_for_graph_type(cls, values):
        graph_type = values.get('graph_type')
        general_filters = values.get('general_filters')
        mentions_filters = values.get('mentions_filters')
        person_authorship_ownership_filters = values.get('person_authorship_ownership_filters')

        if graph_type == 'general' and not general_filters:
            raise ValueError("general_filters must be provided for graph_type 'general'")
        if graph_type == 'mentions' and not mentions_filters:
            raise ValueError("mentions_filters must be provided for graph_type 'mentions'")
        if graph_type == 'person_authorship_ownership' and not person_authorship_ownership_filters:
            raise ValueError("person_authorship_ownership_filters must be provided for graph_type 'person_authorship_ownership'")
        
        return values
# --- MODIFICATION END ---