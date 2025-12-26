# app/config.py
import re
from typing import Pattern, Dict, Tuple

# --- DATABASE CONFIGURATION ---
MONGO_URI: str = "mongodb://localhost:27017/"
#MONGO_URI: str = "mongodb://db:27017/" # for docker containers
#MONGO_URI: str = "mongodb://172.20.27.85:27017/" #over vpn, but ISLAB server has old pymongo...
MONGO_DB_NAME: str = "FaithInterFolia"


# --- PRECOMPILED REGEX ---
# Changed regex to capture any number as a year ---
YEAR_RE: Pattern[str] = re.compile(r'\b(\d+)\b')



# --- PERSON-TO-PERSON RELATIONSHIP MAPPINGS ---
# Maps the internal database relationship name to a tuple of (outgoing_label, incoming_label)
# This allows for correct display on the details page regardless of the relationship's direction.
PERSON_RELATIONSHIPS: Dict[str, Tuple[str, str]] = {
    # =================================================================
    # --- English Relationship Keys & Labels ---
    # =================================================================

    # --- Core Family ---
    "is_parent_of": ("Is the parent of", "Is the child of"),
    "is_the_parent_of": ("Is the parent of", "Is the child of"),
    "is_sibling_of": ("Is the sibling of", "Is the sibling of"),
    "is_the_half_sibling_of": ("Is the half-sibling of", "Is the half-sibling of"),
    "is_the_spouse_of": ("Is the spouse of", "Is the spouse of"),

    # --- Extended Family ---
    "is_the_uncle_of": ("Is the uncle of", "Is the nephew/niece of"),
    "is_the_grandfather/grandmother_of": ("Is the grandfather/grandmother of", "Is the grandson/granddaughter of"),
    "is_the_cousin_of": ("Is the cousin of", "Is the cousin of"),

    # --- In-Laws ---
    "is_parent-in-law_of": ("Is the parent-in-law of", "Is the child-in-law of"),
    "is_the_brother-in-law/sister-in-law_of": ("Is the brother-in-law/sister-in-law of", "Is the brother-in-law/sister-in-law of"),

    # --- Great-Relatives ---
    "is_the_great-uncle/great-aunt_of": ("Is the great-uncle/great-aunt of", "Is the great-nephew/great-niece of"),
    "is_the_great-grandfather/great-grandmother_of": ("Is the great-grandfather/great-grandmother of", "Is the great-grandson/great-granddaughter of"),

    # --- Social & Professional Relationships ---
    "is_the_teacher_of": ("Is the teacher of", "Is the student/pupil of"),
    "is_friend_of": ("Is a friend of", "Is a friend of"),
    "is_the_master_of": ("Is the master of", "Is the apprentice of"),
    "is_the_patron_of": ("Is the patron of", "Is the client of"),
    "is_the_employer_of": ("Is the employer of", "Is the employee of"),
    "is_the_godparent_of": ("Is the godparent of", "Is the godchild of"),
    "is_the_ally_of": ("Is the ally of", "Is the ally of"),
    "is_the_rival_of": ("Is the rival of", "Is the rival of"),
    "influenced": ("Influenced", "Was influenced by"),

    # --- Romantic Relationships ---
    "is_in_love_with": ("Is in love with", "Is in love with"),
    "is_the_lover_of": ("Is the lover of", "Is the lover of"),

    # =================================================================
    # --- Italian Relationship Keys & Labels ---
    # =================================================================

    # --- Core Family (Famiglia Ristretta) ---
    "è_genitore_di": ("È genitore di", "È figlio/a di"),
    "è_fratello/sorella_di": ("È fratello/sorella di", "È fratello/sorella di"),
    "è_fratellastro/sorellastra_di": ("È fratellastro/sorellastra di", "È fratellastro/sorellastra di"),
    "è_sposo/a_di": ("È sposo/a di", "È sposo/a di"),

    # --- Extended Family (Famiglia Allargata) ---
    "è_zio_di": ("È zio di", "È nipote di"),
    "è_nonno/a_di": ("È nonno/a di", "È nipote di"),
    "è_cugino/a_di": ("È cugino/a di", "È cugino/a di"),

    # --- In-Laws (Parenti Acquisiti) ---
    "è_suocero/a_di": ("È suocero/a di", "È genero/nuora di"),
    "è_cognato/a_di": ("È cognato/a di", "È cognato/a di"),

    # --- Great-Relatives (Avi e Discendenti) ---
    "è_prozio/a_di": ("È prozio/a di", "È pronipote di"),
    "è_bisnonno/a_di": ("È bisnonno/a di", "È pronipote di"),

    # --- Social & Professional Relationships (Relazioni Sociali e Professionali) ---
    "è_maestro_di": ("È maestro di", "È allievo di"),
    "è_amico/a_di": ("È amico/a di", "È amico/a di"),
    "è_maestro/a_di": ("È maestro/a di", "È apprendista di"),
    "è_patrono/a_di": ("È patrono/a di", "È cliente di"),
    "è_datore_di_lavoro_di": ("È datore di lavoro di", "È dipendente di"),
    "è_padrino/madrina_di": ("È padrino/madrina di", "È figlioccio/a di"),
    "è_alleato/a_di": ("È alleato/a di", "È alleato/a di"),
    "è_rivale_di": ("È rivale di", "È rivale di"),
    "ha_influenzato": ("Ha influenzato", "È stato/a influenzato/a da"),

    # --- Romantic Relationships (Relazioni Romantiche) ---
    "è_innamorato/a_di": ("È innamorato/a di", "È innamorato/a di"),
    "è_l'amante_di": ("È l'amante di", "È l'amante di"),
}