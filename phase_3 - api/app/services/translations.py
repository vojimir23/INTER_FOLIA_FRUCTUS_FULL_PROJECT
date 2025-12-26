# Mappings for Physical Object Types
# Keys are lowercased for case-insensitive matching.
# Values are the standardized Italian output.

PHYSICAL_OBJECT_TYPE_MAP = {
    # --- Provided List ---
    "binding": "Legatura",
    "legatura": "Legatura",
    "label": "Etichetta",
    "etichetta": "Etichetta",
    "picture": "Fotografia",
    "fotografia": "Fotografia",
    "sacred image": "Immagine sacra",
    "immagine sacra": "Immagine sacra",
    "calling card": "Biglietto da visita",
    "biglietto da visita": "Biglietto da visita",
    "bookmark": "Segnalibro",
    "segnalibro": "Segnalibro",
    "botanical specimen": "Specimen botanico",
    "specimen botanico": "Specimen botanico",
    "ticket": "Biglietto",
    "biglietto": "Biglietto",
    "postcard": "Cartolina",
    "cartolina": "Cartolina",
    "letter": "Lettera",
    "lettera": "Lettera",
    "loose page": "Foglio sciolto",
    "foglio sciolto": "Foglio sciolto",
    "newspaper cutting": "Ritaglio di giornale",
    "ritaglio di giornale": "Ritaglio di giornale",
    "box": "Scatola",
    "scatola": "Scatola",
    "envelop": "Busta",
    "envelope": "Busta", # Handling common spelling
    "busta": "Busta",
    "cartographic material": "Materiale cartografico",
    "materiale cartografico": "Materiale cartografico",
    "musical score": "Spartito musicale",
    "spartito musicale": "Spartito musicale",
    "graphic material": "Materiale grafico",
    "materiale grafico": "Materiale grafico",
    "painting": "Dipinto",
    "dipinto": "Dipinto",
    "sculpture": "Scultura",
    "scultura": "Scultura",
    "medal": "Medaglia",
    "medaglia": "Medaglia",
    "coin": "Moneta",
    "moneta": "Moneta",
    "scientific instrument": "Strumento scientifico",
    "strumento scientifico": "Strumento scientifico",
    "armor": "Armatura",
    "armatura": "Armatura",
    "weapon": "Arma",
    "arma": "Arma",
    "mask": "Maschera",
    "maschera": "Maschera",
    "garment": "Indumento",
    "indumento": "Indumento"
}

# Mappings for Item Properties (from item.py)
ITEM_PRESERVATION_MAP = {
    "n (fantasma)": "Non conservato/non identificato",
    "y (esiste)": "Conservato"
}

ITEM_MATERIAL_MAP = {
    "p": "Carta",
    "v": "Pergamena"
}

ITEM_TYPE_MAP = {
    "manuscript": "Manoscritto",
    "printed": "Libro a stampa"
}

# Mappings for Expression Types (from expression.py)
EXPRESSION_TYPE_MAP = {
    "original text": "Testo originario",
    "testo originario": "Testo originario",
    "translation": "Traduzione",
    "traduzione": "Traduzione",
    "critical edition": "Edizione critica",
    "edizione critica": "Edizione critica",
    "paratext": "Paratesto",
    "paratesto": "Paratesto",
    "database": "Banca dati online",
    "banca dati online": "Banca dati online",
    "review": "Recensione",
    "recensione": "Recensione",
    "summary": "Sintesi",
    "sintesi": "Sintesi",
    "collation": "Collazione",
    "collazione": "Collazione",
    "excerpt": "Estratto",
    "estratto": "Estratto"
}


# For relation "physical_object_has_insertion_type"
PHYSICAL_OBJECT_INSERTION_TYPE_MAP = {
    "loose": "sciolto",
    "bound": "rilegato",
    "glued": "incollato",
    "taped": "applicato con il nastro adesivo",
    "sewed on": "cucito",
    "pinned": "appuntato o spillato"
}

# For relation "visual_object_has_type"
VISUAL_OBJECT_TYPE_MAP = {
    "verbal annotation": "Annotazione verbale",
    "non verbal mark": "Segno non verbale",
    "decoration": "Ornamentazione",
    "drawing": "Disegno",
    "modification of the page": "Alterazione della pagina",
    "picture": "Fotografia",
    "printed or manuscript text": "Testo",
    "watermark": "Filigrana"
}

# For relation "visual_object_has_instrument"
VISUAL_OBJECT_INSTRUMENT_MAP = {
    "pencil": "matita",
    "coloured pencil": "matita colorata",
    "pen": "penna",
    "ballpoint pen": "penna a sfera",
    "stamp": "timbro",
    "tool": "ferro o altro strumento",
    "printed": "stampa"
}

# For relation "visual_object_has_colour"
VISUAL_OBJECT_COLOUR_MAP = {
    "gold": "Oro",
    "black": "Nero",
    "light brown": "Bruno chiaro",
    "red": "Rosso",
    "dark brown": "Bruno",
    "blue": "Blu",
    "grey": "Grigio",
    "gray": "Grigio",
    "blind": "A secco",
    "yellow": "Giallo",
    "green": "Verde",
    "purple": "Viola",
    "silver": "Argento",
    "white": "Bianco",
    "orange": "Arancio",
    "pink": "Rosa"
}

# For relation "visual_object_has_transcription_quality"
VISUAL_OBJECT_TRANSCRIPTION_QUALITY_MAP = {
    "complete": "completa",
    "incomplete": "parziale",
    "impossible": "impossibile",
    "uncertain": "incerta"
}

# For relation "visual_object_has_function"
VISUAL_OBJECT_FUNCTION_MAP = {
    "ownership [provenance]": "indicazione di possesso o provenienza",
    "shelfmark": "segnatura di collocazione",
    "content identification": "identificazione del contenuto",
    "paratesti (indice, titoli correnti)": "elemento paratestuale",
    "highlight": "selezione o rilievo",
    "summary": "sintesi",
    "reference mark": "segno di rimando",
    "correction": "correzione",
    "collation": "collazione",
    "integration": "integrazione",
    "conjecture": "congettura",
    "translation": "traduzione",
    "comment": "commento",
    "unrelated note": "nota non connessa al contenuto",
    "inscription": "dedica"
}


# This map is used for targeted string replacements in date-related fields.
# The order is important to prevent partial replacements (e.g., "not before" before "before").
DATE_NOTES_TRANSLATIONS = {
    "not before": "non prima",
    "century": "sec.",
    "flourit": "flor.",
    "approximation": "ca.",
    "BC": "a.C.",
    "AD": "d.C."
}
