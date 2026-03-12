from __future__ import annotations

import re
from typing import Any


NOT_AVAILABLE = "Not Reported"


LLM_SCHEMA: dict[str, Any] = {
    "paper_identification": {
        "ref_number": "",
        "year": "",
        "authors": "",
        "title": "",
        "journal_source": "",
        "country_population_studied": "",
    },
    "ingredient_intervention": {
        "primary_ingredient": "",
        "ingredient_category": "",
        "glp1_relation": "",
        "other_ingredients_used": "",
    },
    "mechanism_of_action": {
        "mechanism_category": "",
        "biological_pathway": "",
        "hormonal_impact": "",
        "metabolic_function": "",
        "microbiome_link": "",
    },
    "study_design": {
        "study_type": "",
        "sample_size": "",
        "population": "",
        "dose": "",
        "delivery_form": "",
        "duration": "",
    },
    "outcomes_measured": {
        "weight_loss_outcome": "",
        "appetite_or_satiety_outcome": "",
        "fat_mass_body_fat_outcome": "",
        "metabolic_health_outcome": "",
        "blood_lipids_outcome": "",
        "energy_fatigue_outcome": "",
        "cardiometabolic_outcome": "",
        "microbiome_outcome": "",
    },
    "side_effects_safety": {
        "side_effects_reported": "",
        "nutrient_impact": "",
        "safety_level": "",
        "long_term_risk": "",
    },
    "evidence_strength": {
        "evidence_strength": "",
        "clinical_trial": "",
        "study_model": "",
    },
    "strategic_insight_extraction": {
        "key_finding_summary": "",
        "innovation_signal": "",
        "possible_claim_territory": "",
        "consumer_translation": "",
        "relevance_to_weight_management_platform": "",
    },
}

ENUM_ALLOWED_BY_EXCEL_HEADER: dict[str, list[str]] = {
    "Ingredient Category": ["Vitamin", "Botanical", "Dietary Fiber", "Probiotic", "Polyphenol", "Peptide", "Other", NOT_AVAILABLE],
    "GLP-1 Relation": ["Direct", "Indirect", "No", NOT_AVAILABLE],
    # Note: "Mechanism Category" is intentionally free-text (no forced enum).
    # Note: "Hormonal Impact" is intentionally free-text (no forced enum).
    # Note: "Metabolic Function" is intentionally free-text (no forced enum).
    "Microbiome Link": ["Yes", "No", NOT_AVAILABLE],
    "Study Type": ["RCT", "Clinical Trial", "Animal Study", "Observational", "Review", NOT_AVAILABLE],
    "Delivery Form": ["Capsule", "Powder", "Food", "Injection", "Other", NOT_AVAILABLE],
    "Evidence Strength": ["Strong", "Moderate", "Weak", NOT_AVAILABLE],
    "Clinical Trial": ["Yes", "No", NOT_AVAILABLE],
    "Study Model (Human/Animal/In vitro)": ["Human", "Animal", "In Vitro", NOT_AVAILABLE],
    "Relevance to Weight Management Platform": ["High", "Medium", "Low", NOT_AVAILABLE],
}

ENUM_ECHO_OPTIONS_BY_EXCEL_HEADER: dict[str, list[str]] = {
    # Back-compat cleanup for older prompts/caches where the model sometimes echoed the full options list.
    "Mechanism Category": [
        "Appetite Suppression",
        "Metabolism",
        "Fat Oxidation",
        "Gut Microbiome",
        "Insulin Sensitivity",
        "Energy Expenditure",
        "Other",
        NOT_AVAILABLE,
    ],
    "Hormonal Impact": ["GLP-1", "Insulin", "Leptin", "Ghrelin", "None Mentioned", NOT_AVAILABLE],
    "Metabolic Function": ["Fat Metabolism", "Glucose Control", "Energy Metabolism", NOT_AVAILABLE],
}


def normalize_enum(value: object, allowed: list[str]) -> str:
    """
    Normalize an enum-ish LLM value to one of the allowed values.

    - If the model returns the entire "A | B | C" option list, treat as NOT_AVAILABLE.
    - If the model returns multiple values, pick the first valid one in order.
    - Otherwise, return NOT_AVAILABLE if it doesn't match any allowed value.
    """

    vs = str(value).strip() if value is not None else ""
    if not vs or vs == NOT_AVAILABLE:
        return NOT_AVAILABLE

    lower_to_canonical = {a.lower(): a for a in allowed}
    direct = lower_to_canonical.get(vs.lower())
    if direct is not None:
        return direct

    # Detect "returned the whole option list" (common failure mode when the prompt uses that as a placeholder).
    if "|" in vs:
        parts = [p.strip() for p in vs.split("|") if p.strip()]
        if parts:
            parts_set = {p.lower() for p in parts}
            allowed_set = {a.lower() for a in allowed}
            if parts_set == allowed_set:
                return NOT_AVAILABLE

    # If multiple values are present, choose the first valid value.
    tokens = [t.strip() for t in re.split(r"[;|,/]+", vs) if t.strip()]
    best: str | None = None
    for t in tokens:
        cand = lower_to_canonical.get(t.lower())
        if cand is None:
            continue
        # Prefer a real value over Not Reported if both appear.
        if cand == NOT_AVAILABLE:
            best = best or NOT_AVAILABLE
            continue
        return cand

    if best is not None:
        return best

    return NOT_AVAILABLE


def normalize_enum_multi(value: object, allowed: list[str]) -> str:
    """
    Normalize an enum-ish LLM value to a semicolon-separated list of allowed values.

    - If the model returns the entire "A | B | C" option list, treat as NOT_AVAILABLE.
    - If multiple valid values are present, keep them (deduped) in the order they appear.
    - If no valid value is found, return NOT_AVAILABLE.
    """

    vs = str(value).strip() if value is not None else ""
    if not vs or vs == NOT_AVAILABLE:
        return NOT_AVAILABLE

    lower_to_canonical = {a.lower(): a for a in allowed}

    # Detect "returned the whole option list" (common failure mode when the prompt uses that as a placeholder).
    if "|" in vs:
        parts = [p.strip() for p in vs.split("|") if p.strip()]
        if parts:
            parts_set = {p.lower() for p in parts}
            allowed_set = {a.lower() for a in allowed}
            if parts_set == allowed_set:
                return NOT_AVAILABLE

    tokens = [t.strip() for t in re.split(r"[;|,/]+", vs) if t.strip()]
    chosen: list[str] = []
    chosen_set: set[str] = set()

    for t in tokens:
        cand = lower_to_canonical.get(t.lower())
        if cand is None:
            continue
        key = cand.lower()
        if key in chosen_set:
            continue
        chosen_set.add(key)
        chosen.append(cand)

    if not chosen:
        # Single token might be a full match (no separators)
        direct = lower_to_canonical.get(vs.lower())
        if direct is not None:
            return direct
        return NOT_AVAILABLE

    # If any real values exist, drop Not Reported.
    if any(v != NOT_AVAILABLE for v in chosen):
        chosen = [v for v in chosen if v != NOT_AVAILABLE]

    return "; ".join(chosen)


def strip_enum_options_echo(value: object, options: list[str]) -> str:
    """
    If the model returns the entire "A | B | C" option list (a common copy/paste failure),
    treat it as NOT_AVAILABLE. Otherwise keep the text as-is.
    """

    vs = str(value).strip() if value is not None else ""
    if not vs or vs == NOT_AVAILABLE:
        return NOT_AVAILABLE

    # Detect "returned the whole option list" (common failure mode when the prompt uses that as a placeholder).
    if "|" in vs:
        parts = [p.strip() for p in vs.split("|") if p.strip()]
        if parts:
            parts_set = {p.lower() for p in parts}
            options_set = {a.lower() for a in options}
            if parts_set == options_set:
                return NOT_AVAILABLE

    return vs


EXCEL_TO_SCHEMA_PATH: dict[str, tuple[str, str]] = {
    "Ref #": ("paper_identification", "ref_number"),
    "Year": ("paper_identification", "year"),
    "Authors": ("paper_identification", "authors"),
    "Title": ("paper_identification", "title"),
    "Journal / Source": ("paper_identification", "journal_source"),
    "Country / Population Studied": ("paper_identification", "country_population_studied"),
    "Primary Ingredient": ("ingredient_intervention", "primary_ingredient"),
    "Ingredient Category": ("ingredient_intervention", "ingredient_category"),
    "GLP-1 Relation": ("ingredient_intervention", "glp1_relation"),
    "Other Ingredients Used": ("ingredient_intervention", "other_ingredients_used"),
    "Mechanism Category": ("mechanism_of_action", "mechanism_category"),
    "Biological Pathway": ("mechanism_of_action", "biological_pathway"),
    "Hormonal Impact": ("mechanism_of_action", "hormonal_impact"),
    "Metabolic Function": ("mechanism_of_action", "metabolic_function"),
    "Microbiome Link": ("mechanism_of_action", "microbiome_link"),
    "Study Type": ("study_design", "study_type"),
    "Sample Size": ("study_design", "sample_size"),
    "Population": ("study_design", "population"),
    "Dose": ("study_design", "dose"),
    "Delivery Form": ("study_design", "delivery_form"),
    "Duration": ("study_design", "duration"),
    "Weight Loss Outcome": ("outcomes_measured", "weight_loss_outcome"),
    "Appetite or Satiety Outcome": ("outcomes_measured", "appetite_or_satiety_outcome"),
    "Fat Mass / Body Fat Outcome": ("outcomes_measured", "fat_mass_body_fat_outcome"),
    "Metabolic Health Outcome": ("outcomes_measured", "metabolic_health_outcome"),
    "Blood Lipids Outcome": ("outcomes_measured", "blood_lipids_outcome"),
    "Energy / Fatigue Outcome": ("outcomes_measured", "energy_fatigue_outcome"),
    "Cardiometabolic Outcome": ("outcomes_measured", "cardiometabolic_outcome"),
    "Microbiome Outcome": ("outcomes_measured", "microbiome_outcome"),
    "Side Effects Reported": ("side_effects_safety", "side_effects_reported"),
    "Nutrient Impact": ("side_effects_safety", "nutrient_impact"),
    "Safety Level": ("side_effects_safety", "safety_level"),
    "Long-term Risk": ("side_effects_safety", "long_term_risk"),
    "Evidence Strength": ("evidence_strength", "evidence_strength"),
    "Clinical Trial": ("evidence_strength", "clinical_trial"),
    "Study Model (Human/Animal/In vitro)": ("evidence_strength", "study_model"),
    "Key Finding Summary (4–5 sentences)": ("strategic_insight_extraction", "key_finding_summary"),
    "Innovation Signal": ("strategic_insight_extraction", "innovation_signal"),
    "Possible Claim Territory": ("strategic_insight_extraction", "possible_claim_territory"),
    "Consumer Translation": ("strategic_insight_extraction", "consumer_translation"),
    "Relevance to Weight Management Platform": ("strategic_insight_extraction", "relevance_to_weight_management_platform"),
}


def normalize_headers(headers: list[str]) -> list[str]:
    cleaned: list[str] = []
    for h in headers:
        if h is None:
            continue
        hs = str(h).strip()
        if not hs:
            continue
        cleaned.append(hs)
    return cleaned


def coerce_row_values(headers: list[str], data: dict[str, Any]) -> dict[str, str]:
    """
    Ensure every header key exists and every value is a string.
    Missing/empty values become NOT_AVAILABLE.
    """
    out: dict[str, str] = {}
    for h in headers:
        v = data.get(h, NOT_AVAILABLE)
        if v is None:
            out[h] = NOT_AVAILABLE
            continue
        if isinstance(v, (int, float)):
            out[h] = str(v)
            continue
        vs = str(v).strip()
        out[h] = vs if vs else NOT_AVAILABLE
    return out
def ensure_llm_schema(data: dict[str, Any] | None) -> dict[str, dict[str, str]]:
    """
    Ensure the nested schema exists and all leaf values are non-empty strings.
    Missing/empty values become NOT_AVAILABLE.
    """
    out: dict[str, dict[str, str]] = {}
    src = data if isinstance(data, dict) else {}

    for section, fields in LLM_SCHEMA.items():
        sec_src = src.get(section, {})
        if not isinstance(sec_src, dict):
            sec_src = {}
        sec_out: dict[str, str] = {}
        for field in fields.keys():
            v = sec_src.get(field, NOT_AVAILABLE)
            vs = str(v).strip() if v is not None else ""
            sec_out[field] = vs if vs else NOT_AVAILABLE
        out[section] = sec_out
    return out


def flatten_llm_to_excel(
    headers: list[str],
    llm_data: dict[str, Any] | None,
    *,
    ref_number: int,
    fallback_primary_ingredient: str | None,
) -> dict[str, str]:
    nested = ensure_llm_schema(llm_data)
    row: dict[str, str] = {h: NOT_AVAILABLE for h in headers}

    for h in headers:
        path = EXCEL_TO_SCHEMA_PATH.get(h)
        if not path:
            continue
        section, field = path
        row[h] = nested.get(section, {}).get(field, NOT_AVAILABLE)

    # Force Ref # from sequential id if present.
    if "Ref #" in headers:
        row["Ref #"] = str(ref_number)

    # Primary Ingredient: prefer LLM value, else fallback from folder.
    if "Primary Ingredient" in headers:
        if row.get("Primary Ingredient", NOT_AVAILABLE) == NOT_AVAILABLE and fallback_primary_ingredient:
            row["Primary Ingredient"] = str(fallback_primary_ingredient).strip() or NOT_AVAILABLE

    # Enforce single-choice enums for the Excel sheet (except free-text fields).
    multi_ok = {"Study Type", "Delivery Form"}
    for h, allowed in ENUM_ALLOWED_BY_EXCEL_HEADER.items():
        if h not in row:
            continue
        if h in multi_ok:
            row[h] = normalize_enum_multi(row.get(h), allowed)
        else:
            row[h] = normalize_enum(row.get(h), allowed)

    # Back-compat cleanup: remove "options list echoed back" values from older prompts/caches.
    for h, options in ENUM_ECHO_OPTIONS_BY_EXCEL_HEADER.items():
        if h in row:
            row[h] = strip_enum_options_echo(row.get(h), options)

    return row
