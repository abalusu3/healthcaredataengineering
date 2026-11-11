"""Clinical code lists (SNOMED CT, as emitted by Synthea).

These lists are a curated, simplified stand-in for the CMS Planned Readmission Algorithm and
the HRRP cohort definitions (which use ICD-10 / CCS on real claims). Each list was derived by
profiling the actual inpatient encounters and procedures in the generated population.
"""

# --- Planned stays: never count as a readmission (CMS Planned Readmission Algorithm v4) ----

# Procedures that make a stay planned when performed during it.
PLANNED_PROCEDURE_CODES: frozenset[str] = frozenset(
    {
        "703423002",  # Combined chemotherapy and radiation therapy
        "33195004",  # External beam radiation therapy
        "70536003",  # Transplant of kidney
        "711446003",  # Transplantation of kidney regime
        "88039007",  # Transplant of lung
        "32413006",  # Transplantation of heart
        "58776007",  # Autologous bone marrow transplant
        "58390007",  # Allogeneic bone marrow transplantation
        "234336002",  # Hemopoietic stem cell transplant
        "287664005",  # Ligation of bilateral fallopian tubes (elective sterilization)
    }
)

# Encounter (admission-type) codes that are planned by definition.
PLANNED_ENCOUNTER_CODES: frozenset[str] = frozenset(
    {
        "56876005",  # Drug rehabilitation and detoxification
    }
)

# Admission reasons that indicate a scheduled/elective stay.
PLANNED_REASON_CODES: frozenset[str] = frozenset(
    {
        "183996000",  # Sterilization requested
        "698306007",  # Awaiting transplantation of kidney
    }
)

# CMS excludes admissions for medical treatment of cancer from the index population.
CANCER_TREATMENT_PROCEDURE_CODES: frozenset[str] = frozenset(
    {
        "703423002",
        "33195004",
    }
)

# --- HRRP cohorts ----------------------------------------------------------------------------
# Procedure-defined cohorts take precedence over diagnosis-defined ones.
HRRP_PROCEDURE_COHORTS: dict[str, frozenset[str]] = {
    "CABG": frozenset(
        {
            "232717009",  # Coronary artery bypass grafting
            "418824004",  # Off-pump coronary artery bypass
            "414088005",  # Emergency coronary artery bypass graft
            "63697000",  # Cardiopulmonary bypass operation
        }
    ),
    "THA_TKA": frozenset(
        {
            "52734007",  # Total replacement of hip
            "609588000",  # Prosthetic total arthroplasty of knee joint
        }
    ),
}

HRRP_REASON_COHORTS: dict[str, frozenset[str]] = {
    "AMI": frozenset({"22298006", "401303003", "401314000"}),
    "HF": frozenset({"88805009", "84114007"}),
    "PN": frozenset({"233604007"}),
    "COPD": frozenset({"13645005", "87433001", "185086009"}),
}

# CMS HRRP measure-name token -> our cohort code.
HRRP_MEASURE_COHORTS: dict[str, str] = {
    "AMI": "AMI",
    "HF": "HF",
    "PN": "PN",
    "COPD": "COPD",
    "CABG": "CABG",
    "HIP-KNEE": "THA_TKA",
}

# --- Payer classification (Synthea payer NAME -> line of business) --------------------------
PAYER_TYPES: dict[str, str] = {
    "Medicare": "Medicare",
    "Medicaid": "Medicaid",
    "Dual Eligible": "Dual",
    "NO_INSURANCE": "Uninsured",
}
DEFAULT_PAYER_TYPE = "Commercial"
