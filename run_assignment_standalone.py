"""
Run the full FHIR assignment from NDJSON files (no database required).
Answers all 5 assignment questions and prints results.

Usage:
  python run_assignment_standalone.py
  python run_assignment_standalone.py > assignment_results.txt
"""
import json
from pathlib import Path
from collections import defaultdict

BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "sample-bulk-fhir-datasets-10-patients"


def iter_ndjson(path: Path):
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            yield json.loads(line)


def load_json_list(path: Path):
    return list(iter_ndjson(path))


def main():
    print("Loading FHIR data from NDJSON...")
    patients = load_json_list(DATA_DIR / "Patient.000.ndjson")
    immunizations = load_json_list(DATA_DIR / "Immunization.000.ndjson")
    observations = load_json_list(DATA_DIR / "Observation.000.ndjson")
    diagnostic_reports = load_json_list(DATA_DIR / "DiagnosticReport.000.ndjson")

    def patient_id_from_ref(ref):
        if not ref or "/" not in ref:
            return None
        return ref.split("/", 1)[1]

    # -------------------------------------------------------------------------
    # Q1: How many patients had the influenza vaccine?
    # -------------------------------------------------------------------------
    influenza_cvx = "140"
    influenza_patient_ids = set()
    for imm in immunizations:
        coding = (imm.get("vaccineCode") or {}).get("coding") or []
        if not coding:
            continue
        c = coding[0]
        display = (c.get("display") or "").lower()
        code = c.get("code")
        if code == influenza_cvx or "influenza" in display:
            pid = patient_id_from_ref(imm.get("patient", {}).get("reference"))
            if pid:
                influenza_patient_ids.add(pid)

    print("\n" + "=" * 60)
    print("Q1: How many patients had the influenza vaccine?")
    print("=" * 60)
    print(f"Answer: {len(influenza_patient_ids)} patients")
    print(f"Patient IDs: {sorted(influenza_patient_ids)}")

    # -------------------------------------------------------------------------
    # Q2: What are the top 5 most common vaccines?
    # -------------------------------------------------------------------------
    vaccine_counts = defaultdict(int)
    for imm in immunizations:
        coding = (imm.get("vaccineCode") or {}).get("coding") or []
        name = (coding[0].get("display") or coding[0].get("code") or "Unknown") if coding else "Unknown"
        vaccine_counts[name] += 1

    top5 = sorted(vaccine_counts.items(), key=lambda x: (-x[1], x[0]))[:5]
    print("\n" + "=" * 60)
    print("Q2: What are the top 5 most common vaccines?")
    print("=" * 60)
    for i, (name, count) in enumerate(top5, 1):
        print(f"  {i}. {name}: {count} administrations")

    # -------------------------------------------------------------------------
    # Q3: Average number of vital signs measured per patient
    # -------------------------------------------------------------------------
    vital_signs_by_patient = defaultdict(int)
    for obs in observations:
        cat = (obs.get("category") or [{}])
        codings = (cat[0] if cat else {}).get("coding") or []
        code = codings[0].get("code") if codings else None
        if code == "vital-signs":
            pid = patient_id_from_ref(obs.get("subject", {}).get("reference"))
            if pid:
                vital_signs_by_patient[pid] += 1

    all_patient_ids = {p.get("id") for p in patients if p.get("id")}
    vital_counts = [vital_signs_by_patient[pid] for pid in all_patient_ids]
    avg_vitals = sum(vital_counts) / len(vital_counts) if vital_counts else 0

    print("\n" + "=" * 60)
    print("Q3: Average number of vital signs measured per patient")
    print("=" * 60)
    print(f"Answer: {avg_vitals:.2f} (average across {len(all_patient_ids)} patients)")

    # -------------------------------------------------------------------------
    # Q4: Most recent report with a recorded cholesterol lab result per patient
    # -------------------------------------------------------------------------
    cholesterol_loinc = {"2093-3", "2089-1", "2571-8"}  # total chol, LDL, triglycerides
    obs_by_patient_date = defaultdict(list)  # (patient_id, date) -> [obs]
    for obs in observations:
        codings = (obs.get("code") or {}).get("coding") or []
        code = codings[0].get("code") if codings else None
        if code not in cholesterol_loinc:
            continue
        pid = patient_id_from_ref(obs.get("subject", {}).get("reference"))
        eff = obs.get("effectiveDateTime")
        if not pid or not eff:
            continue
        date = eff.split("T")[0] if isinstance(eff, str) else str(eff)[:10]
        obs_by_patient_date[(pid, date)].append(obs)

    # Build set of (patient_id, effective_date) that have cholesterol observations
    dr_by_patient_date = []  # (patient_id, effective_at, report_id, code)
    for dr in diagnostic_reports:
        pid = patient_id_from_ref(dr.get("subject", {}).get("reference"))
        eff = dr.get("effectiveDateTime")
        if not pid or not eff:
            continue
        date = eff.split("T")[0] if isinstance(eff, str) else str(eff)[:10]
        if (pid, date) in obs_by_patient_date:
            dr_by_patient_date.append((pid, eff, dr.get("id"), (dr.get("code") or {}).get("coding") or [{}]))

    # Most recent per patient
    by_patient = defaultdict(list)
    for pid, eff, rid, codings in dr_by_patient_date:
        by_patient[pid].append((eff, rid, codings[0].get("code") if codings else None))

    for pid in by_patient:
        by_patient[pid].sort(key=lambda x: x[0], reverse=True)

    print("\n" + "=" * 60)
    print("Q4: Most recent report with a recorded cholesterol lab result for each patient")
    print("=" * 60)
    for pid in sorted(by_patient.keys()):
        eff, report_id, code = by_patient[pid][0]
        print(f"  Patient {pid}: report_id={report_id}, effective={eff}, code={code}")

    if not by_patient:
        print("  (No diagnostic reports found that coincide with cholesterol lab observations on same date.)")

    # -------------------------------------------------------------------------
    # Q5: Two interesting facts about a specific patient
    # -------------------------------------------------------------------------
    sample_patient_id = next(iter(all_patient_ids)) if all_patient_ids else None
    if not sample_patient_id:
        print("\nQ5: No patients in dataset.")
        return

    sample = next((p for p in patients if p.get("id") == sample_patient_id), None)
    names = (sample or {}).get("name") or []
    first_name = names[0] if names else {}
    family = first_name.get("family", "N/A")
    given = (first_name.get("given") or ["N/A"])[0]
    gender = (sample or {}).get("gender", "N/A")
    birth = (sample or {}).get("birthDate", "N/A")
    imm_count = sum(1 for i in immunizations if patient_id_from_ref(i.get("patient", {}).get("reference")) == sample_patient_id)
    vital_count = vital_signs_by_patient.get(sample_patient_id, 0)
    had_flu = sample_patient_id in influenza_patient_ids

    print("\n" + "=" * 60)
    print("Q5: Two interesting facts about a specific patient")
    print("=" * 60)
    print(f"Patient: {given} {family} (id: {sample_patient_id})")
    print(f"  Gender: {gender}, Birth date: {birth}")
    print("")
    print("  Two interesting facts:")
    print(f"  1. This patient has {imm_count} immunization(s) recorded and {'received' if had_flu else 'did not receive'} the influenza vaccine.")
    print(f"  2. This patient has {vital_count} vital sign observation(s) recorded in the dataset.")
    print("\n" + "=" * 60)
    print("Assignment run complete.")
    print("=" * 60)


if __name__ == "__main__":
    main()
