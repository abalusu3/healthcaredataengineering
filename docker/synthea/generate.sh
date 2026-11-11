#!/usr/bin/env bash
# Generate a synthetic health-plan member population, one Synthea run per state.
# Output: /output/<state_slug>/csv/*.csv
set -euo pipefail

STATES="${SYNTHEA_STATES:-Missouri}"
POPULATION="${SYNTHEA_POPULATION_PER_STATE:-1000}"
SEED="${SYNTHEA_SEED:-42}"
HEAP="${SYNTHEA_JAVA_HEAP:-4g}"

i=0
for STATE in $STATES; do
  slug=$(echo "$STATE" | tr '[:upper:] ' '[:lower:]_')
  out="/output/${slug}"
  echo ">> Generating ${POPULATION} members for ${STATE} -> ${out}"
  rm -rf "${out}"
  # Distinct seed per state keeps runs reproducible but non-identical.
  java -Xmx"${HEAP}" -jar synthea.jar \
    -p "${POPULATION}" \
    -s "$((SEED + i))" \
    --exporter.baseDirectory "${out}" \
    --exporter.csv.export true \
    --exporter.fhir.export false \
    --exporter.hospital.fhir.export false \
    --exporter.practitioner.fhir.export false \
    --exporter.years_of_history 10 \
    "${STATE}"
  # Completion marker: ingestion only picks up states that have it, so a
  # half-written run can never be landed in bronze.
  touch "${out}/_SUCCESS"
  i=$((i + 1))
done

echo ">> Done. Files:"
ls -lh /output/*/csv | head -50
