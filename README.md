# condor_adstash Integration Testing

    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
    !!                                                                !!
    !!  WARNING: THIS ENVIRONMENT IS FOR LOCAL TESTING ONLY.          !!
    !!                                                                !!
    !!  ALL SECURITY IS DISABLED across all services:                 !!
    !!  - Elasticsearch: xpack.security.enabled=false                 !!
    !!  - OpenSearch: DISABLE_SECURITY_PLUGIN=true                    !!
    !!  - HTCondor: CLAIMTOBE authentication (accepts any identity)   !!
    !!                                                                !!
    !!  DO NOT expose these containers to any network beyond your     !!
    !!  local machine. DO NOT use any of these configurations in      !!
    !!  production or on shared infrastructure.                       !!
    !!                                                                !!
    !!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!

## Requirements

- Docker with Docker Compose v2+
- The HTCondor source tree at `../htcondor/` (the adstash package and
  `condor_adstash` script are mounted from there)

## Services

| Service | Image | Host Port |
|---------|-------|-----------|
| es7 | elasticsearch:7.17.23 | 9207 |
| es8 | elasticsearch:8.19.20 | 9208 |
| es9 | elasticsearch:9.4.4 | 9209 |
| os2 | opensearch:2.19.4 | 9202 |
| os3 | opensearch:3.5.0 | 9203 |
| condor | htcondor/mini:lts | - |
| test-runner | Python 3.9 + elasticsearch-py 7 + opensearch-py 2 | - |
| test-runner-es8py | Python 3.9 + elasticsearch-py 8 + opensearch-py 2 | - |
| test-runner-es9py | Python 3.9 + elasticsearch-py 9 + opensearch-py 2 | - |
| test-runner-os3py | Python 3.9 + elasticsearch-py 7 + opensearch-py 3 | - |

## How to Run

Run all backends with the default test-runner (elasticsearch-py 7 + opensearch-py 2):

    docker compose up --build --abort-on-container-exit --remove-orphans \
        condor es7 es8 es9 os2 os3 test-runner; \
        docker compose down -v

Run a subset of backends:

    docker compose up --build --abort-on-container-exit --remove-orphans \
        condor es7 test-runner; \
        docker compose down -v

Test with a different Python library version:

    # elasticsearch-py 8
    docker compose up --build --abort-on-container-exit --remove-orphans \
        condor es7 es8 test-runner-es8py; \
        docker compose down -v

    # elasticsearch-py 9 (not currently supported by adstash)
    docker compose up --build --abort-on-container-exit --remove-orphans \
        condor es9 test-runner-es9py; \
        docker compose down -v

    # opensearch-py 3
    docker compose up --build --abort-on-container-exit --remove-orphans \
        condor os2 os3 test-runner-os3py; \
        docker compose down -v

Tests that target backends not included in the command are automatically
skipped (via DNS resolution check).

## Test Matrix

Tested and passing as of 2026-08-19:

| Server \ Library | es-py 7 | es-py 8 | es-py 9 | os-py 2 | os-py 3 |
|------------------|---------|---------|---------|---------|---------|
| ES 7.17.23 | pass | pass | FAIL | - | - |
| ES 8.19.20 | pass | pass | FAIL | - | - |
| ES 9.4.4 | pass | pass | FAIL | - | - |
| OS 2.19.4 | - | - | - | pass | pass |
| OS 3.5.0 | - | - | - | pass | pass |

elasticsearch-py 9 is not yet officially supported by adstash.

## Test Coverage

End-to-end adstash workflow:
  - Submit a job to the HTCondor schedd and wait for completion
  - Verify the job appears in schedd history
  - Run `condor_adstash --init_index` to generate index setup files
  - Push ILM policy (ES only), index template, and initial index
  - Run `condor_adstash --standalone --schedd_history` to push docs
  - Query the index and verify docs landed with correct fields
  - Clean up index, template, and ILM policy

## Notes

- All search engine data directories use tmpfs (RAM-backed), so no data
  persists after `docker compose down`.
- Index replicas default to 0 (injected into templates and index creation
  bodies) since all backends run as single-node clusters.
- The HTCondor condor uses CLAIMTOBE authentication. The test-runner
  submits jobs as `submituser`.
- ILM policies are only pushed to Elasticsearch backends. OpenSearch uses
  ISM which has a different API.
- The `condor_adstash` script and `adstash` package are mounted read-only
  from the HTCondor source tree, so changes are picked up immediately
  without rebuilding the test-runner image.
