#!/usr/bin/env pytest

"""
Integration tests for condor_adstash.
Submits jobs to a live HTCondor schedd, runs condor_adstash to push
history to search engine backends, then verifies docs landed.
"""

import json
import os
import socket
import subprocess
import time

import pytest
import elasticsearch
import opensearchpy
import htcondor2 as htcondor


CONDOR_HOST = "condor"

SE_BACKENDS = {
    "es7": {"host": os.environ.get("ES7_HOST", "localhost:9207"), "client": "elasticsearch", "interface": "elasticsearch"},
    "es8": {"host": os.environ.get("ES8_HOST", "localhost:9208"), "client": "elasticsearch", "interface": "elasticsearch"},
    "es9": {"host": os.environ.get("ES9_HOST", "localhost:9209"), "client": "elasticsearch", "interface": "elasticsearch"},
    "os2": {"host": os.environ.get("OS2_HOST", "localhost:9202"), "client": "opensearch", "interface": "opensearch"},
    "os3": {"host": os.environ.get("OS3_HOST", "localhost:9203"), "client": "opensearch", "interface": "opensearch"},
}


def get_se_client(backend):
    info = SE_BACKENDS[backend]
    if info["client"] == "elasticsearch":
        return elasticsearch.Elasticsearch(f"http://{info['host']}", timeout=120)
    else:
        host, port = info["host"].split(":")
        return opensearchpy.OpenSearch(hosts=[{"host": host, "port": int(port)}], timeout=120)


def se_is_available(backend):
    host = SE_BACKENDS[backend]["host"].split(":")[0]
    try:
        socket.getaddrinfo(host, None)
        return get_se_client(backend).ping()
    except Exception:
        return False


@pytest.fixture(scope="module")
def schedd():
    try:
        socket.getaddrinfo(CONDOR_HOST, None)
    except socket.gaierror:
        pytest.skip(f"{CONDOR_HOST} not in DNS, container not running")

    for attempt in range(12):
        try:
            collector = htcondor.Collector(CONDOR_HOST)
            schedd_ads = collector.query(htcondor.AdTypes.Schedd)
            if schedd_ads:
                return htcondor.Schedd(schedd_ads[0])
        except Exception:
            pass
        time.sleep(5)
    pytest.skip(f"Schedd not reachable after 60s")


@pytest.fixture(scope="module")
def completed_job(schedd):
    """Submit a short job and wait for it to complete."""
    submit = htcondor.Submit({
        "executable": "/bin/sleep",
        "arguments": "1",
        "transfer_executable": "false",
        "initialdir": "/home/submituser",
        "requirements": "!isUndefined(TARGET.Arch)",
        "log": "/tmp/test_job.log",
    })
    result = schedd.submit(submit, count=2)
    cluster_id = result.cluster()
    print(f"\nSubmitted jobs {cluster_id}.0-1")

    # Poll until both jobs leave the queue
    for attempt in range(60):
        jobs = schedd.query(
            constraint=f"ClusterId == {cluster_id}",
            projection=["ClusterId", "ProcId", "JobStatus"],
        )
        if not jobs:
            print(f"Jobs {cluster_id}.0-1 have left the queue")
            break
        statuses = {j["ProcId"]: j["JobStatus"] for j in jobs}
        print(f"Jobs {cluster_id} statuses: {statuses}")
        time.sleep(2)
    else:
        pytest.fail(f"Jobs {cluster_id}.0-1 did not complete within 120s")

    return cluster_id


class TestJobHistory:
    """Verify the completed job appears in schedd history."""

    def test_job_in_history(self, schedd, completed_job):
        history = list(schedd.history(
            constraint=f"ClusterId == {completed_job}",
            projection=["ClusterId", "ProcId", "JobStatus"],
            match=2,
        ))
        assert len(history) == 2
        for h in history:
            assert h["ClusterId"] == completed_job
            assert h["JobStatus"] == 4  # Completed


@pytest.fixture(params=SE_BACKENDS.keys())
def backend(request):
    backend = request.param
    host = SE_BACKENDS[backend]["host"].split(":")[0]
    try:
        socket.getaddrinfo(host, None)
    except socket.gaierror:
        pytest.skip(f"{backend} ({host}) not in DNS, container not running")
    c = get_se_client(backend)
    for attempt in range(12):
        try:
            if c.ping():
                break
        except Exception:
            pass
        time.sleep(5)
    else:
        pytest.skip(f"{backend} not reachable after 60s")

    return backend


@pytest.fixture
def se_client(backend):
    return get_se_client(backend)


@pytest.fixture
def index_name(backend):
    return f"adstash-test-{backend}"


def init_and_create_index(se_client, index_name, init_dir, backend_info):
    """Run condor_adstash --init_index, then push the generated JSON to the SE backend."""

    # Single-node clusters need 0 replicas
    custom_settings_path = os.path.join(init_dir, "custom_settings.json")
    os.makedirs(init_dir, exist_ok=True)
    with open(custom_settings_path, "w") as f:
        json.dump({"index": {"number_of_replicas": 0}}, f)

    # Generate index setup files (no ILM for testing)
    result = subprocess.run(
        [
            "condor_adstash",
            "--se_index_name", index_name,
            "--init_index",
            "--custom_index_settings", custom_settings_path,
            "--init_output_directory", init_dir,
        ],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, f"init_index failed: {result.stdout}\n{result.stderr}"

    # Push ILM policy (ES only; OpenSearch uses ISM)
    ilm_path = os.path.join(init_dir, f"{index_name}-ilm.json")
    if os.path.exists(ilm_path) and backend_info["client"] == "elasticsearch":
        with open(ilm_path) as f:
            ilm_body = json.load(f)
        try:
            se_client.ilm.put_lifecycle(policy=f"{index_name}-ilm", body=ilm_body)
        except (ValueError, TypeError):
            se_client.ilm.put_lifecycle(name=f"{index_name}-ilm", body=ilm_body)

    # Push index template (inject 0 replicas for single-node test cluster)
    template_path = os.path.join(init_dir, f"{index_name}-template.json")
    if os.path.exists(template_path):
        with open(template_path) as f:
            template_body = json.load(f)
        template_body["template"]["settings"]["index.number_of_replicas"] = 0
        if backend_info["client"] == "opensearch":
            # OpenSearch doesn't support ES ILM settings
            for key in list(template_body["template"]["settings"].keys()):
                if key.startswith("index.lifecycle"):
                    del template_body["template"]["settings"][key]
            # Use legacy template format for OpenSearch
            legacy_body = {
                "index_patterns": template_body["index_patterns"],
                "settings": template_body["template"]["settings"],
                "mappings": template_body["template"]["mappings"],
            }
            se_client.indices.put_template(name=f"{index_name}-template", body=legacy_body)
        else:
            se_client.indices.put_index_template(name=f"{index_name}-template", body=template_body)

    # Create the initial index (inject 0 replicas for single-node test cluster)
    index_path = os.path.join(init_dir, f"{index_name}-000001.json")
    with open(index_path) as f:
        index_body = json.load(f)
    index_body.setdefault("settings", {})["index.number_of_replicas"] = 0
    # OpenSearch legacy templates apply mappings automatically on create;
    # having them in both the template and the create body causes errors
    if backend_info["client"] == "opensearch":
        index_body.pop("mappings", None)
    se_client.indices.create(index=f"{index_name}-000001", body=index_body)


class TestAdstashJsonInterfaces:
    """Run condor_adstash with --interface json/ndjson and verify output files."""

    @pytest.fixture(params=["jsonfile", "ndjsonfile"])
    def json_interface(self, request):
        return request.param

    def test_adstash_json_push(self, completed_job, json_interface, tmp_path):
        json_dir = str(tmp_path / f"output_{json_interface}")
        os.makedirs(json_dir, exist_ok=True)

        result = subprocess.run(
            [
                "condor_adstash",
                "--standalone",
                "--schedd_history",
                "--interface", json_interface,
                "--json_dir", json_dir,
                "--log_level", "DEBUG",
                "--log_file", f"/tmp/adstash_{json_interface}.log",
                "--checkpoint_file", f"/tmp/adstash_{json_interface}_checkpoint.json",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        print(f"\n--- condor_adstash stdout ({json_interface}) ---\n{result.stdout}")
        print(f"--- condor_adstash stderr ({json_interface}) ---\n{result.stderr}")
        assert result.returncode == 0, f"condor_adstash failed: {result.stderr}"

        # Verify output files were created
        output_files = sorted(os.listdir(json_dir))
        print(f"\n{len(output_files)} file(s) in {json_dir}: {output_files}")

        expected_ext = ".json" if json_interface == "jsonfile" else ".jsonl"
        matching = [f for f in output_files if f.endswith(expected_ext)]
        assert len(matching) > 0, f"No {expected_ext} files in {json_dir}: {output_files}"

        # Read and verify content
        output_path = os.path.join(json_dir, matching[0])
        with open(output_path) as f:
            content = f.read()
        print(f"\n--- {matching[0]} ---\n{content}")

        if json_interface == "jsonfile":
            docs = json.loads(content)
            if not isinstance(docs, list):
                docs = [docs]
        else:
            lines = [line for line in content.strip().splitlines() if line.strip()]
            assert len(lines) > 0, f"No lines in ndjson output {output_path}"
            docs = [json.loads(line) for line in lines]

        assert len(docs) == 2, f"Expected 2 docs, got {len(docs)}"
        for doc in docs:
            assert doc.get("ClusterId") == completed_job
            assert doc.get("JobStatus") == 4


class TestAdstashPush:
    """Run condor_adstash to push schedd history to each available SE backend."""

    def test_init_index(self, backend, se_client, index_name, tmp_path):
        init_dir = str(tmp_path / backend)
        try:
            init_and_create_index(se_client, index_name, init_dir, SE_BACKENDS[backend])
        except Exception as e:
            print(f"\ninit_and_create_index failed: {e.__class__.__name__}: {e}")
            raise

        # Verify the index exists (via the alias)
        assert se_client.indices.exists(index=index_name)

    def test_adstash_push(self, completed_job, backend, se_client, index_name):
        info = SE_BACKENDS[backend]

        result = subprocess.run(
            [
                "condor_adstash",
                "--standalone",
                "--schedd_history",
                "--interface", info["interface"],
                "--se_host", info["host"],
                "--se_index_name", index_name,
                "--log_level", "DEBUG",
                "--log_file", f"/tmp/adstash_{backend}.log",
                "--checkpoint_file", f"/tmp/adstash_{backend}_checkpoint.json",
            ],
            capture_output=True,
            text=True,
            timeout=120,
        )
        print(f"\n--- condor_adstash stdout ({backend}) ---\n{result.stdout}")
        print(f"--- condor_adstash stderr ({backend}) ---\n{result.stderr}")
        assert result.returncode == 0, f"condor_adstash failed: {result.stderr}"

    def test_docs_landed(self, completed_job, backend, se_client, index_name):
        se_client.indices.refresh(index=index_name)
        result = se_client.search(index=index_name, body={"query": {"match_all": {}}})

        hits = result["hits"]["hits"]
        assert len(hits) == 2, f"Expected 2 docs in {index_name}, got {len(hits)}"
        print(f"\n{len(hits)} doc(s) in {index_name}")

        for hit in hits:
            doc = hit["_source"]
            assert doc.get("ClusterId") == completed_job
            assert doc.get("JobStatus") == 4
            assert doc.get("Status") == "Completed"
            assert "ScheddName" in doc
            assert "RecordTime" in doc
            assert "@timestamp" in doc

    def test_cleanup_index(self, backend, se_client, index_name):
        """Clean up the test index and template."""
        se_client.indices.delete(index=f"{index_name}-*")
        try:
            if SE_BACKENDS[backend]["client"] == "opensearch":
                se_client.indices.delete_template(name=f"{index_name}-template")
            else:
                se_client.indices.delete_index_template(name=f"{index_name}-template")
        except Exception:
            pass
        if SE_BACKENDS[backend]["client"] == "elasticsearch":
            try:
                se_client.ilm.delete_lifecycle(policy=f"{index_name}-ilm")
            except Exception:
                pass
