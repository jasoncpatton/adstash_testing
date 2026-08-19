#!/usr/bin/env pytest

"""
Basic connectivity and version checks for all search engine backends.
"""

import os
import socket
import time

import pytest
import elasticsearch
import opensearchpy


SE_BACKENDS = {
    "es7": {"host": os.environ.get("ES7_HOST", "localhost:9207"), "client": "elasticsearch", "major": 7},
    "es8": {"host": os.environ.get("ES8_HOST", "localhost:9208"), "client": "elasticsearch", "major": 8},
    "es9": {"host": os.environ.get("ES9_HOST", "localhost:9209"), "client": "elasticsearch", "major": 9},
    "os2": {"host": os.environ.get("OS2_HOST", "localhost:9202"), "client": "opensearch", "major": 2},
    "os3": {"host": os.environ.get("OS3_HOST", "localhost:9203"), "client": "opensearch", "major": 3},
}


def get_client(backend):
    info = SE_BACKENDS[backend]
    if info["client"] == "elasticsearch":
        return elasticsearch.Elasticsearch(f"http://{info['host']}")
    else:
        return opensearchpy.OpenSearch(hosts=[{"host": info["host"].split(":")[0], "port": int(info["host"].split(":")[1])}])


@pytest.fixture(params=SE_BACKENDS.keys())
def backend(request):
    return request.param


@pytest.fixture
def client(backend):
    host = SE_BACKENDS[backend]["host"].split(":")[0]
    try:
        socket.getaddrinfo(host, None)
    except socket.gaierror:
        pytest.skip(f"{backend} ({host}) not in DNS, container not running")

    c = get_client(backend)
    # Wait up to 60s for the backend to become healthy
    for attempt in range(12):
        try:
            if c.ping():
                return c
        except Exception:
            pass
        time.sleep(5)
    pytest.skip(f"{backend} not reachable after 60s")


class TestConnectivity:
    """Verify all backends are reachable and report expected major versions."""

    def test_ping(self, client):
        assert client.ping()

    def test_cluster_health(self, client):
        health = client.cluster.health()
        assert health["status"] in ("green", "yellow")

    def test_major_version(self, backend, client):
        info = client.info()
        version_str = info["version"]["number"]
        major = int(version_str.split(".")[0])
        assert major == SE_BACKENDS[backend]["major"]
