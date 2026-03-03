"""
API integration tests for the ChrisTreasurer backend.
Runs against the live Railway deployment (or API_URL env var override).
All tests clean up their own created data.
"""
import io
import os

import httpx
import pytest

BASE_URL = os.environ.get("API_URL", "https://alloc-api-production.up.railway.app")

# ---------------------------------------------------------------------------
# Shared client fixture
# ---------------------------------------------------------------------------

@pytest.fixture(scope="session")
def client():
    with httpx.Client(base_url=BASE_URL, timeout=60) as c:
        yield c


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def test_health(client):
    r = client.get("/")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


# ---------------------------------------------------------------------------
# Students
# ---------------------------------------------------------------------------

def test_list_students(client):
    r = client.get("/students")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


@pytest.fixture
def test_student(client):
    """Create a student, yield its data, then delete it."""
    r = client.post("/students", json={
        "name": "Pytest Test Student",
        "email": "pytest.student@example.com",
        "year": 1,
        "is_ra": False,
        "accessibility_required": False,
    })
    assert r.status_code == 200
    data = r.json()
    yield data
    client.delete(f"/students/{data['id']}")


def test_create_student(test_student):
    assert test_student["name"] == "Pytest Test Student"
    assert test_student["email"] == "pytest.student@example.com"
    assert test_student["year"] == 1
    assert test_student["is_ra"] is False
    assert test_student["accessibility_required"] is False


def test_update_student(client, test_student):
    r = client.put(
        f"/students/{test_student['id']}",
        json={"name": "Pytest Student Updated"},
    )
    assert r.status_code == 200
    assert r.json()["name"] == "Pytest Student Updated"


def test_delete_student(client):
    r = client.post("/students", json={
        "name": "Pytest Delete Me",
        "email": "pytest.deleteme@example.com",
        "year": 2,
        "is_ra": False,
        "accessibility_required": False,
    })
    assert r.status_code == 200
    student_id = r.json()["id"]

    rd = client.delete(f"/students/{student_id}")
    assert rd.status_code == 200
    assert rd.json()["deleted"] == student_id


VALID_CSV = (
    "name,email,year,is_ra,accessibility_required\n"
    "Pytest Import User,pytest.import@example.com,1,false,false\n"
)
INVALID_CSV = "wrong_col,another_col\nfoo,bar\n"


def test_import_students_valid(client):
    files = {
        "file": ("students.csv", io.BytesIO(VALID_CSV.encode()), "text/csv")
    }
    r = client.post("/students/import", files=files)
    assert r.status_code == 200
    data = r.json()
    assert data["imported"] > 0
    # Cleanup imported rows
    for row in data["rows"]:
        if row["email"] == "pytest.import@example.com":
            client.delete(f"/students/{row['id']}")


def test_import_students_invalid(client):
    files = {
        "file": ("bad.csv", io.BytesIO(INVALID_CSV.encode()), "text/csv")
    }
    r = client.post("/students/import", files=files)
    assert r.status_code == 422


# ---------------------------------------------------------------------------
# Blocks
# ---------------------------------------------------------------------------

def test_list_blocks(client):
    r = client.get("/blocks")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


@pytest.fixture
def test_block(client):
    """Create a block, yield its data, then delete it."""
    r = client.post("/blocks", json={"name": "Pytest Block"})
    assert r.status_code == 200
    data = r.json()
    yield data
    client.delete(f"/blocks/{data['id']}")


def test_create_block(test_block):
    assert test_block["name"] == "Pytest Block"
    assert "id" in test_block


def test_update_block(client, test_block):
    r = client.put(f"/blocks/{test_block['id']}", json={"name": "Pytest Block Updated"})
    assert r.status_code == 200
    assert r.json()["name"] == "Pytest Block Updated"


def test_delete_block(client):
    r = client.post("/blocks", json={"name": "Pytest Delete Block"})
    assert r.status_code == 200
    block_id = r.json()["id"]

    rd = client.delete(f"/blocks/{block_id}")
    assert rd.status_code == 200
    assert rd.json()["deleted"] == block_id


# ---------------------------------------------------------------------------
# Rooms
# ---------------------------------------------------------------------------

def test_list_rooms(client):
    r = client.get("/rooms")
    assert r.status_code == 200
    data = r.json()
    assert isinstance(data, list)
    # Each room should have nested block name
    if data:
        assert "blocks" in data[0]


@pytest.fixture
def test_room(client, test_block):
    """Create a room inside test_block, yield its data, then delete it."""
    r = client.post("/rooms", json={
        "block_id": test_block["id"],
        "room_number": "P-99",
        "floor": 9,
        "room_type": "en-suite",
        "is_accessible": False,
        "is_available": True,
    })
    assert r.status_code == 200
    data = r.json()
    yield data
    client.delete(f"/rooms/{data['id']}")


def test_create_room(test_room, test_block):
    assert test_room["room_number"] == "P-99"
    assert test_room["block_id"] == test_block["id"]
    assert test_room["room_type"] == "en-suite"


def test_update_room(client, test_room):
    r = client.put(f"/rooms/{test_room['id']}", json={"room_type": "studio"})
    assert r.status_code == 200
    assert r.json()["room_type"] == "studio"


def test_delete_room(client, test_block):
    r = client.post("/rooms", json={
        "block_id": test_block["id"],
        "room_number": "P-98",
        "floor": 8,
        "room_type": "shared-bathroom",
        "is_accessible": False,
        "is_available": True,
    })
    assert r.status_code == 200
    room_id = r.json()["id"]

    rd = client.delete(f"/rooms/{room_id}")
    assert rd.status_code == 200
    assert rd.json()["deleted"] == room_id


# ---------------------------------------------------------------------------
# Rules
# ---------------------------------------------------------------------------

def test_list_rules(client):
    r = client.get("/rules")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


@pytest.fixture
def test_rule(client):
    """Create a rule, yield its data, then delete it."""
    r = client.post("/rules", json={
        "name": "Pytest Test Rule",
        "rule_type": "soft",
        "category": "test",
        "config": {"weight": "low"},
        "is_active": True,
    })
    assert r.status_code == 200
    data = r.json()
    yield data
    client.delete(f"/rules/{data['id']}")


def test_create_rule(test_rule):
    assert test_rule["name"] == "Pytest Test Rule"
    assert test_rule["rule_type"] == "soft"
    assert test_rule["is_active"] is True


def test_update_rule(client, test_rule):
    r = client.put(f"/rules/{test_rule['id']}", json={"is_active": False})
    assert r.status_code == 200
    assert r.json()["is_active"] is False


def test_delete_rule(client):
    r = client.post("/rules", json={
        "name": "Pytest Delete Rule",
        "rule_type": "soft",
        "category": "test",
        "config": {},
        "is_active": False,
    })
    assert r.status_code == 200
    rule_id = r.json()["id"]

    rd = client.delete(f"/rules/{rule_id}")
    assert rd.status_code == 200
    assert rd.json()["deleted"] == rule_id


# ---------------------------------------------------------------------------
# Allocation runs
# ---------------------------------------------------------------------------

def test_list_runs(client):
    r = client.get("/runs")
    assert r.status_code == 200
    assert isinstance(r.json(), list)


def test_run_allocation(client):
    """Trigger a full allocation run (5 s placeholder). Timeout set to 30 s."""
    r = client.post("/run", json={"cohort": "first-years"}, timeout=30)
    assert r.status_code == 200
    data = r.json()
    assert data["status"] == "complete"
    assert "run_id" in data
    assert "stats" in data
    stats = data["stats"]
    assert "students_assigned_pct" in stats
    assert "hard_constraint_violations" in stats


def test_get_run_detail(client):
    """Fetch the most recent run and verify its detail endpoint."""
    runs = client.get("/runs").json()
    assert len(runs) > 0, "No runs found — run test_run_allocation first"
    run_id = runs[0]["id"]

    r = client.get(f"/runs/{run_id}")
    assert r.status_code == 200
    data = r.json()
    assert data["id"] == run_id
    assert "allocations" in data
    assert isinstance(data["allocations"], list)
