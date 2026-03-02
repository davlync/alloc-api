import asyncio
import csv
import io
import os

from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from supabase import create_client, Client

app = FastAPI(title="ChrisTreasurer API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

COLLEGE_ID = "00000000-0000-0000-0000-000000000001"


def get_supabase() -> Client:
    url = os.environ["SUPABASE_URL"]
    key = os.environ["SUPABASE_SERVICE_KEY"]
    return create_client(url, key)


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/")
def health_check():
    return {"status": "ok"}


# ── Students ──────────────────────────────────────────────────────────────────

@app.get("/students")
def list_students():
    sb = get_supabase()
    res = sb.table("students").select("*").eq("college_id", COLLEGE_ID).order("name").execute()
    return res.data


@app.post("/students")
def create_student(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("students").insert(data).execute()
    return res.data[0]


@app.put("/students/{student_id}")
def update_student(student_id: str, data: dict):
    sb = get_supabase()
    data.pop("id", None)
    data.pop("college_id", None)
    res = (
        sb.table("students")
        .update(data)
        .eq("id", student_id)
        .eq("college_id", COLLEGE_ID)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Student not found")
    return res.data[0]


@app.delete("/students/{student_id}")
def delete_student(student_id: str):
    sb = get_supabase()
    sb.table("students").delete().eq("id", student_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": student_id}


@app.post("/students/import")
async def import_students(file: UploadFile = File(...)):
    """
    Accepts a CSV with columns: name, email, year, is_ra, accessibility_required
    is_ra and accessibility_required accept: true/false, yes/no, 1/0 (case-insensitive)
    """
    content = await file.read()
    text = content.decode("utf-8-sig")  # handle Excel BOM
    reader = csv.DictReader(io.StringIO(text))

    def to_bool(val: str) -> bool:
        return str(val).strip().lower() in ("true", "yes", "1")

    rows = []
    errors = []
    for i, row in enumerate(reader, start=2):  # row 1 = header
        try:
            rows.append({
                "college_id": COLLEGE_ID,
                "name": row["name"].strip(),
                "email": row["email"].strip().lower(),
                "year": int(row["year"].strip()),
                "is_ra": to_bool(row.get("is_ra", "false")),
                "accessibility_required": to_bool(row.get("accessibility_required", "false")),
            })
        except (KeyError, ValueError) as e:
            errors.append({"row": i, "error": str(e)})

    if errors:
        raise HTTPException(status_code=422, detail={"parse_errors": errors})

    sb = get_supabase()
    # upsert on email so re-uploads don't duplicate
    res = sb.table("students").upsert(rows, on_conflict="college_id,email").execute()
    return {"imported": len(res.data), "rows": res.data}


# ── Blocks ────────────────────────────────────────────────────────────────────

@app.get("/blocks")
def list_blocks():
    sb = get_supabase()
    res = sb.table("blocks").select("*").eq("college_id", COLLEGE_ID).order("name").execute()
    return res.data


@app.post("/blocks")
def create_block(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("blocks").insert(data).execute()
    return res.data[0]


@app.put("/blocks/{block_id}")
def update_block(block_id: str, data: dict):
    sb = get_supabase()
    data.pop("id", None)
    data.pop("college_id", None)
    res = (
        sb.table("blocks")
        .update(data)
        .eq("id", block_id)
        .eq("college_id", COLLEGE_ID)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Block not found")
    return res.data[0]


@app.delete("/blocks/{block_id}")
def delete_block(block_id: str):
    sb = get_supabase()
    sb.table("blocks").delete().eq("id", block_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": block_id}


# ── Rooms ─────────────────────────────────────────────────────────────────────

@app.get("/rooms")
def list_rooms():
    sb = get_supabase()
    res = (
        sb.table("rooms")
        .select("*, blocks(name)")
        .eq("college_id", COLLEGE_ID)
        .order("room_number")
        .execute()
    )
    return res.data


@app.post("/rooms")
def create_room(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("rooms").insert(data).execute()
    return res.data[0]


@app.put("/rooms/{room_id}")
def update_room(room_id: str, data: dict):
    sb = get_supabase()
    data.pop("id", None)
    data.pop("college_id", None)
    data.pop("blocks", None)
    res = (
        sb.table("rooms")
        .update(data)
        .eq("id", room_id)
        .eq("college_id", COLLEGE_ID)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Room not found")
    return res.data[0]


@app.delete("/rooms/{room_id}")
def delete_room(room_id: str):
    sb = get_supabase()
    sb.table("rooms").delete().eq("id", room_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": room_id}


# ── Rules ─────────────────────────────────────────────────────────────────────

@app.get("/rules")
def list_rules():
    sb = get_supabase()
    res = sb.table("rules").select("*").eq("college_id", COLLEGE_ID).order("rule_type").execute()
    return res.data


@app.post("/rules")
def create_rule(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("rules").insert(data).execute()
    return res.data[0]


@app.put("/rules/{rule_id}")
def update_rule(rule_id: str, data: dict):
    sb = get_supabase()
    data.pop("id", None)
    data.pop("college_id", None)
    res = (
        sb.table("rules")
        .update(data)
        .eq("id", rule_id)
        .eq("college_id", COLLEGE_ID)
        .execute()
    )
    if not res.data:
        raise HTTPException(status_code=404, detail="Rule not found")
    return res.data[0]


@app.delete("/rules/{rule_id}")
def delete_rule(rule_id: str):
    sb = get_supabase()
    sb.table("rules").delete().eq("id", rule_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": rule_id}


# ── Allocation runs ───────────────────────────────────────────────────────────

@app.get("/runs")
def list_runs():
    sb = get_supabase()
    res = (
        sb.table("allocation_runs")
        .select("*")
        .eq("college_id", COLLEGE_ID)
        .order("created_at", desc=True)
        .execute()
    )
    return res.data


@app.get("/runs/{run_id}")
def get_run(run_id: str):
    sb = get_supabase()
    run = (
        sb.table("allocation_runs")
        .select("*")
        .eq("id", run_id)
        .single()
        .execute()
    )
    if not run.data:
        raise HTTPException(status_code=404, detail="Run not found")
    allocations = (
        sb.table("allocations")
        .select("*, students(name, email, year), rooms(room_number, room_type, floor, blocks(name))")
        .eq("run_id", run_id)
        .execute()
    )
    return {**run.data, "allocations": allocations.data}


@app.post("/run")
async def run_allocation(data: dict = {}):
    sb = get_supabase()
    cohort = data.get("cohort", "first-years")

    # Create the run record
    run = (
        sb.table("allocation_runs")
        .insert({"college_id": COLLEGE_ID, "cohort": cohort, "status": "running"})
        .execute()
    )
    run_id = run.data[0]["id"]

    # Simulate algorithm (replace with real algorithm later)
    await asyncio.sleep(5)

    # Mark complete with placeholder stats
    stats = {
        "students_assigned_pct": 94,
        "friend_requests_matched_pct": 71,
        "room_type_preferences_met_pct": 88,
        "block_preferences_met_pct": 79,
        "hard_constraint_violations": 0,
    }
    warnings = [
        "14 students: requested block full — assigned to next preferred block",
        "8 students: en-suite unavailable — assigned shared bathroom",
        "3 students: friend request could not be matched within same block",
    ]
    sb.table("allocation_runs").update({
        "status": "complete",
        "stats": stats,
        "warnings": warnings,
        "completed_at": "now()",
    }).eq("id", run_id).execute()

    return {"run_id": run_id, "status": "complete", "stats": stats, "warnings": warnings}
