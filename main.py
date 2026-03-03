import asyncio
import csv
import io
import math
import os
import re

import pandas as pd
from fastapi import FastAPI, HTTPException, UploadFile, File, Query, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
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


def _safe_str(val) -> str | None:
    """Return stripped string or None for NaN/empty values."""
    if val is None:
        return None
    s = str(val).strip()
    return None if s.lower() in ("nan", "none", "null", "") else s


def _safe_bool(val) -> bool | None:
    """Parse 0/1/True/False/NaN to bool or None."""
    if val is None:
        return None
    if isinstance(val, float) and math.isnan(val):
        return None
    s = str(val).strip().lower()
    if s in ("nan", "none", "null", ""):
        return None
    return bool(int(float(s)))


def _natural_key(s: str) -> list:
    """Sort key that orders 'Block 2' before 'Block 10'."""
    return [int(t) if t.isdigit() else t.lower() for t in re.split(r"(\d+)", s)]


def _safe_int(val, default: int) -> int:
    try:
        if isinstance(val, float) and math.isnan(val):
            return default
        return int(val)
    except (TypeError, ValueError):
        return default


# ── Health ────────────────────────────────────────────────────────────────────

@app.get("/")
def health_check():
    return {"status": "ok"}


# ── Semesters ─────────────────────────────────────────────────────────────────

@app.get("/semesters")
def list_semesters():
    sb = get_supabase()
    res = (
        sb.table("semesters")
        .select("*")
        .eq("college_id", COLLEGE_ID)
        .order("start_date", desc=True)
        .execute()
    )
    return res.data


@app.post("/semesters")
def create_semester(data: dict):
    sb = get_supabase()
    copy_from_semester_id = data.pop("copy_from_semester_id", None)
    data["college_id"] = COLLEGE_ID
    res = sb.table("semesters").insert(data).execute()
    new_semester = res.data[0]
    new_semester_id = new_semester["id"]

    if copy_from_semester_id:
        # 1. Copy blocks; build old_id → new_id map
        src_blocks = (
            sb.table("blocks")
            .select("*")
            .eq("college_id", COLLEGE_ID)
            .eq("semester_id", copy_from_semester_id)
            .execute()
        )
        block_id_map: dict[str, str] = {}
        for block in src_blocks.data:
            old_id = block["id"]
            new_block = {k: v for k, v in block.items() if k not in ("id", "created_at")}
            new_block["semester_id"] = new_semester_id
            res_b = sb.table("blocks").insert(new_block).execute()
            block_id_map[old_id] = res_b.data[0]["id"]

        # 2. Copy rooms for each block
        for old_block_id, new_block_id in block_id_map.items():
            src_rooms = (
                sb.table("rooms")
                .select("*")
                .eq("block_id", old_block_id)
                .execute()
            )
            for room in src_rooms.data:
                new_room = {k: v for k, v in room.items() if k not in ("id", "created_at", "blocks")}
                new_room["block_id"] = new_block_id
                new_room["semester_id"] = new_semester_id
                sb.table("rooms").insert(new_room).execute()

        # 3. Copy rules
        src_rules = (
            sb.table("rules")
            .select("*")
            .eq("college_id", COLLEGE_ID)
            .eq("semester_id", copy_from_semester_id)
            .execute()
        )
        for rule in src_rules.data:
            new_rule = {k: v for k, v in rule.items() if k not in ("id", "created_at")}
            new_rule["semester_id"] = new_semester_id
            sb.table("rules").insert(new_rule).execute()

    return new_semester


@app.delete("/semesters/{semester_id}")
def delete_semester(semester_id: str):
    sb = get_supabase()
    completed = (
        sb.table("allocation_runs")
        .select("id")
        .eq("semester_id", semester_id)
        .eq("status", "complete")
        .execute()
    )
    if completed.data:
        raise HTTPException(
            status_code=409,
            detail="Cannot delete semester with completed allocation runs",
        )
    sb.table("semesters").delete().eq("id", semester_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": semester_id}


# ── Students ──────────────────────────────────────────────────────────────────

@app.get("/students")
def list_students(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("students").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    return q.order("name").execute().data


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
async def import_students(
    file: UploadFile = File(...),
    semester_id: str | None = Query(None),
):
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
            entry = {
                "college_id": COLLEGE_ID,
                "name": row["name"].strip(),
                "email": row["email"].strip().lower(),
                "year": int(row["year"].strip()),
                "is_ra": to_bool(row.get("is_ra", "false")),
                "accessibility_required": to_bool(row.get("accessibility_required", "false")),
            }
            if semester_id:
                entry["semester_id"] = semester_id
            rows.append(entry)
        except (KeyError, ValueError) as e:
            errors.append({"row": i, "error": str(e)})

    if errors:
        raise HTTPException(status_code=422, detail={"parse_errors": errors})

    sb = get_supabase()
    res = sb.table("students").upsert(
        rows, on_conflict="college_id,semester_id,email"
    ).execute()
    return {"imported": len(res.data), "rows": res.data}


# ── Blocks ────────────────────────────────────────────────────────────────────

@app.get("/blocks")
def list_blocks(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("blocks").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    data = q.execute().data
    data.sort(key=lambda b: _natural_key(b["name"]))
    return data


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
def list_rooms(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("rooms").select("*, blocks(name)").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    data = q.execute().data
    data.sort(key=lambda r: _natural_key(r["room_number"]))
    return data


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
def list_rules(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("rules").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    return q.order("rule_type").execute().data


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
def list_runs(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("allocation_runs").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    return q.order("created_at", desc=True).execute().data


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
    semester_id = data.get("semester_id")

    run_payload: dict = {"college_id": COLLEGE_ID, "cohort": cohort, "status": "running"}
    if semester_id:
        run_payload["semester_id"] = semester_id

    run = sb.table("allocation_runs").insert(run_payload).execute()
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


# ── Data upload / template ────────────────────────────────────────────────────

@app.post("/data/upload")
async def upload_data(
    file: UploadFile = File(...),
    semester_id: str | None = Form(None),
):
    """
    Accept an xlsx with required sheets: students, blocks, wing_leaders
    and an optional rooms sheet.

    Students sheet columns (all except name/email optional):
      name, email, year, male, accessibility_required, small_room,
      friend_request_1..4, enemy_request_1..4, block_request_1..2

    Blocks sheet columns:
      name, block_cap_low, block_cap_up, male_cap_low, male_cap_up, small_room_cap

    Rooms sheet columns (optional — replaces rooms for processed blocks):
      block (name), room_number, floor, room_type, is_accessible, is_available

    Wing leaders sheet columns:
      name  (must match a student name),  block  (must match a block name)
    """
    content = await file.read()
    try:
        xl = pd.ExcelFile(io.BytesIO(content))
    except Exception:
        raise HTTPException(status_code=422, detail="Invalid Excel file — must be .xlsx")

    missing = {"students", "blocks", "wing_leaders"} - set(xl.sheet_names)
    if missing:
        raise HTTPException(
            status_code=422,
            detail=f"Missing required sheets: {', '.join(sorted(missing))}",
        )

    sb = get_supabase()

    # ── 1. Blocks ─────────────────────────────────────────────────────────────
    df_b = xl.parse("blocks")
    df_b.columns = [c.strip().lower().replace(" ", "_") for c in df_b.columns]

    block_rows = []
    for _, row in df_b.iterrows():
        name = _safe_str(row.get("name"))
        if not name:
            continue
        entry = {
            "college_id":    COLLEGE_ID,
            "name":          name,
            "block_cap_low": float(row.get("block_cap_low", 0.3) or 0.3),
            "block_cap_up":  float(row.get("block_cap_up",  0.9) or 0.9),
            "male_cap_low":  float(row.get("male_cap_low",  0.4) or 0.4),
            "male_cap_up":   float(row.get("male_cap_up",   0.6) or 0.6),
            "small_room_cap": _safe_int(row.get("small_room_cap", 0), 0),
        }
        if semester_id:
            entry["semester_id"] = semester_id
        block_rows.append(entry)

    block_name_to_id: dict[str, str] = {}
    if block_rows:
        res_b = sb.table("blocks").upsert(
            block_rows, on_conflict="college_id,semester_id,name"
        ).execute()
        block_name_to_id = {b["name"]: b["id"] for b in res_b.data}

    # ── 2. Rooms (optional sheet) ─────────────────────────────────────────────
    rooms_upserted = 0
    if "rooms" in xl.sheet_names and block_name_to_id:
        df_r = xl.parse("rooms")
        df_r.columns = [c.strip().lower().replace(" ", "_") for c in df_r.columns]

        valid_room_types = {"en-suite", "shared-bathroom", "studio"}

        # Group rows by block so we can delete-then-insert per block
        rooms_by_block: dict[str, list] = {}
        for _, row in df_r.iterrows():
            block_name = _safe_str(row.get("block"))
            room_number = _safe_str(row.get("room_number"))
            if not block_name or not room_number:
                continue
            block_id = block_name_to_id.get(block_name)
            if not block_id:
                continue
            room_type = _safe_str(row.get("room_type")) or "en-suite"
            if room_type not in valid_room_types:
                room_type = "en-suite"
            entry = {
                "college_id":   COLLEGE_ID,
                "block_id":     block_id,
                "room_number":  room_number,
                "floor":        _safe_int(row.get("floor"), 0),
                "room_type":    room_type,
                "is_accessible": bool(_safe_bool(row.get("is_accessible")) or False),
                "is_available":  bool(_safe_bool(row.get("is_available")) if row.get("is_available") is not None else True),
            }
            if semester_id:
                entry["semester_id"] = semester_id
            rooms_by_block.setdefault(block_id, []).append(entry)

        for block_id, room_rows in rooms_by_block.items():
            # Delete existing rooms for this block before re-inserting
            sb.table("rooms").delete().eq("block_id", block_id).execute()
            if room_rows:
                sb.table("rooms").insert(room_rows).execute()
                rooms_upserted += len(room_rows)

    # ── 3. Students ───────────────────────────────────────────────────────────
    df_s = xl.parse("students")
    df_s.columns = [c.strip().lower().replace(" ", "_") for c in df_s.columns]

    student_rows = []
    for _, row in df_s.iterrows():
        name = _safe_str(row.get("name"))
        if not name:
            continue
        email = _safe_str(row.get("email"))
        if not email:
            email = (
                name.lower()
                .replace(" ", ".")
                .replace("'", "")
                + "@christreasurer.upload"
            )
        entry = {
            "college_id":            COLLEGE_ID,
            "name":                  name,
            "email":                 email.lower(),
            "year":                  _safe_int(row.get("year"), 1),
            "is_ra":                 False,  # set by wing_leaders processing below
            "male":                  _safe_bool(row.get("male")),
            "accessibility_required": bool(_safe_bool(row.get("accessibility_required")) or False),
            "small_room":            bool(_safe_bool(row.get("small_room")) or False),
            "friend_request_1":      _safe_str(row.get("friend_request_1")),
            "friend_request_2":      _safe_str(row.get("friend_request_2")),
            "friend_request_3":      _safe_str(row.get("friend_request_3")),
            "friend_request_4":      _safe_str(row.get("friend_request_4")),
            "enemy_request_1":       _safe_str(row.get("enemy_request_1")),
            "enemy_request_2":       _safe_str(row.get("enemy_request_2")),
            "enemy_request_3":       _safe_str(row.get("enemy_request_3")),
            "enemy_request_4":       _safe_str(row.get("enemy_request_4")),
            "block_request_1":       _safe_str(row.get("block_request_1")),
            "block_request_2":       _safe_str(row.get("block_request_2")),
        }
        if semester_id:
            entry["semester_id"] = semester_id
        student_rows.append(entry)

    student_name_to_id: dict[str, str] = {}
    if student_rows:
        res_s = sb.table("students").upsert(
            student_rows, on_conflict="college_id,semester_id,email"
        ).execute()
        student_name_to_id = {s["name"]: s["id"] for s in res_s.data}

    # ── 4. Wing leaders (RA pins) ─────────────────────────────────────────────
    df_l = xl.parse("wing_leaders")
    df_l.columns = [c.strip().lower().replace(" ", "_") for c in df_l.columns]

    ra_count = 0
    for _, row in df_l.iterrows():
        ra_name   = _safe_str(row.get("name"))
        block_name = _safe_str(row.get("block"))
        if not ra_name or not block_name:
            continue
        student_id = student_name_to_id.get(ra_name)
        block_id   = block_name_to_id.get(block_name)
        if student_id and block_id:
            sb.table("students").update({
                "is_ra":       True,
                "ra_block_id": block_id,
            }).eq("id", student_id).execute()
            ra_count += 1

    return {
        "blocks_upserted":   len(block_rows),
        "rooms_upserted":    rooms_upserted,
        "students_upserted": len(student_rows),
        "ras_pinned":        ra_count,
    }


@app.get("/data/template")
def download_template():
    """Return a blank xlsx template with the correct sheet structure."""
    output = io.BytesIO()
    with pd.ExcelWriter(output, engine="openpyxl") as writer:
        pd.DataFrame({
            "name":                  ["Alice Smith"],
            "email":                 ["alice.smith@college.ac.uk"],
            "year":                  [1],
            "male":                  [0],
            "accessibility_required": [0],
            "small_room":            [0],
            "friend_request_1":      ["Bob Jones"],
            "friend_request_2":      [None],
            "friend_request_3":      [None],
            "friend_request_4":      [None],
            "enemy_request_1":       [None],
            "enemy_request_2":       [None],
            "enemy_request_3":       [None],
            "enemy_request_4":       [None],
            "block_request_1":       ["Block A"],
            "block_request_2":       [None],
        }).to_excel(writer, sheet_name="students", index=False)

        pd.DataFrame({
            "name":          ["Block A", "Block B"],
            "block_cap_low": [0.3,       0.3],
            "block_cap_up":  [0.9,       0.9],
            "male_cap_low":  [0.4,       0.4],
            "male_cap_up":   [0.6,       0.6],
            "small_room_cap":[0,         0],
        }).to_excel(writer, sheet_name="blocks", index=False)

        pd.DataFrame({
            "block":         ["Block A", "Block A", "Block A", "Block B"],
            "room_number":   ["A1",      "A2",      "A3",      "B1"],
            "floor":         [0,         0,         1,         0],
            "room_type":     ["en-suite", "shared-bathroom", "en-suite", "studio"],
            "is_accessible": [0,         1,         0,         0],
            "is_available":  [1,         1,         1,         1],
        }).to_excel(writer, sheet_name="rooms", index=False)

        pd.DataFrame({
            "name":  ["Alice Smith"],
            "block": ["Block A"],
        }).to_excel(writer, sheet_name="wing_leaders", index=False)

    output.seek(0)
    return StreamingResponse(
        output,
        media_type=(
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
        ),
        headers={
            "Content-Disposition": "attachment; filename=christreasurer_template.xlsx"
        },
    )
