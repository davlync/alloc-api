import csv
import io
import math
import os
import random
import re
import traceback
from datetime import datetime, timezone

import jwt
import pandas as pd
from fastapi import APIRouter, BackgroundTasks, Depends, FastAPI, Header, HTTPException, UploadFile, File, Query, Form
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
SUPABASE_JWT_SECRET = os.environ.get("SUPABASE_JWT_SECRET", "")


def verify_token(authorization: str = Header(...)):
    if not authorization.startswith("Bearer "):
        raise HTTPException(status_code=401, detail="Missing bearer token")
    token = authorization[len("Bearer "):]
    try:
        jwt.decode(token, SUPABASE_JWT_SECRET, algorithms=["HS256"], audience="authenticated")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid or expired token")


router = APIRouter(dependencies=[Depends(verify_token)])


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

@router.get("/semesters")
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


@router.post("/semesters")
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


@router.delete("/semesters/{semester_id}")
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

@router.get("/students")
def list_students(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("students").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    return q.order("name").execute().data


@router.post("/students")
def create_student(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("students").insert(data).execute()
    return res.data[0]


@router.put("/students/{student_id}")
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


@router.delete("/students/{student_id}")
def delete_student(student_id: str):
    sb = get_supabase()
    sb.table("students").delete().eq("id", student_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": student_id}


@router.post("/students/import")
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

@router.get("/blocks")
def list_blocks(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("blocks").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    data = q.execute().data
    data.sort(key=lambda b: _natural_key(b["name"]))
    return data


@router.post("/blocks")
def create_block(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("blocks").insert(data).execute()
    return res.data[0]


@router.put("/blocks/{block_id}")
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


@router.delete("/blocks/{block_id}")
def delete_block(block_id: str):
    sb = get_supabase()
    sb.table("blocks").delete().eq("id", block_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": block_id}


# ── Rooms ─────────────────────────────────────────────────────────────────────

@router.get("/rooms")
def list_rooms(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("rooms").select("*, blocks(name)").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    data = q.execute().data
    data.sort(key=lambda r: _natural_key(r["room_number"]))
    return data


@router.post("/rooms")
def create_room(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("rooms").insert(data).execute()
    return res.data[0]


@router.put("/rooms/{room_id}")
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


@router.delete("/rooms/{room_id}")
def delete_room(room_id: str):
    sb = get_supabase()
    sb.table("rooms").delete().eq("id", room_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": room_id}


# ── Rules ─────────────────────────────────────────────────────────────────────

@router.get("/rules")
def list_rules(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("rules").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    return q.order("rule_type").execute().data


@router.post("/rules")
def create_rule(data: dict):
    sb = get_supabase()
    data["college_id"] = COLLEGE_ID
    res = sb.table("rules").insert(data).execute()
    return res.data[0]


@router.put("/rules/{rule_id}")
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


@router.delete("/rules/{rule_id}")
def delete_rule(rule_id: str):
    sb = get_supabase()
    sb.table("rules").delete().eq("id", rule_id).eq("college_id", COLLEGE_ID).execute()
    return {"deleted": rule_id}


# ── Allocation runs ───────────────────────────────────────────────────────────

@router.get("/runs")
def list_runs(semester_id: str | None = Query(None)):
    sb = get_supabase()
    q = sb.table("allocation_runs").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q = q.eq("semester_id", semester_id)
    return q.order("created_at", desc=True).execute().data


@router.get("/runs/{run_id}")
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


@router.get("/runs/{run_id}/status")
def get_run_status(run_id: str):
    """Lightweight poll endpoint — returns run row only, no allocations join."""
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
    return run.data


# ── Diagnostics ───────────────────────────────────────────────────────────────

@router.get("/diagnose")
def diagnose(
    semester_id: str | None = Query(None),
    cohort: str = Query("first-years"),
):
    """
    Pre-flight check for allocation feasibility.
    Returns a structured report and prints the same to stdout (Railway logs).
    """
    sb = get_supabase()
    issues:   list[str] = []
    warnings: list[str] = []

    # ── Blocks ────────────────────────────────────────────────────────────────
    q_blocks = sb.table("blocks").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q_blocks = q_blocks.eq("semester_id", semester_id)
    else:
        q_blocks = q_blocks.is_("semester_id", "null")
    blocks_data = q_blocks.execute().data

    if not blocks_data:
        issues.append("BLOCKS: no blocks found for this semester — upload data first")

    # ── Rooms ─────────────────────────────────────────────────────────────────
    block_uuids = [b["id"] for b in blocks_data]
    rooms_data  = (
        sb.table("rooms").select("*")
        .eq("college_id", COLLEGE_ID)
        .in_("block_id", block_uuids)
        .eq("is_available", True)
        .execute().data
    ) if block_uuids else []

    rooms_by_block: dict[str, list] = {b["id"]: [] for b in blocks_data}
    for r in rooms_data:
        if r["block_id"] in rooms_by_block:
            rooms_by_block[r["block_id"]].append(r)

    block_report = []
    total_capacity = 0
    for b in blocks_data:
        cap      = len(rooms_by_block.get(b["id"], []))
        cap_low  = float(b.get("block_cap_low") or 0.3)
        cap_up   = float(b.get("block_cap_up")  or 0.9)
        m_low    = float(b.get("male_cap_low")  or 0.4)
        m_up     = float(b.get("male_cap_up")   or 0.6)
        sm_cap   = int(b.get("small_room_cap")  or 0)
        total_capacity += cap
        entry = {
            "name":           b["name"],
            "capacity":       cap,
            "block_cap_low":  cap_low,
            "block_cap_up":   cap_up,
            "male_cap_low":   m_low,
            "male_cap_up":    m_up,
            "small_room_cap": sm_cap,
        }
        if cap == 0:
            entry["issue"] = "ZERO ROOMS — block cannot accept any student"
            issues.append(f"BLOCK '{b['name']}': 0 rooms (capacity=0)")
        if cap_up < 0.5:
            entry["warning"] = f"block_cap_up={cap_up} looks very low (expected ~0.9)"
            warnings.append(f"BLOCK '{b['name']}': block_cap_up={cap_up} suspiciously low")
        block_report.append(entry)

    # ── Students ──────────────────────────────────────────────────────────────
    q_students = sb.table("students").select(
        "id, name, year, is_ra, male, small_room, accessibility_required, ra_block_id, "
        "friend_request_1, friend_request_2, block_request_1, block_request_2"
    ).eq("college_id", COLLEGE_ID)
    if semester_id:
        q_students = q_students.eq("semester_id", semester_id)
    else:
        q_students = q_students.is_("semester_id", "null")
    all_students = q_students.execute().data

    cohort_students = [s for s in all_students if cohort != "first-years" or s.get("year") == 1]
    n_students  = len(cohort_students)
    n_males     = sum(1 for s in cohort_students if s.get("male"))
    n_ras       = sum(1 for s in cohort_students if s.get("is_ra"))
    n_small     = sum(1 for s in cohort_students if s.get("small_room"))
    n_access    = sum(1 for s in cohort_students if s.get("accessibility_required"))
    male_ratio  = round(n_males / n_students, 3) if n_students else 0

    if n_students == 0:
        issues.append(f"STUDENTS: no {cohort} students found for this semester")

    # ── Capacity feasibility ──────────────────────────────────────────────────
    if n_students > 0 and total_capacity > 0:
        if total_capacity < n_students:
            issues.append(
                f"CAPACITY: total rooms ({total_capacity}) < students ({n_students}) "
                f"— {n_students - total_capacity} students cannot be placed"
            )
        elif total_capacity < n_students * 1.05:
            warnings.append(
                f"CAPACITY: tight — only {total_capacity - n_students} spare rooms "
                f"for {n_students} students"
            )

    # ── Gender feasibility ────────────────────────────────────────────────────
    gender_enabled = male_ratio >= 0.1
    if gender_enabled:
        for b in blocks_data:
            m_low = float(b.get("male_cap_low") or 0.4)
            m_up  = float(b.get("male_cap_up")  or 0.6)
            if male_ratio < m_low:
                issues.append(
                    f"GENDER: male ratio {male_ratio:.0%} is below male_cap_low "
                    f"{m_low:.0%} on block '{b['name']}' — hard constraint will be infeasible"
                )
            if male_ratio > m_up:
                issues.append(
                    f"GENDER: male ratio {male_ratio:.0%} exceeds male_cap_up "
                    f"{m_up:.0%} on block '{b['name']}' — hard constraint will be infeasible"
                )
    else:
        warnings.append(
            f"GENDER: only {n_males}/{n_students} students have male=true ({male_ratio:.0%}) "
            f"— gender constraints will be disabled at runtime"
        )

    # ── RA pins ───────────────────────────────────────────────────────────────
    ra_pin_report = []
    block_id_to_name = {b["id"]: b["name"] for b in blocks_data}
    for s in cohort_students:
        if s.get("is_ra"):
            ra_block_id = s.get("ra_block_id")
            if not ra_block_id:
                warnings.append(f"RA '{s['name']}': no ra_block_id set — will not be pinned")
                ra_pin_report.append({"ra": s["name"], "block": None, "issue": "no block assigned"})
            elif ra_block_id not in block_id_to_name:
                issues.append(
                    f"RA '{s['name']}': ra_block_id points to a block not in this semester"
                )
                ra_pin_report.append({"ra": s["name"], "block": ra_block_id, "issue": "block not in semester"})
            else:
                block_cap = len(rooms_by_block.get(ra_block_id, []))
                ra_pin_report.append({"ra": s["name"], "block": block_id_to_name[ra_block_id], "capacity": block_cap})

    # ── Small room check ──────────────────────────────────────────────────────
    total_small_cap = sum(b.get("small_room_cap") or 0 for b in blocks_data)
    if n_small > total_small_cap:
        warnings.append(
            f"SMALL ROOMS: {n_small} students want small rooms but total small_room_cap "
            f"across all blocks is {total_small_cap} — soft slack will absorb this"
        )

    # ── Friend/block reference integrity ──────────────────────────────────────
    student_names = {s["name"].strip().lower() for s in cohort_students}
    block_names   = {b["name"].strip().lower() for b in blocks_data}
    n_bad_friends = 0
    n_bad_blocks  = 0
    for s in cohort_students:
        for k in range(1, 5):
            fv = s.get(f"friend_request_{k}")
            if fv and fv.strip().lower() not in student_names:
                n_bad_friends += 1
        for k in range(1, 3):
            bv = s.get(f"block_request_{k}")
            if bv and bv.strip().lower() not in block_names:
                n_bad_blocks += 1
    if n_bad_friends:
        warnings.append(f"PREFS: {n_bad_friends} friend_request values don't match any student name in cohort")
    if n_bad_blocks:
        warnings.append(f"PREFS: {n_bad_blocks} block_request values don't match any block name in semester")

    # ── Summary ───────────────────────────────────────────────────────────────
    feasible = len(issues) == 0
    report = {
        "feasible":       feasible,
        "issues":         issues,
        "warnings":       warnings,
        "summary": {
            "semester_id":     semester_id,
            "cohort":          cohort,
            "n_students":      n_students,
            "n_all_students":  len(all_students),
            "n_males":         n_males,
            "male_ratio":      male_ratio,
            "gender_constraints_enabled": gender_enabled,
            "total_capacity":  total_capacity,
            "capacity_slack":  total_capacity - n_students,
            "n_ras":           n_ras,
            "n_small_room":    n_small,
            "n_accessibility": n_access,
            "n_blocks":        len(blocks_data),
            "n_rooms":         len(rooms_data),
        },
        "blocks": block_report,
        "ra_pins": ra_pin_report,
    }

    # Print to stdout for Railway logs
    print("\n" + "="*60)
    print(f"DIAGNOSE  semester={semester_id}  cohort={cohort}")
    print("="*60)
    print(f"  Students  : {n_students} ({cohort})  |  all in semester: {len(all_students)}")
    print(f"  Capacity  : {total_capacity} rooms across {len(blocks_data)} blocks  |  slack: {total_capacity - n_students}")
    print(f"  Gender    : {n_males}/{n_students} male ({male_ratio:.0%})  constraints={'ON' if gender_enabled else 'OFF'}")
    print(f"  RAs       : {n_ras}  |  small_room: {n_small}  |  accessibility: {n_access}")
    print()
    for b in block_report:
        flag = "  *** " + b.get("issue", b.get("warning", "")) if ("issue" in b or "warning" in b) else ""
        print(f"  {b['name']:20s}  cap={b['capacity']:3d}  "
              f"low={b['block_cap_low']:.2f} up={b['block_cap_up']:.2f}  "
              f"m_low={b['male_cap_low']:.2f} m_up={b['male_cap_up']:.2f}  "
              f"sm={b['small_room_cap']}{flag}")
    if issues:
        print("\n  ISSUES (will cause infeasibility):")
        for i in issues:
            print(f"    ✗ {i}")
    if warnings:
        print("\n  WARNINGS:")
        for w in warnings:
            print(f"    ⚠ {w}")
    print(f"\n  VERDICT: {'FEASIBLE ✓' if feasible else 'LIKELY INFEASIBLE ✗'}")
    print("="*60 + "\n")

    return report


# ── Allocation algorithm helpers ──────────────────────────────────────────────

def _build_allocation_dataframes(sb, cohort: str, semester_id: str | None):
    """Pull students, blocks, rooms from DB and build algorithm DataFrames."""

    # Blocks
    q_blocks = sb.table("blocks").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q_blocks = q_blocks.eq("semester_id", semester_id)
    else:
        q_blocks = q_blocks.is_("semester_id", "null")
    blocks_data = q_blocks.execute().data
    if not blocks_data:
        raise ValueError("No blocks found for this semester")

    block_uuids = [b["id"] for b in blocks_data]

    # Rooms grouped by block (only available rooms count as capacity)
    rooms_data = (
        sb.table("rooms")
        .select("*")
        .eq("college_id", COLLEGE_ID)
        .in_("block_id", block_uuids)
        .eq("is_available", True)
        .execute()
        .data
    )
    rooms_by_block: dict[str, list] = {b["id"]: [] for b in blocks_data}
    for room in rooms_data:
        bid = room["block_id"]
        if bid in rooms_by_block:
            rooms_by_block[bid].append(room)

    # Students
    q_students = sb.table("students").select("*").eq("college_id", COLLEGE_ID)
    if semester_id:
        q_students = q_students.eq("semester_id", semester_id)
    else:
        q_students = q_students.is_("semester_id", "null")
    if cohort == "first-years":
        q_students = q_students.eq("year", 1)
    students_data = q_students.execute().data
    if not students_data:
        raise ValueError("No students found for this cohort/semester")

    # UUID ↔ int mappings (sequential integers)
    student_uuid_to_int = {s["id"]: i for i, s in enumerate(students_data)}
    int_to_student_uuid = {i: s["id"] for i, s in enumerate(students_data)}
    block_uuid_to_int   = {b["id"]: i for i, b in enumerate(blocks_data)}
    int_to_block_uuid   = {i: b["id"] for i, b in enumerate(blocks_data)}

    # Name → UUID lookups (friend/block requests stored as names in DB)
    student_name_to_uuid: dict[str, str] = {
        s["name"].strip().lower(): s["id"] for s in students_data
    }
    block_name_to_uuid: dict[str, str] = {
        b["name"].strip().lower(): b["id"] for b in blocks_data
    }

    def _resolve_friend(name_val) -> float:
        if name_val is None:
            return float("nan")
        if isinstance(name_val, float) and math.isnan(name_val):
            return float("nan")
        uuid = student_name_to_uuid.get(str(name_val).strip().lower())
        if uuid is None:
            return float("nan")
        idx = student_uuid_to_int.get(uuid)
        return float(idx) if idx is not None else float("nan")

    def _resolve_block(name_val) -> float:
        if name_val is None:
            return float("nan")
        if isinstance(name_val, float) and math.isnan(name_val):
            return float("nan")
        uuid = block_name_to_uuid.get(str(name_val).strip().lower())
        if uuid is None:
            return float("nan")
        idx = block_uuid_to_int.get(uuid)
        return float(idx) if idx is not None else float("nan")

    # Build df_prefs
    prefs_rows = []
    for s in students_data:
        is_ra = bool(s.get("is_ra"))
        row: dict = {
            "student":        student_uuid_to_int[s["id"]],
            "male":           1 if s.get("male") else 0,
            "community_mult": 0.0 if is_ra else float(s.get("community_mult") or 0.1),
            "small_room":     1 if s.get("small_room") else 0,
        }
        for k in range(1, 5):
            row[f"friend_request_{k}"] = _resolve_friend(s.get(f"friend_request_{k}"))
            row[f"enemy_request_{k}"]  = _resolve_friend(s.get(f"enemy_request_{k}"))
        for k in range(1, 3):
            row[f"block_request_{k}"] = _resolve_block(s.get(f"block_request_{k}"))
        prefs_rows.append(row)
    df_prefs = pd.DataFrame(prefs_rows)

    # _assign_pref_weights expects block_request_1..4; DB only has 1..2
    for k in (3, 4):
        df_prefs[f"block_request_{k}"] = float("nan")

    # Gender ratio: disable bounds if data is sparse.
    # When RA pins are present, widen bounds by 0.15 per side to absorb the
    # degrees-of-freedom lost to pinning (RA pins are hard and can push individual
    # blocks outside tight bounds, making the MIP infeasible).
    n_male = sum(1 for s in students_data if s.get("male"))
    n_ra   = sum(1 for s in students_data if s.get("is_ra") and s.get("ra_block_id"))
    gender_data_available = n_male / len(students_data) >= 0.1 if students_data else False
    # Extra tolerance when many hard RA pins reduce the solver's gender flexibility
    gender_tolerance = min(0.15, 0.05 * n_ra / max(len(blocks_data), 1)) if n_ra > 0 else 0.0
    print(f"  [run] {n_male}/{len(students_data)} students have male=true; "
          f"gender constraints {'enabled' if gender_data_available else 'DISABLED (sparse data)'}; "
          f"ra_pins={n_ra}  gender_tolerance={gender_tolerance:.3f}")

    # Build df_info
    info_rows = []
    for b in blocks_data:
        n_rooms = len(rooms_by_block.get(b["id"], []))
        if gender_data_available:
            m_low = max(0.0, float(b.get("male_cap_low") or 0.4) - gender_tolerance)
            m_up  = min(1.0, float(b.get("male_cap_up")  or 0.6) + gender_tolerance)
        else:
            m_low, m_up = 0.0, 1.0
        info_rows.append({
            "block":         block_uuid_to_int[b["id"]],
            "capacity":      n_rooms,
            "block_cap_low": float(b.get("block_cap_low") or 0.3),
            "block_cap_up":  float(b.get("block_cap_up")  or 0.9),
            "male_cap_low":  m_low,
            "male_cap_up":   m_up,
            "small_room_cap":int(b.get("small_room_cap")  or 0),
        })
    df_info = pd.DataFrame(info_rows)

    # Build df_ra (RAs with an assigned block)
    ra_rows = []
    for s in students_data:
        if s.get("is_ra") and s.get("ra_block_id"):
            ra_block_uuid = s["ra_block_id"]
            if ra_block_uuid in block_uuid_to_int:
                ra_rows.append({
                    "ra":    student_uuid_to_int[s["id"]],
                    "block": block_uuid_to_int[ra_block_uuid],
                })
    df_ra = pd.DataFrame(ra_rows if ra_rows else [{"ra": 0, "block": 0}])
    df_ra = df_ra.astype(int)
    if not ra_rows:
        df_ra = df_ra.iloc[0:0]  # empty with correct dtypes

    return (
        df_prefs, df_info, df_ra,
        students_data, blocks_data, rooms_by_block,
        student_uuid_to_int, int_to_student_uuid,
        block_uuid_to_int, int_to_block_uuid,
    )


def _assign_rooms(
    alloc_int: dict,
    int_to_student_uuid: dict,
    int_to_block_uuid: dict,
    rooms_by_block: dict,
    students_data: list,
) -> list[tuple]:
    """
    Translate int alloc → UUIDs, then pick a room per student in their block.
    Prioritises accessible rooms for accessibility_required students, and
    shared-bathroom rooms for small_room students.  Otherwise random.
    Returns list of (student_uuid, room_uuid, is_flagged, flag_reason).
    """
    student_by_uuid = {s["id"]: s for s in students_data}

    # Per-block mutable list of available rooms (we pop as we assign)
    available: dict[str, list] = {}
    for block_uuid, rooms in rooms_by_block.items():
        available[block_uuid] = list(rooms)  # copy so we can pop

    assignments = []
    for student_int, block_int in alloc_int.items():
        student_uuid = int_to_student_uuid[student_int]
        block_uuid   = int_to_block_uuid[block_int]
        student      = student_by_uuid[student_uuid]

        room_pool = available.get(block_uuid, [])
        if not room_pool:
            assignments.append((student_uuid, None, True, "No available room in assigned block"))
            continue

        wants_accessible = bool(student.get("accessibility_required"))
        wants_small      = bool(student.get("small_room"))

        def _priority(r):
            score = 0
            if wants_accessible and r.get("is_accessible"):
                score += 4
            if wants_small and r.get("room_type") == "shared-bathroom":
                score += 2
            score += random.random()  # tiebreak
            return -score  # sort ascending (highest priority first)

        room_pool.sort(key=_priority)
        chosen = room_pool.pop(0)

        is_flagged  = wants_accessible and not chosen.get("is_accessible")
        flag_reason = "No accessible room available in assigned block" if is_flagged else None
        assignments.append((student_uuid, chosen["id"], is_flagged, flag_reason))

    return assignments


def _compute_run_stats(alloc_int: dict, df_prefs: pd.DataFrame, students_data: list) -> dict:
    """Compute stats dict for the allocation run."""
    n_students = len(students_data)
    n_assigned  = len(alloc_int)

    # Friend preferences
    # friend_any_matched_pct : % of students (with ≥1 request) who got ≥1 friend in same block
    # friend_score_pct       : weighted % — rank-1 match worth 4pts, rank-2=3, rank-3=2, rank-4=1
    RANK_WEIGHTS = {1: 4, 2: 3, 3: 2, 4: 1}
    n_with_friends       = 0
    n_any_matched        = 0
    total_weighted_max   = 0
    total_weighted_score = 0
    for _, row in df_prefs.iterrows():
        si = int(row["student"])
        my_block = alloc_int.get(si)
        if my_block is None:
            continue
        reqs = {}
        for k in range(1, 5):
            fv = row.get(f"friend_request_{k}")
            if not pd.isna(fv):
                reqs[k] = int(fv)
        if not reqs:
            continue
        n_with_friends += 1
        any_match = False
        for k, fid in reqs.items():
            w = RANK_WEIGHTS[k]
            total_weighted_max += w
            if alloc_int.get(fid) == my_block:
                total_weighted_score += w
                any_match = True
        if any_match:
            n_any_matched += 1

    friend_any_pct   = round(n_any_matched / n_with_friends * 100)   if n_with_friends > 0 else 100
    friend_score_pct = round(total_weighted_score / total_weighted_max * 100) if total_weighted_max > 0 else 100

    # Block preferences matched
    total_block_prefs = 0
    block_pref_met    = 0
    for _, row in df_prefs.iterrows():
        si = int(row["student"])
        my_block = alloc_int.get(si)
        if my_block is None:
            continue
        if float(row.get("community_mult", 0.1)) < 0.01:
            continue  # RA — skip
        for k in range(1, 3):
            bv = row.get(f"block_request_{k}")
            if pd.isna(bv):
                continue
            total_block_prefs += 1
            if int(bv) == my_block:
                block_pref_met += 1

    block_pct = round(block_pref_met / total_block_prefs * 100) if total_block_prefs > 0 else 100

    # Hard constraint violations — students with preferences but none satisfied
    n_no_pref_met = 0
    violating_student_ints: list[int] = []
    for _, row in df_prefs.iterrows():
        si = int(row["student"])
        my_block = alloc_int.get(si)
        if my_block is None:
            continue
        cm = float(row.get("community_mult", 0.1))
        if cm < 0.01:
            continue
        has_prefs = False
        any_met   = False
        for k in range(1, 5):
            fv = row.get(f"friend_request_{k}")
            if not pd.isna(fv):
                has_prefs = True
                if alloc_int.get(int(fv)) == my_block:
                    any_met = True
        for k in range(1, 3):
            bv = row.get(f"block_request_{k}")
            if not pd.isna(bv):
                has_prefs = True
                if int(bv) == my_block:
                    any_met = True
        if has_prefs and not any_met:
            n_no_pref_met += 1
            violating_student_ints.append(si)

    return {
        "students_assigned_pct":          round(n_assigned / n_students * 100) if n_students > 0 else 0,
        "friend_any_matched_pct":         friend_any_pct,
        "friend_score_pct":               friend_score_pct,
        "room_type_preferences_met_pct":  0,  # room-level preference tracking not yet implemented
        "block_preferences_met_pct":      block_pct,
        "hard_constraint_violations":     n_no_pref_met,
        "_violating_student_ints":        violating_student_ints,  # popped before DB write
    }


def _run_allocation_task(run_id: str, cohort: str, semester_id: str | None, time_limit: int):
    """Background task: run the SCIP/LNS allocation and write results to DB."""
    sb = get_supabase()
    try:
        (
            df_prefs, df_info, df_ra,
            students_data, blocks_data, rooms_by_block,
            student_uuid_to_int, int_to_student_uuid,
            block_uuid_to_int, int_to_block_uuid,
        ) = _build_allocation_dataframes(sb, cohort, semester_id)

        # ── Diagnostic summary ────────────────────────────────────────────────
        total_capacity = int(df_info["capacity"].sum())
        n_students     = len(df_prefs)
        n_males        = int(df_prefs["male"].sum())
        n_ras          = len(df_ra)
        print(f"\n[run diagnostic]")
        print(f"  students  : {n_students}  (cohort={cohort})")
        print(f"  blocks    : {len(df_info)}  total_capacity={total_capacity}")
        print(f"  males     : {n_males}/{n_students}")
        print(f"  RAs       : {n_ras}")
        for _, row in df_info.iterrows():
            print(f"  block {int(row['block'])}: cap={int(row['capacity'])}  "
                  f"cap_low={row['block_cap_low']:.2f}  cap_up={row['block_cap_up']:.2f}  "
                  f"male_low={row['male_cap_low']:.2f}  male_up={row['male_cap_up']:.2f}  "
                  f"small_cap={int(row['small_room_cap'])}")
        if total_capacity < n_students:
            print(f"  *** INFEASIBLE: total_capacity ({total_capacity}) < n_students ({n_students})")
        print()

        # Assign preference weight columns expected by the solver
        from algorithm.room_allocator import _assign_pref_weights
        _assign_pref_weights(df_prefs)

        # Run LNS solver
        from algorithm.lns import lns_solve
        result = lns_solve(
            df_prefs, df_info, df_ra,
            time_limit=time_limit,
            solver_name="SCIP",
            verbose=False,
        )

        alloc_int: dict = result.get("alloc", {})

        # Assign rooms
        assignments = _assign_rooms(
            alloc_int, int_to_student_uuid, int_to_block_uuid,
            rooms_by_block, students_data,
        )

        # ── Per-student quality fields ────────────────────────────────────────
        _RW = {1: 4, 2: 3, 3: 2, 4: 1}
        student_uuid_to_quality: dict[str, dict] = {}
        for _, row in df_prefs.iterrows():
            si       = int(row["student"])
            my_block = alloc_int.get(si)
            if my_block is None:
                continue
            s_uuid_q = int_to_student_uuid.get(si)
            if not s_uuid_q:
                continue

            # Block match: None = no pref, True/False = pref existed
            block_reqs = [row.get(f"block_request_{k}") for k in (1, 2)]
            block_reqs = [int(v) for v in block_reqs if not pd.isna(v)]
            if block_reqs:
                block_matched_q = my_block in block_reqs
            else:
                block_matched_q = None

            # Friend score: None = no prefs, 0-100 otherwise
            max_w = achieved_w = 0
            for k in range(1, 5):
                fv = row.get(f"friend_request_{k}")
                if not pd.isna(fv):
                    w = _RW[k]
                    max_w += w
                    if alloc_int.get(int(fv)) == my_block:
                        achieved_w += w
            friend_score_q = round(achieved_w / max_w * 100) if max_w > 0 else None

            student_uuid_to_quality[s_uuid_q] = {
                "block_matched":    block_matched_q,
                "friend_score_pct": friend_score_q,
            }

        # Insert allocation rows
        alloc_rows = [
            {
                "run_id":           run_id,
                "student_id":       s_uuid,
                "room_id":          r_uuid,
                "is_flagged":       is_flagged,
                "flag_reason":      flag_reason,
                "block_matched":    student_uuid_to_quality.get(s_uuid, {}).get("block_matched"),
                "friend_score_pct": student_uuid_to_quality.get(s_uuid, {}).get("friend_score_pct"),
            }
            for s_uuid, r_uuid, is_flagged, flag_reason in assignments
            if r_uuid is not None
        ]
        if alloc_rows:
            sb.table("allocations").insert(alloc_rows).execute()

        # Compute stats and warnings
        stats    = _compute_run_stats(alloc_int, df_prefs, students_data)
        violating_ints: list[int] = stats.pop("_violating_student_ints", [])
        warnings: list[str] = []
        n_unassigned = len(students_data) - len(alloc_int)
        if n_unassigned > 0:
            warnings.append(f"{n_unassigned} student(s) could not be assigned a block")
        n_flagged = sum(1 for _, _, f, _ in assignments if f)
        if n_flagged:
            warnings.append(f"{n_flagged} student(s) have allocation flags (see Results page)")
        if violating_ints:
            student_int_to_name = {
                student_uuid_to_int[s["id"]]: s["name"]
                for s in students_data
                if s["id"] in student_uuid_to_int
            }
            names = [student_int_to_name.get(si, f"student #{si}") for si in violating_ints]
            warnings.append(
                f"{len(names)} student(s) had preferences but none were satisfied: "
                + ", ".join(names)
            )

        # Gender rule breach check — compare actual male ratio against the original (unrelaxed)
        # configured bounds from blocks_data, not the solver's widened tolerance bounds.
        male_bin_map = {int(row["student"]): int(row["male"]) for _, row in df_prefs.iterrows()
                        if not pd.isna(row.get("male"))}
        for b in blocks_data:
            if b["id"] not in block_uuid_to_int:
                continue
            bint  = block_uuid_to_int[b["id"]]
            assigned_ints = [si for si, bi in alloc_int.items() if bi == bint]
            if not assigned_ints:
                continue
            n_m   = sum(male_bin_map.get(si, 0) for si in assigned_ints)
            ratio = n_m / len(assigned_ints)
            low   = float(b.get("male_cap_low") or 0.4)
            up    = float(b.get("male_cap_up")  or 0.6)
            bname = b["name"]
            if ratio < low:
                warnings.append(
                    f"Gender rule breach — {bname}: {ratio:.0%} male "
                    f"(min {low:.0%}, {n_m}/{len(assigned_ints)} students)"
                )
            elif ratio > up:
                warnings.append(
                    f"Gender rule breach — {bname}: {ratio:.0%} male "
                    f"(max {up:.0%}, {n_m}/{len(assigned_ints)} students)"
                )

        sb.table("allocation_runs").update({
            "status":       "complete",
            "stats":        stats,
            "warnings":     warnings,
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", run_id).execute()

    except Exception as exc:
        traceback.print_exc()
        sb.table("allocation_runs").update({
            "status":       "failed",
            "warnings":     [str(exc)],
            "completed_at": datetime.now(timezone.utc).isoformat(),
        }).eq("id", run_id).execute()


@router.post("/run")
def run_allocation(data: dict, background_tasks: BackgroundTasks):
    sb = get_supabase()
    cohort      = data.get("cohort", "first-years")
    semester_id = data.get("semester_id")
    time_limit  = int(data.get("time_limit", 30))

    run_payload: dict = {"college_id": COLLEGE_ID, "cohort": cohort, "status": "running"}
    if semester_id:
        run_payload["semester_id"] = semester_id

    run    = sb.table("allocation_runs").insert(run_payload).execute()
    run_id = run.data[0]["id"]

    background_tasks.add_task(_run_allocation_task, run_id, cohort, semester_id, time_limit)

    return {"run_id": run_id, "status": "running"}


# ── Data upload / template ────────────────────────────────────────────────────

@router.post("/data/upload")
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
      name, block_cap_low, block_cap_up, male_cap_low, male_cap_up, small_room_cap,
      num_rooms (optional — auto-generates rooms if no rooms sheet),
      default_room_type (optional — en-suite/shared-bathroom/studio, default en-suite)

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

    valid_room_types = {"en-suite", "shared-bathroom", "studio"}

    # ── 1. Blocks ─────────────────────────────────────────────────────────────
    df_b = xl.parse("blocks")
    df_b.columns = [c.strip().lower().replace(" ", "_") for c in df_b.columns]

    block_rows = []
    # Track num_rooms / default_room_type per block name for auto-generation
    block_auto_rooms: dict[str, tuple[int, str]] = {}
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

        num_rooms = _safe_int(row.get("num_rooms", 0), 0)
        if num_rooms > 0:
            default_type = _safe_str(row.get("default_room_type")) or "en-suite"
            if default_type not in valid_room_types:
                default_type = "en-suite"
            block_auto_rooms[name] = (num_rooms, default_type)

    block_name_to_id: dict[str, str] = {}
    if block_rows:
        res_b = sb.table("blocks").upsert(
            block_rows, on_conflict="college_id,semester_id,name"
        ).execute()
        block_name_to_id = {b["name"]: b["id"] for b in res_b.data}

    # ── 2. Rooms (auto-generated from num_rooms, overridden by explicit sheet) ──
    rooms_upserted = 0

    def _make_room(block_id: str, room_number: str, floor: int,
                   room_type: str, accessible: bool) -> dict:
        entry = {
            "college_id":    COLLEGE_ID,
            "block_id":      block_id,
            "room_number":   room_number,
            "floor":         floor,
            "room_type":     room_type,
            "is_accessible": accessible,
            "is_available":  True,
        }
        if semester_id:
            entry["semester_id"] = semester_id
        return entry

    # Start with auto-generated rooms (from num_rooms column on blocks sheet)
    rooms_by_block: dict[str, list] = {}
    for block_name, (num_rooms, default_type) in block_auto_rooms.items():
        block_id = block_name_to_id.get(block_name)
        if not block_id:
            continue
        m = re.search(r"\d+", block_name)
        prefix = m.group() if m else re.sub(r"\s+", "", block_name)[:3].upper()
        rooms_by_block[block_id] = [
            _make_room(block_id, f"{prefix}-{i}", 0, default_type, False)
            for i in range(1, num_rooms + 1)
        ]

    # Explicit rooms sheet wins per block — replaces any auto-generated rooms
    if "rooms" in xl.sheet_names and block_name_to_id:
        df_r = xl.parse("rooms")
        df_r.columns = [c.strip().lower().replace(" ", "_") for c in df_r.columns]
        explicit_by_block: dict[str, list] = {}
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
            explicit_by_block.setdefault(block_id, []).append(
                _make_room(
                    block_id, room_number,
                    _safe_int(row.get("floor"), 0),
                    room_type,
                    bool(_safe_bool(row.get("is_accessible")) or False),
                )
            )
        rooms_by_block.update(explicit_by_block)  # explicit overrides per block

    if rooms_by_block:
        for block_id, room_rows in rooms_by_block.items():
            sb.table("rooms").delete().eq("block_id", block_id).execute()
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


@router.get("/data/template")
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
            "name":              ["Block A", "Block B"],
            "block_cap_low":     [0.3,       0.3],
            "block_cap_up":      [0.9,       0.9],
            "male_cap_low":      [0.4,       0.4],
            "male_cap_up":       [0.6,       0.6],
            "small_room_cap":    [0,         0],
            "num_rooms":         [20,        15],
            "default_room_type": ["en-suite", "shared-bathroom"],
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


app.include_router(router)
