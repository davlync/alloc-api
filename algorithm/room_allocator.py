"""
room_allocator.py
Python/PuLP port of room_allocation_app.jl

Matches the Julia model exactly:
  - Same objective function and penalty weights
  - Same constraints (capacity, gender ratio, small rooms, RA pins,
    friend/block preferences, "at least one preference met")
  - Enemy preferences and RA support-person pins commented out,
    matching current Julia behaviour

Requirements:
    pip install pulp openpyxl pandas

Usage:
    python room_allocator.py path/to/data.xlsx [options]

    Options:
      --time-limit INT    Solver time limit in seconds (default: 3600)
      --solver NAME       CBC (default) or SCIP
      --output-dir PATH   Where to write output files (default: Output/<stem>)
      --quiet             Suppress solver log
"""

import os
import sys
import time
import math
import argparse
import pandas as pd
import numpy as np

try:
    import pulp
except ImportError:
    sys.exit("PuLP not found. Install with:  pip install pulp")

# ─────────────────────────────────────────────────────────────
# Preference weight tables  (mirrors assign_pref_weights())
# ─────────────────────────────────────────────────────────────
#
# Points awarded for the k-th ranked preference being satisfied,
# given that the student listed n preferences in total.
#
#          rank:   1   2   3   4
POINTS = {
    4:          [ 4,  3,  2,  1],
    3:          [ 5,  3,  2,  0],
    2:          [ 6,  4,  0,  0],
    1:          [10,  0,  0,  0],
    0:          [ 0,  0,  0,  0],
}

# Penalty weights (match Julia objective exactly)
W_PREF_SLACK  = 20
W_CAP_SLACK   = 40
W_SMALL_SLACK = 40


# ─────────────────────────────────────────────────────────────
# Data loading  (mirrors create_frame())
# ─────────────────────────────────────────────────────────────

def load_data(path: str):
    """
    Read and validate data from the xlsx file.
    Returns (df_prefs, df_info, df_ra) with preference weight
    columns already computed.
    """
    print(f"Reading {path} ...")
    df_prefs = pd.read_excel(path, sheet_name="preference_info")
    df_info  = pd.read_excel(path, sheet_name="block_info")
    df_ra    = pd.read_excel(path, sheet_name="ra_info")

    # Type coercions
    df_prefs["student"]    = df_prefs["student"].astype(int)
    df_prefs["male"]       = df_prefs["male"].astype(int)
    df_prefs["small_room"] = df_prefs["small_room"].astype(int)
    df_info["block"]       = df_info["block"].astype(int)
    df_info["capacity"]    = df_info["capacity"].astype(int)
    df_ra["ra"]            = df_ra["ra"].astype(int)
    df_ra["block"]         = df_ra["block"].astype(int)

    _validate(df_prefs, df_info, df_ra)
    _assign_pref_weights(df_prefs)

    n_s = len(df_prefs)
    n_b = len(df_info)
    n_ra = len(df_ra)
    print(f"  {n_s} students, {n_b} blocks, {n_ra} RAs")
    return df_prefs, df_info, df_ra


def _assign_pref_weights(df: pd.DataFrame):
    """
    Add points_per_{friend|enemy|block}_{1..4} columns.
    Mirrors the assign_pref_weights() inner function in Julia.
    """
    for prefix in ("friend", "enemy", "block"):
        cols   = [f"{prefix}_request_{i}" for i in range(1, 5)]
        counts = df[cols].notna().sum(axis=1)           # how many non-null
        for rank in range(1, 5):                        # rank 1..4
            df[f"points_per_{prefix}_{rank}"] = counts.map(
                lambda c, r=rank: POINTS.get(min(c, 4), [0,0,0,0])[r - 1]
            )


def _validate(df_prefs, df_info, df_ra):
    """Abort early if any preference references a non-existent student/block."""
    students = set(df_prefs["student"])
    blocks   = set(df_info["block"])
    for i in range(1, 5):
        fakes = set(df_prefs[f"friend_request_{i}"].dropna().astype(int)) - students
        if fakes:
            raise ValueError(f"friend_request_{i}: unknown students {fakes}")
        fakes = set(df_prefs[f"enemy_request_{i}"].dropna().astype(int)) - students
        if fakes:
            raise ValueError(f"enemy_request_{i}: unknown students {fakes}")
    for i in range(1, 5):
        fakes = set(df_prefs[f"block_request_{i}"].dropna().astype(int)) - blocks
        if fakes:
            raise ValueError(f"block_request_{i}: unknown blocks {fakes}")
    ra_fakes = set(df_ra["ra"]) - students
    if ra_fakes:
        raise ValueError(f"ra_info.ra: unknown students {ra_fakes}")
    ra_block_fakes = set(df_ra["block"]) - blocks
    if ra_block_fakes:
        raise ValueError(f"ra_info.block: unknown blocks {ra_block_fakes}")


# ─────────────────────────────────────────────────────────────
# Group-only (v3) formulation
# ─────────────────────────────────────────────────────────────

def _build_and_solve_v3(
    df_prefs, df_info, df_ra,
    students, blocks, n_s, n_b, s_pos, b_pos,
    male_bin, small_pref, com_mult,
    block_cap, block_cap_low, block_cap_up,
    male_cap_low, male_cap_up, small_room_cap,
    friend_w, block_w,
    candidate_groups_scored,        # list of (frozenset, score)
    singleton_removal_score,        # drop singleton for students in groups >= this
    time_limit, solver_name, verbose,
    locked_communities,
):
    """
    Pure group-partition formulation (v3).

    Variables
    ---------
    y[g, j]    binary   group g assigned to block j
    w[i, k, j] binary   student i AND their rank-k friend both in block j
    pref_slack / cap_sl / small_sl   (same slack variables as v1/v2)

    No x[i,j], f[i,k], or bp[i,k] variables.

    Capacity / gender / small-room constraints are expressed as
      sum_g  |g| * y[g,j]   (headcount in block j — exact by partition)
    so the hard integer rounding of group sizes is preserved.
    """
    from collections import defaultdict as _dd

    # Unpack (frozenset, score) pairs
    candidate_groups = [g for g, _ in candidate_groups_scored]
    group_scores     = {g: sc for g, sc in candidate_groups_scored}

    # ── Determine which students can lose their singleton ──────
    # A student loses their singleton if they appear in at least one candidate
    # group whose score >= singleton_removal_score (and threshold > 0).
    # Without a singleton, the solver MUST assign that student via one of their
    # candidate groups — safe as long as at least one group can fit in some block.
    no_singleton = set()
    if singleton_removal_score > 0:
        for g in candidate_groups:
            if group_scores[g] >= singleton_removal_score:
                no_singleton |= g

    # ── Build group list ───────────────────────────────────────
    # Singletons are indexed 0..n_s-1 where included (None where removed).
    # Candidate groups are appended after.  We keep the singleton index
    # scheme stable: singleton for student i always lives at index i when present.
    singleton_groups = [
        frozenset([i]) if i not in no_singleton else None
        for i in range(n_s)
    ]
    all_groups = [g for g in singleton_groups if g is not None] + candidate_groups

    # Build a stable index: singleton_idx[i] = group index, or None if removed
    singleton_idx = {}
    idx = 0
    for i, sg in enumerate(singleton_groups):
        if sg is not None:
            singleton_idx[i] = idx
            idx += 1
        else:
            singleton_idx[i] = None
    cand_start = idx   # candidate groups start at this index

    n_g = len(all_groups)

    # Pre-compute per-group aggregates
    group_size  = [len(g) for g in all_groups]
    group_males = [sum(int(male_bin[i]) for i in g) for g in all_groups]
    group_small = [sum(int(small_pref[i]) for i in g) for g in all_groups]

    # Student → list of group indices that contain them
    student_to_groups = _dd(list)
    for g_idx, grp in enumerate(all_groups):
        for i in grp:
            student_to_groups[i].append(g_idx)

    has_friend = {
        (i, k): not pd.isna(df_prefs.iloc[i][f"friend_request_{k + 1}"])
        for i in range(n_s) for k in range(4)
    }

    n_group_students = len(set().union(*candidate_groups)) if candidate_groups else 0
    n_removed = len(no_singleton)
    print(f"  Group partition: {len(candidate_groups)} candidate groups, "
          f"{n_group_students} unique students covered")
    if n_removed:
        print(f"  Singleton removal (score>={singleton_removal_score:.1f}): "
              f"{n_removed} singletons removed → {n_g} total groups "
              f"(was {n_s + len(candidate_groups)})")

    # ── Model ──────────────────────────────────────────────────
    model = pulp.LpProblem("room_allocation_v3", pulp.LpMaximize)

    # ── Variables ──────────────────────────────────────────────

    # y[g, j]: group g assigned to block j
    y = pulp.LpVariable.dicts(
        "y",
        [(g, j) for g in range(n_g) for j in range(n_b)],
        cat="Binary",
    )

    # w[i, k, j]: student i AND their rank-k friend both in block j.
    # Upper-bounded by each party's group assignment sum (see constraints).
    # Since the objective maximises w, only the upper bounds are needed.
    w = pulp.LpVariable.dicts(
        "w",
        [(i, k, j) for i in range(n_s) for k in range(4)
         for j in range(n_b) if has_friend[i, k]],
        cat="Binary",
    )

    pref_slack = pulp.LpVariable.dicts(
        "pref_slack", range(n_s), lowBound=0, cat="Integer"
    )
    small_slack = pulp.LpVariable.dicts(
        "small_slack", range(n_b), lowBound=0, upBound=2, cat="Integer"
    )
    block_low_sl = pulp.LpVariable.dicts(
        "bls", range(n_b), lowBound=0, cat="Integer"
    )
    block_up_sl = pulp.LpVariable.dicts(
        "bus", range(n_b), lowBound=0, cat="Integer"
    )

    print("Variables defined.")

    # ── Objective ──────────────────────────────────────────────

    # Friend preference: each w[i,k,j]=1 earns the ranked friend weight
    friend_obj = pulp.lpSum(
        com_mult[i] * friend_w[i, k] * w[i, k, j]
        for i in range(n_s) for k in range(4) for j in range(n_b)
        if has_friend[i, k]
    )

    # Block preference: expressed directly as sum of y over student's groups
    block_obj_terms = []
    for i in range(n_s):
        for k in range(2):
            bv = df_prefs.iloc[i][f"block_request_{k + 1}"]
            if pd.isna(bv):
                continue
            bj = b_pos[int(bv)]
            block_obj_terms.append(
                com_mult[i] * block_w[i, k]
                * pulp.lpSum(y[g, bj] for g in student_to_groups[i])
            )

    # Group co-location bonus: 0.01 per extra member (singletons contribute 0)
    group_bonus = 0.01 * pulp.lpSum(
        (group_size[g] - 1) * y[g, j]
        for g in range(cand_start, n_g)   # candidate groups only
        for j in range(n_b)
    )

    model += (
        friend_obj
        + pulp.lpSum(block_obj_terms)
        - W_PREF_SLACK  * pulp.lpSum(pref_slack[i]                        for i in range(n_s))
        - W_CAP_SLACK   * pulp.lpSum(block_low_sl[j] + block_up_sl[j]     for j in range(n_b))
        - W_SMALL_SLACK * pulp.lpSum(small_slack[j]                        for j in range(n_b))
        + group_bonus
    )

    print("Objective defined.")

    # ── Partition: each student assigned to exactly one group × block ──
    for i in range(n_s):
        model += pulp.lpSum(
            y[g, j] for g in student_to_groups[i] for j in range(n_b)
        ) == 1

    print("Assignment (partition) constraints done.")

    # ── Group coherence: each group in at most one block ──────
    for g in range(n_g):
        model += pulp.lpSum(y[g, j] for j in range(n_b)) <= 1

    # ── Friend co-assignment upper bounds ──────────────────────
    # w[i,k,j] can only be 1 if BOTH student i and their friend fi_k
    # have their active group assigned to block j.
    # Since the objective maximises w, only upper bounds are needed.
    for i in range(n_s):
        for k in range(4):
            if not has_friend[i, k]:
                continue
            fi = s_pos[int(df_prefs.iloc[i][f"friend_request_{k + 1}"])]
            i_in_j  = [pulp.lpSum(y[g, j] for g in student_to_groups[i])  for j in range(n_b)]
            fi_in_j = [pulp.lpSum(y[g, j] for g in student_to_groups[fi]) for j in range(n_b)]
            for j in range(n_b):
                model += w[i, k, j] <= i_in_j[j]
                model += w[i, k, j] <= fi_in_j[j]

    print("Friend preference constraints done.")
    print("Block preference constraints done.")

    # ── Block capacity constraints ─────────────────────────────
    # headcount[j] = sum_g |g| * y[g,j]  (exact by partition)
    for j in range(n_b):
        headcount_j = pulp.lpSum(group_size[g] * y[g, j] for g in range(n_g))
        model += headcount_j <= block_cap[j]
        model += headcount_j <= block_cap[j] * block_cap_up[j]  + block_up_sl[j]
        model += headcount_j + block_low_sl[j] >= block_cap[j] * block_cap_low[j]

    print("Capacity constraints done.")

    # ── Gender ratio constraints ───────────────────────────────
    for j in range(n_b):
        males_j = pulp.lpSum(group_males[g] * y[g, j] for g in range(n_g))
        total_j = pulp.lpSum(group_size[g]  * y[g, j] for g in range(n_g))
        model += males_j - male_cap_up[j]  * total_j <= 0
        model += males_j - male_cap_low[j] * total_j >= 0

    print("Gender ratio constraints done.")

    # ── Small room constraints ─────────────────────────────────
    for j in range(n_b):
        model += (
            pulp.lpSum(group_small[g] * y[g, j] for g in range(n_g))
            <= small_room_cap[j] + small_slack[j]
        )

    print("Small room constraints done.")

    # ── At least one preference met ───────────────────────────
    for i in range(n_s):
        fp1 = df_prefs.iloc[i]["points_per_friend_1"]
        bp1 = df_prefs.iloc[i]["points_per_block_1"]
        cm  = df_prefs.iloc[i]["community_mult"]
        if fp1 + bp1 >= 1 and cm >= 0.01:
            f_terms  = [w[i, k, j]
                        for k in range(4) for j in range(n_b)
                        if has_friend[i, k]]
            bp_terms = []
            for k in range(2):
                bv = df_prefs.iloc[i][f"block_request_{k + 1}"]
                if not pd.isna(bv):
                    bj = b_pos[int(bv)]
                    bp_terms.append(
                        pulp.lpSum(y[g, bj] for g in student_to_groups[i])
                    )
            model += pulp.lpSum(f_terms) + pulp.lpSum(bp_terms) + pref_slack[i] >= 1

    print("Preference slack constraints done.")

    # ── RA pin constraints ─────────────────────────────────────
    for _, row in df_ra.iterrows():
        ra_s = int(row["ra"])
        ra_b = int(row["block"])
        if ra_s not in s_pos or ra_b not in b_pos:
            continue
        i = s_pos[ra_s]
        j = b_pos[ra_b]
        g_sing = singleton_idx[i]   # None if singleton was removed
        if g_sing is not None:
            model += y[g_sing, j] == 1
            for jj in range(n_b):
                if jj != j:
                    model += y[g_sing, jj] == 0
        # Any candidate group containing this RA must also use their block
        for g in student_to_groups[i]:
            if g == g_sing:
                continue
            for jj in range(n_b):
                if jj != j:
                    model += y[g, jj] == 0

    print("RA pin constraints done.")

    print(f"\nModel has {len(model.variables())} variables "
          f"and {len(model.constraints)} constraints.")

    # ── Solve ──────────────────────────────────────────────────
    solver = _get_solver(solver_name, time_limit, verbose)
    print(f"\nSolving with {solver_name} (time limit {time_limit}s) ...\n")
    t0 = time.time()
    model.solve(solver)
    solve_time = time.time() - t0

    status = pulp.LpStatus[model.status]
    obj    = pulp.value(model.objective) or 0.0
    try:
        mip_gap = model.solverModel.getGap()
    except Exception:
        mip_gap = float("nan")
    print(f"\nStatus:    {status}  (sol_status={model.sol_status})")
    print(f"Objective: {obj:.2f}  MIP gap: {mip_gap:.2%}" if mip_gap == mip_gap else
          f"Objective: {obj:.2f}  MIP gap: n/a")
    print(f"Solve time: {solve_time:.2f}s")

    # ── Slack diagnostics ──────────────────────────────────────
    pref_slack_vals  = [(i, pulp.value(pref_slack[i]) or 0) for i in range(n_s)]
    pref_slack_fired = [(i, v) for i, v in pref_slack_vals if v > 0.5]
    pref_slack_total = sum(v for _, v in pref_slack_vals)

    cap_low_vals  = [(j, pulp.value(block_low_sl[j]) or 0) for j in range(n_b)]
    cap_up_vals   = [(j, pulp.value(block_up_sl[j])  or 0) for j in range(n_b)]
    small_vals    = [(j, pulp.value(small_slack[j])   or 0) for j in range(n_b)]

    cap_low_fired = [(j, v) for j, v in cap_low_vals if v > 0.001]
    cap_up_fired  = [(j, v) for j, v in cap_up_vals  if v > 0.001]
    small_fired   = [(j, v) for j, v in small_vals   if v > 0.001]

    print(f"\n── Slack summary ──────────────────────────────────────")
    print(f"  pref_slack  : {len(pref_slack_fired)} students fired  "
          f"(total={pref_slack_total:.1f}, cost=-{pref_slack_total * W_PREF_SLACK:.1f})")
    if pref_slack_fired:
        for i, v in pref_slack_fired:
            print(f"    student row {i:3d} (id={students[i]}): pref_slack={v:.3f}")
    print(f"  cap_low_sl  : {len(cap_low_fired)} blocks fired  "
          f"(cost=-{sum(v for _,v in cap_low_vals) * W_CAP_SLACK:.1f})")
    for j, v in cap_low_fired:
        print(f"    block {blocks[j]}: under-cap slack={v:.3f}")
    print(f"  cap_up_sl   : {len(cap_up_fired)} blocks fired  "
          f"(cost=-{sum(v for _,v in cap_up_vals) * W_CAP_SLACK:.1f})")
    for j, v in cap_up_fired:
        print(f"    block {blocks[j]}: over-cap slack={v:.3f}")
    print(f"  small_slack : {len(small_fired)} blocks fired  "
          f"(cost=-{sum(v for _,v in small_vals) * W_SMALL_SLACK:.1f})")
    for j, v in small_fired:
        print(f"    block {blocks[j]}: small_slack={v:.3f}")
    total_slack_cost = (pref_slack_total * W_PREF_SLACK
                        + sum(v for _,v in cap_low_vals) * W_CAP_SLACK
                        + sum(v for _,v in cap_up_vals)  * W_CAP_SLACK
                        + sum(v for _,v in small_vals)   * W_SMALL_SLACK)
    print(f"  Total slack cost: -{total_slack_cost:.1f}")
    print(f"──────────────────────────────────────────────────────")

    # ── Extract allocation ─────────────────────────────────────
    alloc = {}
    for i in range(n_s):
        for g_idx in student_to_groups[i]:
            for j in range(n_b):
                if (pulp.value(y[g_idx, j]) or 0) > 0.5:
                    alloc[students[i]] = blocks[j]
                    break
            if students[i] in alloc:
                break

    # ── Pref-slack deep dive ───────────────────────────────────
    if pref_slack_fired:
        print(f"\n── Pref-slack deep dive ───────────────────────────────")
        for i, _ in pref_slack_fired:
            sid      = students[i]
            row      = df_prefs.iloc[i]
            my_block = alloc.get(sid, "?")
            print(f"\n  Student {sid} (row {i}) → assigned block {my_block}")
            bp1 = row.get("block_request_1")
            bp2 = row.get("block_request_2")
            print(f"    Block prefs : "
                  f"{int(bp1) if pd.notna(bp1) else '—'}  "
                  f"{int(bp2) if pd.notna(bp2) else '—'}")
            print(f"    Friend prefs:")
            for k in range(1, 5):
                fv = row.get(f"friend_request_{k}")
                if pd.isna(fv):
                    continue
                fid       = int(fv)
                fid_block = alloc.get(fid, "?")
                same      = "✓ same block" if fid_block == my_block else f"✗ block {fid_block}"
                in_group  = ""
                for lc in locked_communities:
                    fi_pos = s_pos.get(fid)
                    if fi_pos is not None and fi_pos in lc:
                        in_group = " [candidate group]"
                        break
                print(f"      rank {k}: student {fid} → {same}{in_group}")
        print(f"──────────────────────────────────────────────────────")

    return {
        "status":             status,
        "sol_status":         model.sol_status,
        "mip_gap":            mip_gap,
        "objective":          obj,
        "solve_time":         solve_time,
        "alloc":              alloc,
        "formulation":        "v3",
        "locked_communities": locked_communities,
        # model objects
        "students":           students,
        "blocks":             blocks,
        "n_s":                n_s,
        "n_b":                n_b,
        "s_pos":              s_pos,
        "b_pos":              b_pos,
        "df_prefs":           df_prefs,
        "df_info":            df_info,
        # v3-specific variables (f/bp/x are None for v3)
        "x":                  None,
        "f":                  None,
        "bp":                 None,
        "y":                  y,
        "w":                  w,
        "all_groups":         all_groups,
        "student_to_groups":  dict(student_to_groups),
        "has_friend":         has_friend,
        # raw arrays
        "male_bin":           male_bin,
        "small_pref":         small_pref,
        "block_cap":          block_cap,
        "small_room_cap":     small_room_cap,
    }


# ─────────────────────────────────────────────────────────────
# Model  (mirrors room_allocation())
# ─────────────────────────────────────────────────────────────

def build_and_solve(
    df_prefs,
    df_info,
    df_ra,
    time_limit: int = 3600,
    solver_name: str = "SCIP",
    verbose: bool = True,
    formulation: str = "v1",
    # ── Clique locking ────────────────────────────────────────
    use_clique_lock:        bool  = False,
    clique_join_frac:       float = 0.5,
    clique_max_size:        int   = 8,
    clique_max_community:   int   = 5,
    clique_min_density:     float = 0.5,
    clique_min_score:       float = 3.0,
    clique_use_louvain:     bool  = True,
    clique_louvain_res:     float = 1.0,
    clique_block_bonus:             float = 2.0,
    clique_block_coherence_min:     float = 0.6,
    clique_singleton_removal_score: float = 0.0,
):
    """
    Build the PuLP MIP and solve it.
    Returns a result dict containing the solution and all variable objects.

    formulation : "v1" — f[i,j,k] indexed by (student, block, rank)

    Clique locking pre-constrains groups of mutually-listed students to the
    same block, reducing the effective search space.  Controlled by:
      use_clique_lock      : enable/disable (default off)
      clique_join_frac     : min fraction of group a fuzzy candidate must connect to
      clique_max_size      : max fuzzy-expansion size
      clique_max_community : max merged community size
      clique_min_density   : min mutual-pair density to lock a community
      clique_min_score     : min mean mutual-pair strength to lock a community
      clique_use_louvain   : also run Louvain pipeline (default on)
      clique_louvain_res   : Louvain resolution (lower → bigger communities)
      clique_block_bonus          : edge-weight bonus for shared top block pref
      clique_block_coherence_min  : min fraction of community that must share the
                                    most common top block pref (0 to disable)
    """

    # ── Index helpers ─────────────────────────────────────────
    students = list(df_prefs["student"].astype(int))
    blocks   = list(df_info["block"].astype(int))
    n_s, n_b = len(students), len(blocks)
    s_pos    = {s: i for i, s in enumerate(students)}   # student id → row index
    b_pos    = {b: j for j, b in enumerate(blocks)}     # block id   → col index

    # ── Raw arrays ────────────────────────────────────────────
    male_bin       = df_prefs["male"].values.astype(int)
    small_pref     = df_prefs["small_room"].values.astype(int)
    com_mult       = df_prefs["community_mult"].values.astype(float)
    block_cap      = df_info["capacity"].values.astype(int)
    block_cap_low  = df_info["block_cap_low"].values.astype(float)
    block_cap_up   = df_info["block_cap_up"].values.astype(float)
    male_cap_low   = df_info["male_cap_low"].values.astype(float)
    male_cap_up    = df_info["male_cap_up"].values.astype(float)
    small_room_cap = df_info["small_room_cap"].values.astype(int)

    # ── Preference weight matrices ────────────────────────────
    friend_w = df_prefs[
        [f"points_per_friend_{k}" for k in range(1, 5)]
    ].values.astype(float)                              # (n_s, 4)

    block_w = df_prefs[
        [f"points_per_block_{k}" for k in range(1, 3)]
    ].values.astype(float)                              # (n_s, 2)  — only first 2, matching Julia

    # ── Clique locking ─────────────────────────────────────────
    # Find disjoint communities (with stranding pre-pass protection).
    # For each community {r, i1, i2, ...} we will add co-location constraints:
    #   x[ik, j] == x[r, j]  for all non-representative ik, for all blocks j
    locked_communities = []
    if use_clique_lock:
        from .clique_utils import find_lockable_communities
        locked_communities = find_lockable_communities(
            df_prefs, df_info, df_ra,
            join_frac          = clique_join_frac,
            max_size           = clique_max_size,
            max_community      = clique_max_community,
            min_density        = clique_min_density,
            min_score          = clique_min_score,
            use_louvain        = clique_use_louvain,
            louvain_resolution = clique_louvain_res,
            block_bonus          = clique_block_bonus,
            block_coherence_min  = clique_block_coherence_min,
        )
        n_locked_students = len(set().union(*locked_communities)) if locked_communities else 0
        print(f"  Clique lock: {len(locked_communities)} communities, "
              f"{n_locked_students} students pre-locked")

    # ── Model ──────────────────────────────────────────────────
    model = pulp.LpProblem("room_allocation", pulp.LpMaximize)

    # ── Variables ─────────────────────────────────────────────

    # allocation_var[i, j] — student i assigned to block j
    x = pulp.LpVariable.dicts(
        "x",
        [(i, j) for i in range(n_s) for j in range(n_b)],
        cat="Binary",
    )

    # friend_pref_met variables — shape depends on formulation
    # v1: f[i,j,k]  (student × block × rank) — original
    # v2: f[i,k]    (student × rank)          — block dim removed
    # Only create variables for non-NaN friend request slots; Julia does the same.
    # Without this guard, SCIP sets unconstrained f[i,j,k] freely to 1 on empty
    # slots, inflating reported pref counts and corrupting the pref-slack constraint.
    has_friend = {
        (i, k): not pd.isna(df_prefs.iloc[i][f"friend_request_{k + 1}"])
        for i in range(n_s) for k in range(4)
    }
    if formulation == "v1":
        f = pulp.LpVariable.dicts(
            "f",
            [(i, j, k) for i in range(n_s) for j in range(n_b) for k in range(4)
             if has_friend[i, k]],
            cat="Binary",
        )
    else:
        f = pulp.LpVariable.dicts(
            "f",
            [(i, k) for i in range(n_s) for k in range(4)
             if has_friend[i, k]],
            cat="Binary",
        )

    # block_pref_met_var[i, k] — student i's k-th block pref met (k = 0,1)
    bp = pulp.LpVariable.dicts(
        "bp",
        [(i, k) for i in range(n_s) for k in range(2)],
        cat="Binary",
    )

    # pref_slack_var[i] — penalty for student with no preference met
    pref_slack = pulp.LpVariable.dicts(
        "pref_slack", range(n_s), lowBound=0, cat="Integer"
    )

    # small_room_slack_var[j] — overflow of small-room preference in block j
    small_slack = pulp.LpVariable.dicts(
        "small_slack", range(n_b), lowBound=0, upBound=2, cat="Integer"
    )

    # block_low_slack_var[j], block_up_slack_var[j]
    block_low_sl = pulp.LpVariable.dicts(
        "bls", range(n_b), lowBound=0, cat="Integer"
    )
    block_up_sl = pulp.LpVariable.dicts(
        "bus", range(n_b), lowBound=0, cat="Integer"
    )

    print("Variables defined.")

    # ── Objective ─────────────────────────────────────────────
    if formulation == "v1":
        friend_obj = pulp.lpSum(
            com_mult[i] * friend_w[i, k] * f[i, j, k]
            for i in range(n_s) for j in range(n_b) for k in range(4)
            if has_friend[i, k]
        )
    else:
        friend_obj = pulp.lpSum(
            com_mult[i] * friend_w[i, k] * f[i, k]
            for i in range(n_s) for k in range(4)
            if has_friend[i, k]
        )
    model += (
        friend_obj
        + pulp.lpSum(
            com_mult[i] * block_w[i, k] * bp[i, k]
            for i in range(n_s) for k in range(2)
        )
        - W_PREF_SLACK  * pulp.lpSum(pref_slack[i]                        for i in range(n_s))
        - W_CAP_SLACK   * pulp.lpSum(block_low_sl[j] + block_up_sl[j]     for j in range(n_b))
        - W_SMALL_SLACK * pulp.lpSum(small_slack[j]                        for j in range(n_b))
    )

    print("Objective defined.")

    # ── Constraint: each student in exactly one block ─────────
    for i in range(n_s):
        model += pulp.lpSum(x[i, j] for j in range(n_b)) == 1

    print("Assignment constraints done.")

    # ── Friend preference constraints ─────────────────────────
    # Skipped (i,k) pairs have no variable: zero-weight slots need no
    # constraint (they don't exist); mutual aliases share the canonical
    # variable whose constraints already cover both directions.
    for i in range(n_s):
        for k in range(4):
            fv = df_prefs.iloc[i][f"friend_request_{k + 1}"]
            if pd.isna(fv):
                continue
            fi = s_pos[int(fv)]
            if formulation == "v1":
                for j in range(n_b):
                    model += 2 * f[i, j, k] <= x[i, j] + x[fi, j]
            else:
                for j in range(n_b):
                    model += f[i, k] <= 1 - x[i, j] + x[fi, j]

    print("Friend preference constraints done.")

    # ── Enemy preference constraints ──────────────────────────
    # NOTE: commented out in Julia — kept here but disabled.
    # To enable, uncomment and add enemy_pref_ig_var to objective.
    #
    # for i in range(n_s):
    #     for k in range(4):
    #         col = f"enemy_request_{k+1}"
    #         ev  = df_prefs.iloc[i][col]
    #         if pd.isna(ev):
    #             ...
    #         else:
    #             ei = s_pos[int(ev)]
    #             for j in range(n_b):
    #                 model += enemy_var[i,j,k] + 1 >= x[i,j] + x[ei,j]

    # ── Block preference constraints ──────────────────────────
    # bp[i,k] <= x[i, preferred_block]
    # (only first 2 preferences used, matching Julia)
    for i in range(n_s):
        for k in range(2):
            col = f"block_request_{k + 1}"
            bv  = df_prefs.iloc[i][col]
            if pd.isna(bv):
                model += bp[i, k] == 0
            else:
                bj = b_pos[int(bv)]
                model += bp[i, k] <= x[i, bj]

    print("Block preference constraints done.")

    # ── Block capacity constraints ────────────────────────────
    for j in range(n_b):
        total_j = pulp.lpSum(x[i, j] for i in range(n_s))

        # Hard ceiling
        model += total_j <= block_cap[j]

        # Soft upper bound (with slack)
        model += total_j <= block_cap[j] * block_cap_up[j] + block_up_sl[j]

        # Soft lower bound (with slack)
        model += total_j + block_low_sl[j] >= block_cap[j] * block_cap_low[j]

        # Non-negative (implicit, but explicit for clarity)
        model += total_j >= 0

    print("Capacity constraints done.")

    # ── Gender ratio constraints ──────────────────────────────
    # males_j <= total_j * male_cap_up[j]
    # males_j >= total_j * male_cap_low[j]
    # Both are linear because male_bin is a parameter, not a variable.
    for j in range(n_b):
        males_j = pulp.lpSum(x[i, j] * int(male_bin[i]) for i in range(n_s))
        total_j = pulp.lpSum(x[i, j] for i in range(n_s))
        model += males_j - male_cap_up[j]  * total_j <= 0
        model += males_j - male_cap_low[j] * total_j >= 0

    print("Gender ratio constraints done.")

    # ── Small room capacity ───────────────────────────────────
    for j in range(n_b):
        model += (
            pulp.lpSum(x[i, j] * int(small_pref[i]) for i in range(n_s))
            <= small_room_cap[j] + small_slack[j]
        )

    print("Small room constraints done.")

    # ── At least one preference met ───────────────────────────
    for i in range(n_s):
        fp1 = df_prefs.iloc[i]["points_per_friend_1"]
        bp1 = df_prefs.iloc[i]["points_per_block_1"]
        cm  = df_prefs.iloc[i]["community_mult"]
        if fp1 + bp1 >= 1 and cm >= 0.01:
            f_terms = []
            for k in range(4):
                if not has_friend[i, k]:
                    continue
                if formulation == "v1":
                    f_terms += [f[i, j, k] for j in range(n_b)]
                else:
                    f_terms.append(f[i, k])
            f_sum = pulp.lpSum(f_terms)
            model += f_sum + pulp.lpSum(bp[i, k] for k in range(2)) + pref_slack[i] >= 1

    print("Preference slack constraints done.")

    # ── RA pin constraints ────────────────────────────────────
    for _, row in df_ra.iterrows():
        ra_s = int(row["ra"])
        ra_b = int(row["block"])
        if ra_s not in s_pos or ra_b not in b_pos:
            continue
        i = s_pos[ra_s]
        j = b_pos[ra_b]
        model += x[i, j] == 1
        for jj in range(n_b):
            if jj != j:
                model += x[i, jj] == 0

    # NOTE: RA support-person (sup_1, sup_2) pins are commented out in Julia.
    # Uncomment here to enable.

    print("RA pin constraints done.")

    # ── Co-location constraints ───────────────────────────────
    # For each locked community, all members must be assigned to the same block.
    for community in locked_communities:
        members = sorted(community)
        rep = members[0]
        for ik in members[1:]:
            for j in range(n_b):
                model += x[ik, j] == x[rep, j]

    if locked_communities:
        print(f"Co-location constraints done "
              f"({len(locked_communities)} communities).")

    print(f"\nModel has {len(model.variables())} variables "
          f"and {len(model.constraints)} constraints.")

    # ── Solve ─────────────────────────────────────────────────
    solver = _get_solver(solver_name, time_limit, verbose)

    print(f"\nSolving with {solver_name} (time limit {time_limit}s) ...\n")
    t0 = time.time()
    model.solve(solver)
    solve_time = time.time() - t0

    status = pulp.LpStatus[model.status]
    obj    = pulp.value(model.objective) or 0.0
    try:
        mip_gap = model.solverModel.getGap()
    except Exception:
        mip_gap = float("nan")
    print(f"\nStatus:    {status}  (sol_status={model.sol_status})")
    print(f"Objective: {obj:.2f}  MIP gap: {mip_gap:.2%}" if mip_gap == mip_gap else
          f"Objective: {obj:.2f}  MIP gap: n/a")
    print(f"Solve time: {solve_time:.2f}s")

    # ── Slack diagnostics ──────────────────────────────────────
    pref_slack_vals = [(i, pulp.value(pref_slack[i]) or 0) for i in range(n_s)]
    pref_slack_fired = [(i, v) for i, v in pref_slack_vals if v > 0.5]
    pref_slack_total = sum(v for _, v in pref_slack_vals)

    cap_low_vals  = [(j, pulp.value(block_low_sl[j]) or 0) for j in range(n_b)]
    cap_up_vals   = [(j, pulp.value(block_up_sl[j])  or 0) for j in range(n_b)]
    small_vals    = [(j, pulp.value(small_slack[j])   or 0) for j in range(n_b)]

    cap_low_fired   = [(j, v) for j, v in cap_low_vals  if v > 0.001]
    cap_up_fired    = [(j, v) for j, v in cap_up_vals   if v > 0.001]
    small_fired     = [(j, v) for j, v in small_vals    if v > 0.001]

    print(f"\n── Slack summary ──────────────────────────────────────")
    print(f"  pref_slack  : {len(pref_slack_fired)} students fired  "
          f"(total={pref_slack_total:.1f}, cost=-{pref_slack_total * W_PREF_SLACK:.1f})")
    if pref_slack_fired:
        for i, v in pref_slack_fired:
            print(f"    student row {i:3d} (id={students[i]}): pref_slack={v:.3f}")

    print(f"  cap_low_sl  : {len(cap_low_fired)} blocks fired  "
          f"(cost=-{sum(v for _,v in cap_low_vals) * W_CAP_SLACK:.1f})")
    for j, v in cap_low_fired:
        print(f"    block {blocks[j]}: under-cap slack={v:.3f}")

    print(f"  cap_up_sl   : {len(cap_up_fired)} blocks fired  "
          f"(cost=-{sum(v for _,v in cap_up_vals) * W_CAP_SLACK:.1f})")
    for j, v in cap_up_fired:
        print(f"    block {blocks[j]}: over-cap slack={v:.3f}")

    print(f"  small_slack : {len(small_fired)} blocks fired  "
          f"(cost=-{sum(v for _,v in small_vals) * W_SMALL_SLACK:.1f})")
    for j, v in small_fired:
        print(f"    block {blocks[j]}: small_slack={v:.3f}")

    total_slack_cost = (pref_slack_total * W_PREF_SLACK
                        + sum(v for _,v in cap_low_vals) * W_CAP_SLACK
                        + sum(v for _,v in cap_up_vals)  * W_CAP_SLACK
                        + sum(v for _,v in small_vals)   * W_SMALL_SLACK)
    print(f"  Total slack cost: -{total_slack_cost:.1f}")
    print(f"──────────────────────────────────────────────────────")

    # ── Extract allocation ─────────────────────────────────────
    alloc = {}
    for i in range(n_s):
        for j in range(n_b):
            if (pulp.value(x[i, j]) or 0) > 0.5:
                alloc[students[i]] = blocks[j]

    # ── Pref-slack deep dive ────────────────────────────────────
    # For each student with pref_slack fired, explain why:
    # show their block, block prefs, listed friends and where each ended up.
    if pref_slack_fired:
        print(f"\n── Pref-slack deep dive ───────────────────────────────")
        for i, _ in pref_slack_fired:
            sid  = students[i]
            row  = df_prefs.iloc[i]
            my_block = alloc.get(sid, "?")
            print(f"\n  Student {sid} (row {i}) → assigned block {my_block}")

            # Block preferences
            bp1 = row.get("block_request_1")
            bp2 = row.get("block_request_2")
            print(f"    Block prefs : "
                  f"{int(bp1) if pd.notna(bp1) else '—'}  "
                  f"{int(bp2) if pd.notna(bp2) else '—'}")

            # Friend preferences and where each ended up
            print(f"    Friend prefs:")
            for k in range(1, 5):
                fv = row.get(f"friend_request_{k}")
                if pd.isna(fv):
                    continue
                fid       = int(fv)
                fid_block = alloc.get(fid, "?")
                same      = "✓ same block" if fid_block == my_block else f"✗ block {fid_block}"
                locked_tag = ""
                for lc in locked_communities:
                    fi_pos = s_pos.get(fid)
                    if fi_pos is not None and fi_pos in lc:
                        locked_tag = " [locked]"
                        break
                print(f"      rank {k}: student {fid} → {same}{locked_tag}")
        print(f"──────────────────────────────────────────────────────")

    return {
        "status":             status,
        "sol_status":         model.sol_status,
        "mip_gap":            mip_gap,
        "objective":          obj,
        "solve_time":         solve_time,
        "alloc":              alloc,
        "formulation":        formulation,
        "locked_communities": locked_communities,
        # model objects
        "students":      students,
        "blocks":        blocks,
        "n_s":           n_s,
        "n_b":           n_b,
        "s_pos":         s_pos,
        "b_pos":         b_pos,
        "df_prefs":      df_prefs,
        "df_info":       df_info,
        "x":             x,
        "f":             f,
        "bp":            bp,
        "male_bin":      male_bin,
        "small_pref":    small_pref,
        "block_cap":     block_cap,
        "small_room_cap":small_room_cap,
    }


def _get_solver(name, time_limit, verbose):
    msg = 1 if verbose else 0
    if name.upper() == "SCIP":
        # Prefer pyscipopt (native Python bindings, no subprocess overhead)
        try:
            import pyscipopt  # noqa: F401
            solver = pulp.SCIP_PY(timeLimit=time_limit, msg=msg)
            print("  [solver] Using SCIP via pyscipopt (native)")
            return solver
        except (ImportError, AttributeError):
            pass
        # Fall back to SCIP binary via command line
        try:
            solver = pulp.SCIP_CMD(msg=msg, timeLimit=time_limit)
            print("  [solver] Using SCIP via SCIP_CMD (binary)")
            return solver
        except Exception:
            print("  [solver] SCIP unavailable, falling back to CBC.")
    if name.upper() == "HIGHS":
        # Locate the HiGHS binary — try several sources in order:
        #   1. highspy package ships a bundled binary alongside its .so
        #   2. highs binary on PATH (e.g. brew install highs)
        import shutil, importlib
        highs_bin = shutil.which("highs")
        if highs_bin is None:
            try:
                import highspy
                pkg_dir = importlib.util.find_spec("highspy").submodule_search_locations[0]
                import pathlib
                candidates = list(pathlib.Path(pkg_dir).rglob("highs"))
                if candidates:
                    highs_bin = str(candidates[0])
            except Exception:
                pass
        if highs_bin:
            try:
                solver = pulp.HiGHS_CMD(path=highs_bin, msg=msg, timeLimit=time_limit)
                print(f"  [solver] Using HiGHS binary at {highs_bin}")
                return solver
            except Exception as e:
                print(f"  [solver] HiGHS binary found but failed to init ({e})")
        else:
            print("  [solver] HiGHS binary not found.")
            print("  [solver]   Install with: pip3 install highspy")
            print("  [solver]   Or:           brew install highs")
            print("  [solver] Falling back to CBC.")
    return pulp.PULP_CBC_CMD(msg=msg, timeLimit=time_limit)


# ─────────────────────────────────────────────────────────────
# Output  (mirrors Julia output section)
# ─────────────────────────────────────────────────────────────

def write_output(result: dict, output_dir: str = "Output"):
    """Write blocks.txt, pref_met.txt and pref_nothing_met.txt."""
    os.makedirs(output_dir, exist_ok=True)

    students     = result["students"]
    blocks       = result["blocks"]
    alloc        = result["alloc"]
    df_prefs     = result["df_prefs"]
    df_info      = result["df_info"]
    male_bin     = result["male_bin"]
    small_pref   = result["small_pref"]
    block_cap    = result["block_cap"]
    small_room_cap = result["small_room_cap"]
    s_pos        = result["s_pos"]
    n_b          = result["n_b"]
    f_vars       = result["f"]
    bp_vars      = result["bp"]

    # ── blocks.txt ────────────────────────────────────────────
    with open(os.path.join(output_dir, "blocks.txt"), "w") as io:
        roomed = 0
        for j, b in enumerate(blocks):
            assigned = sorted(s for s in students if alloc.get(s) == b)
            n_m  = sum(male_bin[s_pos[s]]   for s in assigned)
            n_sm = sum(small_pref[s_pos[s]] for s in assigned)
            io.write(f"Block - {b}:\n")
            io.write(f"{len(assigned)}/{block_cap[j]} total rooms.\n")
            io.write(f"{n_m} of {len(assigned)} are male.\n")
            io.write(f"{n_sm}/{small_room_cap[j]} small rooms filled.\n\n")
            for s in assigned:
                io.write(f"{s}\n")
                roomed += 1
            io.write("\n\n")
        if roomed > len(students):
            print("WARNING: some students allocated to multiple blocks")
        elif roomed < len(students):
            print(f"WARNING: {len(students) - roomed} student(s) not allocated")

    # ── pref_met.txt ──────────────────────────────────────────
    formulation = result.get("formulation", "v1")
    n_b_out     = result["n_b"]

    if formulation == "v3":
        # v3: friend met from w[i,k,j]; block pref met from y[g, bj]
        w_vars            = result["w"]
        y_vars            = result["y"]
        student_to_groups = result["student_to_groups"]
        b_pos_out         = result["b_pos"]

        def _f_met_out(i, k):
            return any(
                (v := w_vars.get((i, k, j))) is not None and (pulp.value(v) or 0) > 0.5
                for j in range(n_b_out)
            )

        def _bp_met_out(i, k):
            bv = df_prefs.iloc[i].get(f"block_request_{k + 1}")
            if pd.isna(bv):
                return False
            bj = b_pos_out[int(bv)]
            return any(
                (pulp.value(y_vars.get((g, bj)) or 0)) > 0.5
                for g in student_to_groups[i]
            )
    else:
        def _f_met_out(i, k):
            if formulation == "v1":
                return any(
                    (v := f_vars.get((i, j, k))) is not None and (pulp.value(v) or 0) > 0.5
                    for j in range(n_b_out)
                )
            v = f_vars.get((i, k))
            return v is not None and (pulp.value(v) or 0) > 0.5

        def _bp_met_out(i, k):
            return (pulp.value(bp_vars[i, k]) or 0) > 0.5

    with open(os.path.join(output_dir, "pref_met.txt"), "w") as io:
        for i, s in enumerate(students):
            io.write(f"{s}:\n")
            for k in range(4):
                if _f_met_out(i, k):
                    io.write(f"Friend preference {k + 1} met\n")
            for k in range(2):
                if _bp_met_out(i, k):
                    io.write(f"Block preference {k + 1} met\n")
            io.write("\n")

    # ── pref_nothing_met.txt ──────────────────────────────────
    with open(os.path.join(output_dir, "pref_nothing_met.txt"), "w") as io:
        for i, s in enumerate(students):
            fp1 = df_prefs.iloc[i]["points_per_friend_1"]
            bp1 = df_prefs.iloc[i]["points_per_block_1"]
            cm  = df_prefs.iloc[i]["community_mult"]
            f_total  = sum(1 for k in range(4) if _f_met_out(i, k))
            bp_total = sum(1 for k in range(2) if _bp_met_out(i, k))
            if fp1 + bp1 >= 1 and cm >= 0.01 and f_total + bp_total < 0.1:
                io.write(f"{s}: Didn't get any friend or block preferences\n\n")

    print(f"Output written to {output_dir}/")


def preference_stats(result: dict) -> dict:
    """Return preference satisfaction summary as a dict."""
    students    = result["students"]
    n_s, n_b    = result["n_s"], result["n_b"]
    df_prefs    = result["df_prefs"]
    formulation = result.get("formulation", "v1")

    if formulation == "v3":
        w_vars            = result["w"]
        y_vars            = result["y"]
        student_to_groups = result["student_to_groups"]
        b_pos_st          = result["b_pos"]

        def _f_met(i, k):
            return any(
                (v := w_vars.get((i, k, j))) is not None and (pulp.value(v) or 0) > 0.5
                for j in range(n_b)
            )

        def _bp_met(i, k):
            bv = df_prefs.iloc[i].get(f"block_request_{k + 1}")
            if pd.isna(bv):
                return False
            bj = b_pos_st[int(bv)]
            return any(
                (pulp.value(y_vars.get((g, bj)) or 0)) > 0.5
                for g in student_to_groups[i]
            )
    else:
        f_vars  = result["f"]
        bp_vars = result["bp"]

        def _f_met(i, k):
            if formulation == "v1":
                return any(
                    (v := f_vars.get((i, j, k))) is not None and (pulp.value(v) or 0) > 0.5
                    for j in range(n_b)
                )
            v = f_vars.get((i, k))
            return v is not None and (pulp.value(v) or 0) > 0.5

        def _bp_met(i, k):
            return (pulp.value(bp_vars[i, k]) or 0) > 0.5

    friend_met = sum(1 for i in range(n_s) for k in range(4) if _f_met(i, k))
    block_met  = sum(1 for i in range(n_s) for k in range(2) if _bp_met(i, k))

    students_any, students_with_prefs = 0, 0
    for i in range(n_s):
        fp1 = df_prefs.iloc[i]["points_per_friend_1"]
        bp1 = df_prefs.iloc[i]["points_per_block_1"]
        cm  = df_prefs.iloc[i]["community_mult"]
        if fp1 + bp1 >= 1 and cm >= 0.01:
            students_with_prefs += 1
            f_t  = sum(1 for k in range(4) if _f_met(i, k))
            bp_t = sum(1 for k in range(2) if _bp_met(i, k))
            if f_t + bp_t > 0.5:
                students_any += 1

    return {
        "friend_prefs_met":   friend_met,
        "block_prefs_met":    block_met,
        "students_any_met":   students_any,
        "students_with_prefs":students_with_prefs,
    }


# ─────────────────────────────────────────────────────────────
# CLI entry point
# ─────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Room allocation MIP solver")
    parser.add_argument("data_path", help="Path to input xlsx file")
    parser.add_argument("--time-limit",            type=int,   default=3600)
    parser.add_argument("--solver",                default="SCIP", choices=["CBC", "SCIP", "HIGHS"])
    parser.add_argument("--method",                default="mip", choices=["mip", "lns"],
                        help="Solve method: mip (default) or lns")
    parser.add_argument("--output-dir",            default=None)
    parser.add_argument("--quiet",                 action="store_true")
    # Clique locking
    parser.add_argument("--clique-lock",           action="store_true",
                        help="Enable clique co-location locking")
    parser.add_argument("--clique-min-density",    type=float, default=0.5,
                        help="Min mutual-pair density to lock a community (default 0.5)")
    parser.add_argument("--clique-min-score",      type=float, default=3.0,
                        help="Min mean mutual strength to lock a community (default 3.0)")
    parser.add_argument("--clique-max-community",  type=int,   default=6,
                        help="Max community size to lock (default 6)")
    parser.add_argument("--clique-join-frac",      type=float, default=0.5,
                        help="Fuzzy join fraction (default 0.5)")
    args = parser.parse_args()

    out_dir = args.output_dir or os.path.join(
        "Output", os.path.splitext(os.path.basename(args.data_path))[0]
    )

    df_prefs, df_info, df_ra = load_data(args.data_path)

    if args.method == "lns":
        from lns import lns_solve
        result = lns_solve(
            df_prefs, df_info, df_ra,
            time_limit           = args.time_limit,
            solver_name          = args.solver,
            verbose              = not args.quiet,
            use_clique_lock      = args.clique_lock,
            clique_min_density   = args.clique_min_density,
            clique_min_score     = args.clique_min_score,
            clique_max_community = args.clique_max_community,
            clique_join_frac     = args.clique_join_frac,
        )
    else:
        result = build_and_solve(
            df_prefs, df_info, df_ra,
            time_limit           = args.time_limit,
            solver_name          = args.solver,
            verbose              = not args.quiet,
            use_clique_lock      = args.clique_lock,
            clique_min_density   = args.clique_min_density,
            clique_min_score     = args.clique_min_score,
            clique_max_community = args.clique_max_community,
            clique_join_frac     = args.clique_join_frac,
        )
    write_output(result, out_dir)


if __name__ == "__main__":
    main()
