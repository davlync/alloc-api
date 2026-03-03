"""
lns.py  —  Large Neighbourhood Search for room allocation.

Strategy
────────
1. Initial solution: short MIP run (initial_time seconds).
   Falls back to greedy if MIP finds nothing in time.
2. LNS loop until time budget exhausted:
   a. Destroy: unassign a subset of students (free set)
   b. Repair:  re-solve the sub-MIP for free students only,
               with fixed-student contributions as constants
   c. Accept:  keep if objective improves (greedy acceptance)
3. Return best solution found.

Destroy operators (cycled adaptively):
  random  — uniformly random subset
  block   — all students in the block with the worst mean preference satisfaction

Repair MIP
──────────
Variables only for free students.  Per-block constants pre-computed from
the fixed assignment eliminate their contributions from capacity, gender,
and small-room constraints.  Friend preferences use a v2-style f[i,k]
formulation; free→fixed cross-terms are handled via a simple link
  f[i,k]  <=  x[i, b_fixed_friend]
so the objective correctly captures satisfaction of these preferences.

Entry point
───────────
  result = lns_solve(df_prefs, df_info, df_ra,
                     time_limit=30, solver_name="SCIP", ...)
  # Returns same dict shape as build_and_solve in room_allocator.py
"""

import time
import random
import math
import pandas as pd
import numpy as np

try:
    import pulp
except ImportError:
    raise ImportError("PuLP not found.  Install with:  pip install pulp")

from .room_allocator import (
    POINTS, W_PREF_SLACK, W_CAP_SLACK, W_SMALL_SLACK,
    _get_solver, _assign_pref_weights, build_and_solve,
)


# ── Objective computation ──────────────────────────────────────────────────────

def _compute_obj(alloc, df_prefs, df_info, s_pos, b_pos,
                 com_mult, friend_w, block_w):
    """
    Compute the true MIP objective for a given alloc dict — matching what
    Julia/SCIP would report for the same assignment.

    This is:
        sum of (com_mult[i] * points * pref_met[i])
        - W_PREF_SLACK  * sum(pref_slack[i])
        - W_CAP_SLACK   * sum(block_low_sl[j] + block_up_sl[j])
        - W_SMALL_SLACK * sum(small_slack[j])

    Slack values are derived directly from the alloc dict rather than from MIP
    variables, so the result is always consistent with the assignment.
    """
    students       = list(df_prefs["student"].astype(int))
    blocks         = list(df_info["block"].astype(int))
    n_b            = len(blocks)

    block_cap      = df_info["capacity"].values.astype(int)
    block_cap_low  = df_info["block_cap_low"].values.astype(float)
    block_cap_up   = df_info["block_cap_up"].values.astype(float)
    small_room_cap = df_info["small_room_cap"].values.astype(int)
    male_bin_arr   = df_prefs["male"].values.astype(int)
    small_pref_arr = df_prefs["small_room"].values.astype(int)

    # Per-block occupancy counters (needed for capacity/small-room slack)
    n_in_block    = [0] * n_b
    n_small_in    = [0] * n_b

    pref_score   = 0.0
    pref_penalty = 0.0

    for i, sid in enumerate(students):
        bi = alloc.get(sid)
        if bi is None:
            continue
        j = b_pos.get(bi)
        if j is not None:
            n_in_block[j] += 1
            n_small_in[j] += int(small_pref_arr[i])

        any_met = False

        # Friend preferences
        for k in range(4):
            fv = df_prefs.iloc[i].get(f"friend_request_{k+1}")
            if pd.isna(fv):
                continue
            fid = int(fv)
            if alloc.get(fid) == bi:
                pref_score += com_mult[i] * friend_w[i, k]
                any_met = True

        # Block preferences
        for k in range(2):
            bv = df_prefs.iloc[i].get(f"block_request_{k+1}")
            if pd.isna(bv):
                continue
            if int(bv) == bi:
                pref_score += com_mult[i] * block_w[i, k]
                any_met = True

        # pref_slack penalty: student has preferences but none were met
        fp1 = df_prefs.iloc[i]["points_per_friend_1"]
        bp1 = df_prefs.iloc[i]["points_per_block_1"]
        cm  = df_prefs.iloc[i]["community_mult"]
        if fp1 + bp1 >= 1 and cm >= 0.01 and not any_met:
            pref_penalty += W_PREF_SLACK   # 1 unit of pref_slack

    # Capacity and small-room slack penalties
    cap_penalty   = 0.0
    small_penalty = 0.0
    for j in range(n_b):
        n   = n_in_block[j]
        cap = block_cap[j]
        cap_penalty += W_CAP_SLACK * max(0.0, n - cap * block_cap_up[j])   # upper
        cap_penalty += W_CAP_SLACK * max(0.0, cap * block_cap_low[j] - n)  # lower
        small_viol   = min(2, max(0, n_small_in[j] - small_room_cap[j]))
        small_penalty += W_SMALL_SLACK * small_viol

    return pref_score - pref_penalty - cap_penalty - small_penalty


# ── Destroy operators ─────────────────────────────────────────────────────────

def _destroy_random(alloc, students, frac, rng):
    """Return a set of student_ids to unassign (random frac of all students)."""
    k = max(2, int(len(students) * frac))
    return set(rng.sample(students, k))


def _destroy_block_worst(alloc, students, df_prefs, s_pos, b_pos,
                         com_mult, friend_w):
    """
    Return the set of student_ids currently assigned to the block with the
    worst mean preference satisfaction.  'Worst' = lowest mean fraction of
    friend preferences actually met.
    """
    blocks = list(b_pos.keys())
    block_students = {b: [] for b in blocks}
    for sid in students:
        b = alloc.get(sid)
        if b is not None:
            block_students[b].append(sid)

    best_score = math.inf
    worst_block = None
    for b, sids in block_students.items():
        if not sids:
            continue
        score = 0.0
        total_weight = 0.0
        for sid in sids:
            i = s_pos[sid]
            for k in range(4):
                fv = df_prefs.iloc[i].get(f"friend_request_{k+1}")
                if pd.isna(fv):
                    continue
                w = com_mult[i] * friend_w[i, k]
                total_weight += w
                if alloc.get(int(fv)) == b:
                    score += w
        mean = (score / total_weight) if total_weight > 0 else 1.0
        if mean < best_score:
            best_score = mean
            worst_block = b

    if worst_block is None:
        # Fallback: pick a random block
        worst_block = rng_fallback.choice(blocks)

    return set(block_students[worst_block])

_rng_fb = random.Random(0)


# ── Repair MIP ────────────────────────────────────────────────────────────────

def _repair(df_prefs, df_info, df_ra,
            alloc, free_sids,
            students, blocks, n_s, n_b, s_pos, b_pos,
            male_bin, small_pref, com_mult, friend_w, block_w,
            block_cap, block_cap_low, block_cap_up,
            male_cap_low, male_cap_up, small_room_cap,
            locked_communities,
            time_limit, solver_name, verbose):
    """
    Re-solve allocation for free_sids only.  All other students are fixed.

    Returns (new_alloc, objective) where new_alloc is the updated full
    assignment dict, or (None, None) if the solver failed / found nothing.
    """
    free_set  = set(free_sids)
    fixed_map = {sid: alloc[sid] for sid in students if sid not in free_set
                 and sid in alloc}

    # ── Pre-compute per-block fixed-student contributions ─────────────────
    n_fixed       = [0] * n_b
    n_fixed_male  = [0] * n_b
    n_fixed_small = [0] * n_b
    for sid, bid in fixed_map.items():
        if bid not in b_pos:
            continue
        j = b_pos[bid]
        i = s_pos[sid]
        n_fixed[j]       += 1
        n_fixed_male[j]  += int(male_bin[i])
        n_fixed_small[j] += int(small_pref[i])

    # ── Free student index (row indices only for free students) ───────────
    free_rows = [s_pos[sid] for sid in students if sid in free_set]
    n_free    = len(free_rows)
    if n_free == 0:
        return alloc.copy(), _compute_obj(alloc, df_prefs, df_info,
                                          s_pos, b_pos, com_mult,
                                          friend_w, block_w)

    # ── RA pins among free students ───────────────────────────────────────
    ra_pins = {}   # row_i → col_j
    if df_ra is not None:
        for _, row in df_ra.iterrows():
            ra_s = int(row["ra"])
            ra_b = int(row["block"])
            if ra_s in free_set and ra_s in s_pos and ra_b in b_pos:
                ra_pins[s_pos[ra_s]] = b_pos[ra_b]

    # ── Co-location constraints among free students ───────────────────────
    # Only apply co-location if ALL members of a community are free.
    repair_locked = []
    for comm in locked_communities:
        comm_sids = {students[ri] for ri in comm}
        if comm_sids.issubset(free_set):
            repair_locked.append(comm)
        # If some are fixed, their blocks are already determined — the free
        # members of the community must go to the same block as the fixed rep.
        else:
            fixed_members = [students[ri] for ri in comm
                             if students[ri] not in free_set]
            if fixed_members:
                target_bid = fixed_map.get(fixed_members[0])
                if target_bid is not None and target_bid in b_pos:
                    target_j = b_pos[target_bid]
                    for ri in comm:
                        if students[ri] in free_set:
                            ra_pins[ri] = target_j  # force to same block

    has_friend = {
        (i, k): not pd.isna(df_prefs.iloc[i][f"friend_request_{k+1}"])
        for i in free_rows for k in range(4)
    }

    # ── Build sub-MIP ─────────────────────────────────────────────────────
    model = pulp.LpProblem("repair", pulp.LpMaximize)

    x = pulp.LpVariable.dicts(
        "x",
        [(i, j) for i in free_rows for j in range(n_b)],
        cat="Binary",
    )
    f = pulp.LpVariable.dicts(
        "f",
        [(i, k) for i in free_rows for k in range(4)
         if has_friend.get((i, k), False)],
        cat="Binary",
    )
    bp = pulp.LpVariable.dicts(
        "bp",
        [(i, k) for i in free_rows for k in range(2)],
        cat="Binary",
    )
    pref_slack = pulp.LpVariable.dicts(
        "ps", free_rows, lowBound=0, cat="Integer"
    )
    small_slack  = pulp.LpVariable.dicts("ss",  range(n_b), lowBound=0, upBound=2, cat="Integer")
    block_low_sl = pulp.LpVariable.dicts("bls", range(n_b), lowBound=0, cat="Integer")
    block_up_sl  = pulp.LpVariable.dicts("bus", range(n_b), lowBound=0, cat="Integer")

    # ── Objective ─────────────────────────────────────────────────────────
    # Friend preferences: free→free and free→fixed cross-terms
    friend_obj_terms = []
    for i in free_rows:
        for k in range(4):
            if not has_friend.get((i, k), False):
                continue
            fv  = df_prefs.iloc[i][f"friend_request_{k+1}"]
            fid = int(fv)
            w   = com_mult[i] * friend_w[i, k]
            if w == 0:
                continue
            friend_obj_terms.append(w * f[i, k])

    block_obj_terms = [
        com_mult[i] * block_w[i, k] * bp[i, k]
        for i in free_rows for k in range(2)
    ]
    slack_terms = (
          W_PREF_SLACK  * pulp.lpSum(pref_slack[i]                        for i in free_rows)
        + W_CAP_SLACK   * pulp.lpSum(block_low_sl[j] + block_up_sl[j]     for j in range(n_b))
        + W_SMALL_SLACK * pulp.lpSum(small_slack[j]                        for j in range(n_b))
    )
    model += pulp.lpSum(friend_obj_terms) + pulp.lpSum(block_obj_terms) - slack_terms

    # ── Partition ─────────────────────────────────────────────────────────
    for i in free_rows:
        model += pulp.lpSum(x[i, j] for j in range(n_b)) == 1

    # ── Friend preference constraints ─────────────────────────────────────
    for i in free_rows:
        for k in range(4):
            if not has_friend.get((i, k), False):
                continue
            fv  = df_prefs.iloc[i][f"friend_request_{k+1}"]
            fid = int(fv)
            fi  = s_pos.get(fid)

            if fid in free_set:
                # free→free: v2 style  f[i,k] <= 1 - x[i,j] + x[fi,j]
                for j in range(n_b):
                    model += f[i, k] <= 1 - x[i, j] + x[fi, j]
            else:
                # free→fixed: friend is fixed in block b_fixed
                b_fixed = fixed_map.get(fid)
                if b_fixed is not None and b_fixed in b_pos:
                    j_fixed = b_pos[b_fixed]
                    model += f[i, k] <= x[i, j_fixed]
                else:
                    model += f[i, k] == 0   # friend unassigned or unknown

    # ── Block preference constraints ──────────────────────────────────────
    for i in free_rows:
        for k in range(2):
            bv = df_prefs.iloc[i].get(f"block_request_{k+1}")
            if pd.isna(bv):
                model += bp[i, k] == 0
            else:
                bj = b_pos.get(int(bv))
                if bj is not None:
                    model += bp[i, k] <= x[i, bj]
                else:
                    model += bp[i, k] == 0

    # ── Capacity constraints (adjusted for fixed students) ────────────────
    for j in range(n_b):
        free_total_j = pulp.lpSum(x[i, j] for i in free_rows)
        cap_remaining = block_cap[j] - n_fixed[j]

        # Hard ceiling
        model += free_total_j <= max(0, cap_remaining)
        # Soft upper
        model += free_total_j <= block_cap[j] * block_cap_up[j] - n_fixed[j] + block_up_sl[j]
        # Soft lower
        model += free_total_j + block_low_sl[j] >= block_cap[j] * block_cap_low[j] - n_fixed[j]

    # ── Gender ratio constraints (adjusted for fixed students) ────────────
    for j in range(n_b):
        free_males_j = pulp.lpSum(x[i, j] * int(male_bin[i]) for i in free_rows)
        free_total_j = pulp.lpSum(x[i, j] for i in free_rows)
        # (free_males + fixed_males) <= cap_up * (free_total + fixed_total)
        # → free_males - cap_up * free_total <= cap_up * n_fixed - n_fixed_male
        model += (free_males_j - male_cap_up[j]  * free_total_j
                  <= male_cap_up[j]  * n_fixed[j] - n_fixed_male[j])
        model += (free_males_j - male_cap_low[j] * free_total_j
                  >= male_cap_low[j] * n_fixed[j] - n_fixed_male[j])

    # ── Small room constraints (adjusted for fixed students) ──────────────
    for j in range(n_b):
        free_small_j = pulp.lpSum(x[i, j] * int(small_pref[i]) for i in free_rows)
        model += (free_small_j
                  <= max(0, small_room_cap[j] - n_fixed_small[j]) + small_slack[j])

    # ── At-least-one preference met ───────────────────────────────────────
    for i in free_rows:
        fp1 = df_prefs.iloc[i]["points_per_friend_1"]
        bp1 = df_prefs.iloc[i]["points_per_block_1"]
        cm  = df_prefs.iloc[i]["community_mult"]
        if fp1 + bp1 >= 1 and cm >= 0.01:
            f_terms = [f[i, k] for k in range(4)
                       if has_friend.get((i, k), False)]
            model += (pulp.lpSum(f_terms)
                      + pulp.lpSum(bp[i, k] for k in range(2))
                      + pref_slack[i] >= 1)

    # ── RA pins ───────────────────────────────────────────────────────────
    for i, j in ra_pins.items():
        if i in free_rows:
            model += x[i, j] == 1
            for jj in range(n_b):
                if jj != j:
                    model += x[i, jj] == 0

    # ── Co-location among free students ───────────────────────────────────
    for comm in repair_locked:
        members = sorted(comm)
        rep = members[0]
        for ik in members[1:]:
            if ik in free_rows and rep in free_rows:
                for j in range(n_b):
                    model += x[ik, j] == x[rep, j]

    # ── Solve ─────────────────────────────────────────────────────────────
    solver = _get_solver(solver_name, time_limit, verbose)
    model.solve(solver)

    status = pulp.LpStatus[model.status]
    if status not in ("Optimal", "Not Solved") and model.status != 1:
        # No feasible solution found
        return None, None

    # ── Extract solution ──────────────────────────────────────────────────
    new_alloc = dict(alloc)   # start from current (keeps fixed)
    for i in free_rows:
        for j in range(n_b):
            if (pulp.value(x.get((i, j))) or 0) > 0.5:
                new_alloc[students[i]] = blocks[j]
                break

    new_obj = _compute_obj(new_alloc, df_prefs, df_info,
                           s_pos, b_pos, com_mult, friend_w, block_w)
    return new_alloc, new_obj


# ── Main LNS entry point ──────────────────────────────────────────────────────

def lns_solve(df_prefs, df_info, df_ra,
              time_limit:       int   = 30,
              solver_name:      str   = "SCIP",
              initial_time:     int   = 8,
              repair_time:      int   = 4,
              destroy_frac:     float = 0.25,
              verbose:          bool  = False,
              use_clique_lock:  bool  = False,
              seed:             int   = 42,
              no_improve_limit: int   = 100,
              **clique_kwargs):
    """
    Large Neighbourhood Search for room allocation.

    Parameters
    ----------
    time_limit    : total wall-clock budget in seconds (default 30)
    solver_name   : "SCIP", "HIGHS", or "CBC"
    initial_time  : time limit for the initial MIP solve (default 8)
    repair_time   : time limit per repair MIP call (default 4)
    destroy_frac  : fraction of students destroyed per random iteration (default 0.25)
    verbose       : stream solver output
    use_clique_lock: pre-lock friend communities before LNS
    seed          : random seed for reproducibility

    Returns
    -------
    dict  matching the shape of build_and_solve's result dict, with an
    additional "lns_iterations" key reporting how many repair calls were made.
    """
    t_start = time.time()
    rng     = random.Random(seed)

    students  = list(df_prefs["student"].astype(int))
    blocks    = list(df_info["block"].astype(int))
    n_s, n_b  = len(students), len(blocks)
    s_pos     = {s: i for i, s in enumerate(students)}
    b_pos     = {b: j for j, b in enumerate(blocks)}

    male_bin       = df_prefs["male"].values.astype(int)
    small_pref     = df_prefs["small_room"].values.astype(int)
    com_mult       = df_prefs["community_mult"].values.astype(float)
    block_cap      = df_info["capacity"].values.astype(int)
    block_cap_low  = df_info["block_cap_low"].values.astype(float)
    block_cap_up   = df_info["block_cap_up"].values.astype(float)
    male_cap_low   = df_info["male_cap_low"].values.astype(float)
    male_cap_up    = df_info["male_cap_up"].values.astype(float)
    small_room_cap = df_info["small_room_cap"].values.astype(int)

    friend_w = df_prefs[
        [f"points_per_friend_{k}" for k in range(1, 5)]
    ].values.astype(float)
    block_w = df_prefs[
        [f"points_per_block_{k}" for k in range(1, 3)]
    ].values.astype(float)

    # ── Clique locking ────────────────────────────────────────────────────
    # clique_kwargs arrive with "clique_" prefix from run_benchmark/CLI.
    # find_lockable_communities expects plain names, so strip the prefix.
    _PREFIX = "clique_"
    _RENAME = {
        "clique_join_frac":          "join_frac",
        "clique_max_size":           "max_size",
        "clique_max_community":      "max_community",
        "clique_min_density":        "min_density",
        "clique_min_score":          "min_score",
        "clique_use_louvain":        "use_louvain",
        "clique_louvain_res":        "louvain_resolution",
        "clique_block_bonus":        "block_bonus",
        "clique_block_coherence_min":"block_coherence_min",
    }
    lockable_kwargs = {
        _RENAME.get(k, k): v
        for k, v in clique_kwargs.items()
        if _RENAME.get(k, k) not in ("clique_singleton_removal_score",)
    }

    locked_communities = []
    if use_clique_lock:
        from .clique_utils import find_lockable_communities
        locked_communities = find_lockable_communities(
            df_prefs, df_info, df_ra, **lockable_kwargs
        )
        if locked_communities:
            n_locked = sum(len(c) for c in locked_communities)
            print(f"  Locked {len(locked_communities)} communities "
                  f"({n_locked} students)")

    # ── Phase 1: Initial solution via short MIP ───────────────────────────
    t_init = min(initial_time, time_limit * 0.4)
    print(f"\n[LNS] Phase 1: initial MIP ({t_init:.0f}s limit) ...")
    init_result = build_and_solve(
        df_prefs, df_info, df_ra,
        time_limit=int(t_init),
        solver_name=solver_name,
        verbose=verbose,
        formulation="v1",
        use_clique_lock=use_clique_lock,
        **{k: v for k, v in clique_kwargs.items()},
    )

    if not init_result.get("ok", True) or not init_result.get("alloc"):
        print("  [LNS] Initial MIP found no solution — aborting LNS.")
        return init_result

    current_alloc = dict(init_result["alloc"])
    current_obj   = _compute_obj(current_alloc, df_prefs, df_info,
                                 s_pos, b_pos, com_mult, friend_w, block_w)
    best_alloc    = dict(current_alloc)
    best_obj      = current_obj

    print(f"  [LNS] Initial obj: {current_obj:.3f}  "
          f"(MIP status: {init_result.get('status', '?')}  "
          f"elapsed: {time.time()-t_start:.1f}s)")

    if init_result.get("status") == "Optimal":
        print("  [LNS] MIP proved optimal — skipping LNS phase.")
        init_result["lns_iterations"] = 0
        init_result["n_locked"]       = sum(len(c) for c in locked_communities)
        return init_result

    # ── Phase 2: LNS loop ─────────────────────────────────────────────────
    iteration          = 0
    improvements       = 0
    iters_no_improve   = 0
    op_counts   = {"random": 0, "block": 0}
    op_wins     = {"random": 0, "block": 0}

    while True:
        elapsed   = time.time() - t_start
        remaining = time_limit - elapsed
        if remaining < repair_time + 0.5:
            break
        if no_improve_limit > 0 and iters_no_improve >= no_improve_limit:
            print(f"  [LNS] Early stop: no improvement for {iters_no_improve} iterations.")
            break

        this_repair = min(repair_time, remaining - 0.5)

        # Choose destroy operator (adaptive: weight by past improvement rate)
        r_rate = (op_wins["random"] + 1) / (op_counts["random"] + 2)
        b_rate = (op_wins["block"]  + 1) / (op_counts["block"]  + 2)
        use_block = rng.random() < (b_rate / (r_rate + b_rate))

        if use_block:
            free_sids = _destroy_block_worst(
                current_alloc, students, df_prefs, s_pos, b_pos,
                com_mult, friend_w,
            )
            op = "block"
        else:
            free_sids = _destroy_random(current_alloc, students, destroy_frac, rng)
            op = "random"

        op_counts[op] += 1

        print(f"  [LNS] iter {iteration+1:3d}  op={op:6s}  "
              f"free={len(free_sids):3d}  "
              f"repair_limit={this_repair:.1f}s  "
              f"best={best_obj:.3f}  elapsed={elapsed:.1f}s")

        new_alloc, new_obj = _repair(
            df_prefs, df_info, df_ra,
            current_alloc, free_sids,
            students, blocks, n_s, n_b, s_pos, b_pos,
            male_bin, small_pref, com_mult, friend_w, block_w,
            block_cap, block_cap_low, block_cap_up,
            male_cap_low, male_cap_up, small_room_cap,
            locked_communities,
            time_limit=int(math.ceil(this_repair)),
            solver_name=solver_name,
            verbose=verbose,
        )

        if new_alloc is not None and new_obj >= current_obj - 1e-6:
            current_alloc = new_alloc
            current_obj   = new_obj
            if new_obj > best_obj + 1e-6:
                best_alloc       = dict(new_alloc)
                best_obj         = new_obj
                op_wins[op]     += 1
                improvements     += 1
                iters_no_improve  = 0
                print(f"    ✓ improved to {best_obj:.3f}")
            else:
                iters_no_improve += 1
        else:
            iters_no_improve += 1

        iteration += 1

    total_time = time.time() - t_start
    print(f"\n[LNS] Done.  {iteration} iterations, {improvements} improvements, "
          f"{total_time:.1f}s total.  Best obj: {best_obj:.3f}")

    # ── Return result in build_and_solve format ───────────────────────────
    # Patch the initial result with the best alloc found
    init_result["alloc"]          = best_alloc
    init_result["objective"]      = best_obj
    init_result["solve_time"]     = total_time
    init_result["status"]         = "LNS"
    init_result["lns_iterations"] = iteration
    init_result["n_locked"]       = sum(len(c) for c in locked_communities)
    return init_result
