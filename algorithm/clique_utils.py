"""
clique_utils.py
Shared clique/community detection logic used by both clique_analysis.py
and room_allocator.py.

Public API
──────────
  build_graphs(df_prefs)
      → directed, mutual, enemy_set, s_pos, students, adj,
        directed_mip, mutual_mip

  find_strict_cliques(mutual, adj, n)          → list[frozenset]
  find_fuzzy_cliques(strict, adj, mutual,
                     enemy_set, n,
                     join_frac, max_size)       → list[frozenset]
  merge_to_communities(fuzzy, mutual,
                       enemy_set, max_community,
                       directed_mip)            → list[(frozenset, bool, int)]
  louvain_communities(mutual_w, adj, n,
                      enemy_set, resolution)    → list[(frozenset, bool, int)]
  clique_score(group, mutual)                  → float   (legacy, topology weights)
  clique_density(group, mutual)               → float
  mip_group_value(group, directed_mip)        → float   (MIP-aligned scoring)
  feasibility_check(community, df_info)       → bool

  find_lockable_communities(df_prefs, df_info, df_ra, **kwargs)
      → list[frozenset]   ← disjoint communities for co-location constraints

  find_candidate_groups(df_prefs, df_info, df_ra, **kwargs)
      → list[frozenset]   ← all quality communities (overlapping ok) for group-partition formulation

Edge weight conventions
───────────────────────
  Topology weights (used for clique-finding, fuzzy expansion, adjacency):
    w(i→j) = 5 - rank   (rank 1 → 4 pts, rank 4 → 1 pt)
    mutual strength(i,j) = w(i→j) + w(j→i)   max 8, min 2

  MIP-aligned weights (used for scoring, ranking, Louvain):
    w_mip(i→j) = com_mult[i] * POINTS[n_prefs_i][rank]
    mip_group_value(G) = sum of w_mip(i→j) for all directed edges inside G
    This mirrors the actual MIP objective: asymmetric, uses the same POINTS
    table, includes com_mult, counts one-directional preferences.

Block preference bonus
──────────────────────
  When block_bonus > 0, mutual_mip edge weights are augmented by +block_bonus
  for any pair who share the same top block preference (block_request_1).
  This makes Louvain naturally favour groups that not only like each other
  but also want to live in the same place.

  min_score threshold
  ───────────────────
  With MIP-aligned scoring, min_score is a lower bound on mip_group_value
  (the sum of satisfied-preference points the group generates if co-located).
  A value of ~6 corresponds roughly to a tight 2-person pair both listing
  each other around rank 2 (e.g. 3+3 pts with 4 preferences each).

Louvain
───────
  Lightweight Louvain modularity optimisation implemented from scratch —
  no external dependencies.  Runs on the MIP-aligned mutual_mip weights
  (optionally block-bonus augmented) so communities naturally cluster
  students whose co-location generates high objective value.
  Resolution parameter controls community granularity (lower → bigger).
"""

import math
import random
import pandas as pd
from collections import defaultdict

# Points awarded for the k-th ranked preference being satisfied,
# given that the student listed n preferences in total.
# Mirrors the POINTS table in room_allocator.py — keep in sync.
#          rank:   1   2   3   4
_POINTS = {
    4:          [ 4,  3,  2,  1],
    3:          [ 5,  3,  2,  0],
    2:          [ 6,  4,  0,  0],
    1:          [10,  0,  0,  0],
    0:          [ 0,  0,  0,  0],
}


# ── Graph construction ────────────────────────────────────────────────────────

def build_graphs(df_prefs):
    """
    Build directed friend graph, mutual subgraph, and enemy set.

    Two sets of directed weights are produced:
      directed      — topology weights (5 - rank), used for clique-finding
                      and adjacency construction (unchanged from original)
      directed_mip  — MIP-aligned weights (com_mult * POINTS[n_prefs][rank]),
                      mirroring the actual solver objective exactly

    Returns
    -------
    directed      : dict (row_i, row_j) → int weight  (topology, 5-rank)
    mutual        : dict (row_i, row_j) → int combined weight  (row_i < row_j, topology)
    enemy_set     : set of (row_i, row_j) pairs  (row_i < row_j)
    s_pos         : dict student_id → row_index
    students      : list of student ids (in row order)
    adj           : defaultdict(set)  mutual adjacency
    directed_mip  : dict (row_i, row_j) → float  (MIP-aligned weights)
    mutual_mip    : dict (row_i, row_j) → float  combined MIP weight (row_i < row_j)
    """
    students = list(df_prefs["student"].astype(int))
    s_pos    = {s: idx for idx, s in enumerate(students)}
    directed     = {}
    directed_mip = {}
    enemy_set    = set()

    # Pre-extract per-student arrays for MIP weights.
    # community_mult is always present; points_per_friend_{k} columns are added
    # by _assign_pref_weights in load_data — if missing we compute on the fly.
    com_mult      = df_prefs["community_mult"].values.astype(float)
    has_pref_cols = all(
        f"points_per_friend_{k}" in df_prefs.columns for k in range(1, 5)
    )

    for _, row in df_prefs.iterrows():
        si = s_pos[int(row["student"])]

        # Points row for fallback (if pre-computed columns are absent)
        if not has_pref_cols:
            n_friends = sum(
                pd.notna(row.get(f"friend_request_{r}")) for r in range(1, 5)
            )
            pts_row = _POINTS.get(min(n_friends, 4), [0, 0, 0, 0])

        for rank in range(1, 5):
            fv = row.get(f"friend_request_{rank}")
            if pd.notna(fv):
                sj = s_pos[int(fv)]
                # Topology weight (original, used for clique-finding)
                directed[(si, sj)] = 5 - rank
                # MIP-aligned weight: what the solver actually scores for
                # student si having their rank-k friend sj in the same block
                if has_pref_cols:
                    pts = float(row[f"points_per_friend_{rank}"])
                else:
                    pts = float(pts_row[rank - 1])
                directed_mip[(si, sj)] = pts * com_mult[si]

            ev = row.get(f"enemy_request_{rank}")
            if pd.notna(ev):
                sj = s_pos[int(ev)]
                enemy_set.add((min(si, sj), max(si, sj)))

    # Build topology mutual graph (symmetric, for clique-finding / adjacency)
    mutual = {}
    adj    = defaultdict(set)
    for (si, sj), w_ij in directed.items():
        if (sj, si) in directed:
            key = (min(si, sj), max(si, sj))
            if key not in mutual:
                mutual[key] = directed[(si, sj)] + directed[(sj, si)]
                adj[si].add(sj)
                adj[sj].add(si)

    # Build MIP-aligned mutual graph (used for Louvain and scoring)
    # Sums both directed MIP weights where a mutual friendship exists.
    # One-directional edges still appear in directed_mip but not mutual_mip.
    mutual_mip = {}
    for (si, sj) in mutual:          # only mutual pairs
        w_ij = directed_mip.get((si, sj), 0.0)
        w_ji = directed_mip.get((sj, si), 0.0)
        mutual_mip[(si, sj)] = w_ij + w_ji

    return directed, mutual, enemy_set, s_pos, students, adj, directed_mip, mutual_mip


def block_coherence(community, df_prefs, s_pos):
    """
    Fraction of community members that share the single most common top block
    preference.  Returns 1.0 if everyone wants the same block, ~1/k if they
    are evenly split across k blocks.  Members without a block preference are
    ignored; returns 1.0 for an empty community.
    """
    from collections import Counter
    counts = Counter()
    for row_idx in community:
        # look up the student id for this row index
        # s_pos maps student_id → row_idx, so invert on the fly
        pass

    # Build inverse map once per call (small communities, cheap)
    idx_to_sid = {v: k for k, v in s_pos.items()}
    sid_to_block = {}
    for _, row in df_prefs.iterrows():
        bv = row.get("block_request_1")
        if pd.notna(bv):
            sid_to_block[int(row["student"])] = int(bv)

    counts = Counter()
    for row_idx in community:
        sid = idx_to_sid.get(row_idx)
        if sid is not None and sid in sid_to_block:
            counts[sid_to_block[sid]] += 1

    if not counts:
        return 1.0
    return counts.most_common(1)[0][1] / sum(counts.values())


def _apply_block_bonus(mutual, df_prefs, s_pos, block_bonus):
    """
    Return an augmented copy of mutual with +block_bonus on edges where
    both students share the same top block preference (block_request_1).
    Only existing mutual edges are augmented — no new edges are created.
    """
    if block_bonus <= 0:
        return mutual

    top_block = {}
    for _, row in df_prefs.iterrows():
        si = s_pos[int(row["student"])]
        bv = row.get("block_request_1")
        if pd.notna(bv):
            top_block[si] = int(bv)

    augmented = dict(mutual)
    for (i, j) in list(augmented.keys()):
        if top_block.get(i) is not None and top_block.get(i) == top_block.get(j):
            augmented[(i, j)] = augmented[(i, j)] + block_bonus

    return augmented


# ── Strict cliques ────────────────────────────────────────────────────────────

def _bron_kerbosch(adj, R, P, X, out):
    if not P and not X:
        if len(R) >= 2:
            out.append(frozenset(R))
        return
    for v in list(P):
        _bron_kerbosch(adj, R | {v}, P & adj[v], X & adj[v], out)
        P.remove(v)
        X.add(v)


def find_strict_cliques(mutual, adj, n):
    """Return all maximal strict cliques (size ≥ 2), sorted largest first."""
    cliques = []
    _bron_kerbosch(adj, set(), set(range(n)), set(), cliques)
    return sorted(cliques, key=len, reverse=True)


# ── Fuzzy clique expansion ────────────────────────────────────────────────────

def _fuzzy_expand(seed, adj, mutual, enemy_set, join_frac, max_size):
    """Grow a strict seed by admitting well-connected, enemy-free candidates."""
    group   = set(seed)
    changed = True
    while changed and len(group) < max_size:
        changed = False
        candidate_conn = {}
        for m in group:
            for nb in adj[m]:
                if nb not in group:
                    candidate_conn[nb] = candidate_conn.get(nb, 0) + 1
        for cand, conn in sorted(candidate_conn.items(), key=lambda x: -x[1]):
            if len(group) >= max_size:
                break
            if conn < math.ceil(len(group) * join_frac):
                continue
            if any((min(cand, m), max(cand, m)) in enemy_set for m in group):
                continue
            group.add(cand)
            changed = True
    return frozenset(group)


def find_fuzzy_cliques(strict_cliques, adj, mutual, enemy_set,
                       n, join_frac, max_size):
    """Expand every strict clique with fuzzy logic; keep only maximal results."""
    expanded = set()
    for seed in strict_cliques:
        expanded.add(_fuzzy_expand(seed, adj, mutual, enemy_set, join_frac, max_size))
    ranked = sorted(expanded, key=len, reverse=True)
    maximal = []
    for cq in ranked:
        if not any(cq < other for other in maximal):
            maximal.append(cq)
    return maximal


# ── Scoring ───────────────────────────────────────────────────────────────────

def clique_score(group, mutual):
    """
    Legacy scoring: mean topology-weight mutual-pair strength.
    Kept for backward compatibility and structural diagnostics.
    For filtering/ranking use mip_group_value instead.
    """
    members = list(group)
    pairs = [(members[a], members[b])
             for a in range(len(members))
             for b in range(a + 1, len(members))]
    if not pairs:
        return 0.0
    scores = [mutual.get((min(i, j), max(i, j)), 0) for i, j in pairs]
    return sum(scores) / len(pairs)


def mip_group_value(group, directed_mip):
    """
    MIP-aligned group value: sum of directed_mip weights for every
    friend-preference edge that falls within the group.

    This mirrors what the solver actually scores if the group is co-located:
      sum over (i, j) in group × group, i≠j, where i listed j as a friend:
          com_mult[i] * POINTS[n_prefs_i][rank_of_j]

    Unlike clique_score this is:
      - A sum not a mean  (larger groups with strong edges score higher)
      - Asymmetric        (i listing j counts even if j didn't list i back)
      - MIP-scaled        (uses the same POINTS table and com_mult as the solver)
    """
    if len(group) < 2:
        return 0.0
    return sum(
        directed_mip[(i, j)]
        for i in group
        for j in group
        if i != j and (i, j) in directed_mip
    )


def clique_density(group, mutual):
    """Fraction of all pairs that have a mutual edge."""
    members = list(group)
    total   = len(members) * (len(members) - 1) / 2
    if total == 0:
        return 0.0
    have = sum(
        1 for a in range(len(members))
        for b in range(a + 1, len(members))
        if (min(members[a], members[b]), max(members[a], members[b])) in mutual
    )
    return have / total


def _has_enemy(group, enemy_set):
    members = list(group)
    return any(
        (min(a, b), max(a, b)) in enemy_set
        for i, a in enumerate(members)
        for b in members[i + 1:]
    )


# ── Union-find community merge (original pipeline) ────────────────────────────

def merge_to_communities(fuzzy_cliques, mutual, enemy_set, max_community_size,
                         directed_mip=None):
    """
    Merge overlapping fuzzy cliques into final lockable communities.

    Rules
    -----
    1. Union-Find: merge overlapping cliques that share a member AND have no
       enemy pair in their combined membership.
    2. Per connected component:
       a. If union ≤ max_community_size AND no enemy pairs → one community.
       b. Otherwise → greedy split by score (highest first); each student
          assigned to at most one community; stragglers left free.

    Returns
    -------
    list of (community frozenset, is_merged bool, n_source_cliques int)
    """
    if not fuzzy_cliques:
        return []

    clist = list(fuzzy_cliques)
    n     = len(clist)

    parent = list(range(n))

    def find(x):
        root = x
        while parent[root] != root:
            root = parent[root]
        while parent[x] != root:
            parent[x], x = root, parent[x]
        return root

    def union(x, y):
        rx, ry = find(x), find(y)
        if rx != ry:
            parent[ry] = rx

    for i in range(n):
        for j in range(i + 1, n):
            if clist[i] & clist[j]:
                combined = clist[i] | clist[j]
                if not _has_enemy(combined, enemy_set):
                    union(i, j)

    components = defaultdict(list)
    for i in range(n):
        components[find(i)].append(i)

    communities = []
    for _, idxs in components.items():
        comp_cliques = [clist[i] for i in idxs]
        union_all    = frozenset().union(*comp_cliques)
        n_src        = len(comp_cliques)

        if len(union_all) <= max_community_size and not _has_enemy(union_all, enemy_set):
            communities.append((union_all, n_src > 1, n_src))
            continue

        # Greedy split: sort by MIP value desc (fall back to topology score)
        def _score(c):
            if directed_mip is not None:
                return mip_group_value(c, directed_mip)
            return clique_score(c, mutual)

        scored   = sorted(comp_cliques, key=_score, reverse=True)
        assigned = set()
        for cq in scored:
            free = cq - assigned
            if len(free) < 2:
                continue
            if len(free) > max_community_size:
                free = frozenset(sorted(
                    free,
                    key=lambda s: sum(
                        1 for m in free
                        if m != s and (min(s, m), max(s, m)) in mutual
                    ),
                    reverse=True
                )[:max_community_size])
            if _has_enemy(free, enemy_set):
                continue
            communities.append((frozenset(free), False, 1))
            assigned |= free

    return communities


# ── Louvain community detection ───────────────────────────────────────────────

def _louvain_partition(mutual, adj, n, resolution=1.0, seed=42):
    """
    Lightweight Louvain modularity optimisation on a weighted mutual graph.

    Each node starts in its own community.  We iteratively move each node to
    the neighbouring community that yields the greatest modularity gain,
    repeating until no improvement.  A fixed random seed is used to make
    node-visit order deterministic.

    Parameters
    ----------
    mutual     : dict (min(i,j), max(i,j)) → weight
    adj        : defaultdict(set) adjacency
    n          : number of nodes
    resolution : controls granularity — lower → fewer, larger communities
    seed       : for reproducible node-visit ordering

    Returns
    -------
    comm : list[int] of length n — community label for each node
    """
    # Weighted degree per node
    degree = [0.0] * n
    for (i, j), w in mutual.items():
        degree[i] += w
        degree[j] += w
    m = sum(mutual.values())   # total edge weight (each edge counted once)
    if m == 0:
        return list(range(n))

    comm     = list(range(n))
    comm_deg = list(degree[:])   # total weighted degree of each community

    rng   = random.Random(seed)
    order = list(range(n))

    improved = True
    while improved:
        improved = False
        rng.shuffle(order)
        for i in order:
            if not adj[i]:
                continue
            curr_c = comm[i]
            ki     = degree[i]

            # Temporarily remove i from its community
            comm[i]         = -1
            comm_deg[curr_c] -= ki

            # Weighted connections from i to each neighbouring community
            neigh_w = defaultdict(float)
            for j in adj[i]:
                c = comm[j]
                if c >= 0:
                    neigh_w[c] += mutual.get((min(i, j), max(i, j)), 0)

            ki_curr    = neigh_w.get(curr_c, 0.0)
            sigma_curr = comm_deg[curr_c]

            # Find best community (including current)
            best_c    = curr_c
            best_gain = 0.0
            for c, ki_c in neigh_w.items():
                if c == curr_c:
                    continue
                # ΔQ = (ki_c - ki_curr)/m
                #     - resolution * ki * (comm_deg[c] - sigma_curr) / (2m²)
                gain = ((ki_c - ki_curr) / m
                        - resolution * ki * (comm_deg[c] - sigma_curr)
                        / (2.0 * m * m))
                if gain > best_gain:
                    best_gain = gain
                    best_c    = c

            comm[i]         = best_c
            comm_deg[best_c] += ki
            if best_c != curr_c:
                improved = True

    return comm


def louvain_communities(mutual_w, adj, n, enemy_set,
                        max_community_size, resolution=1.0):
    """
    Run Louvain on the (optionally block-bonus-augmented) mutual graph and
    return communities in the same format as merge_to_communities.

    Enemy pairs are checked after partition; communities containing enemies
    are dropped rather than used.

    Returns
    -------
    list of (community frozenset, is_merged=False, n_src=0)
    """
    comm_labels = _louvain_partition(mutual_w, adj, n, resolution=resolution)

    # Group nodes by label
    groups = defaultdict(set)
    for node, label in enumerate(comm_labels):
        groups[label].add(node)

    communities = []
    for members in groups.values():
        fs = frozenset(members)
        if len(fs) < 2:
            continue
        if _has_enemy(fs, enemy_set):
            continue
        # If oversized, trim to highest-degree members
        if len(fs) > max_community_size:
            fs = frozenset(sorted(
                fs,
                key=lambda s: sum(
                    mutual_w.get((min(s, m), max(s, m)), 0)
                    for m in fs if m != s
                ),
                reverse=True,
            )[:max_community_size])
        communities.append((fs, False, 0))

    return communities


# ── Deduplication ─────────────────────────────────────────────────────────────

def _dedup_communities(communities, mutual, directed_mip=None):
    """
    Remove redundant communities: if A is a proper subset of B, drop A.
    When two communities overlap (but neither contains the other), keep both —
    the solver constraints remain valid.  Sort by MIP value descending so the
    highest-value communities are processed first (and win conflicts).
    """
    sets  = [c for c, _, _ in communities]
    meta  = list(communities)
    keep  = []
    seen = set()
    for i, fs in enumerate(sets):
        if fs in seen:
            continue   # exact duplicate
        if any(fs < sets[j] for j in range(len(sets)) if i != j):
            continue   # proper subset of another → redundant
        seen.add(fs)
        keep.append(meta[i])
    # Sort by MIP value descending (fall back to legacy topology score)
    if directed_mip is not None:
        keep.sort(key=lambda t: mip_group_value(t[0], directed_mip), reverse=True)
    else:
        keep.sort(key=lambda t: clique_score(t[0], mutual), reverse=True)
    return keep


# ── Feasibility (capacity only) ───────────────────────────────────────────────

def feasibility_check(community, df_info):
    """
    Does any block have enough raw capacity to hold all community members?
    Gender ratio is intentionally excluded — the model has slack variables.
    """
    caps = df_info["capacity"].values.astype(int)
    return any(len(community) <= cap for cap in caps)


# ── Top-level entry point for the solver ─────────────────────────────────────

def find_lockable_communities(
    df_prefs,
    df_info,
    df_ra           = None,
    join_frac       = 0.5,
    max_size        = 8,
    max_community   = 5,
    min_density     = 0.5,
    min_score       = 0.5,
    use_louvain          = True,
    louvain_resolution   = 1.0,
    block_bonus          = 2.0,
    block_coherence_min  = 0.6,
):
    """
    Full pipeline: graphs → strict/fuzzy/merge + optional Louvain
                → quality filter → feasibility.

    Both the clique-based and Louvain pipelines run on the same
    (block-bonus-augmented) edge weights.  Results are merged and
    deduplicated before quality filtering.

    Pinned RAs are trimmed from communities before quality checks, so
    locking never interferes with RA pin constraints.

    Parameters
    ----------
    join_frac          : min fraction of group a fuzzy candidate must connect to
    max_size           : max size during fuzzy expansion
    max_community      : max merged community size
    min_density        : min fraction of pairs that must be mutually connected
    min_score          : min mip_group_value for a community to be kept.
                         This is the total expected MIP objective contribution
                         from co-locating the group (sum of com_mult * POINTS
                         weights for all directed friend edges inside the group).
                         Scale depends on com_mult values in the data — with
                         com_mult ≤ 0.2 (typical), good pairs score ~1–3 and
                         good larger groups score ~3–6.  Default 0.5 is a low
                         bar that keeps structurally dense groups with at least
                         a couple of meaningful preferences.
    use_louvain        : also run Louvain and merge results
    louvain_resolution : Louvain resolution (lower → bigger communities)
    block_bonus        : weight added to MIP mutual edges when students share
                         their top block preference (0 to disable).  Applied on
                         top of MIP weights so Louvain favours block-coherent
                         groups that also generate high objective value.
    block_coherence_min: min fraction of members that must share the most
                         common top block preference (0 to disable).
                         e.g. 0.6 means at least 2/3 must agree on block.

    Returns
    -------
    list of frozenset  — each is a set of row indices (not student IDs)
    """
    n = len(df_prefs)
    directed, mutual, enemy_set, s_pos, students, adj, directed_mip, mutual_mip = \
        build_graphs(df_prefs)

    # Topology graph stays plain (5-rank weights); adj is derived from it.
    # MIP-aligned mutual gets block_bonus augmentation for Louvain.
    mutual_mip_w = _apply_block_bonus(mutual_mip, df_prefs, s_pos, block_bonus)

    # Row indices of pinned RAs
    ra_rows = set()
    if df_ra is not None:
        for _, row in df_ra.iterrows():
            ra_id = int(row["ra"])
            if ra_id in s_pos:
                ra_rows.add(s_pos[ra_id])

    # ── Pipeline 1: strict → fuzzy → union-find merge ──────────────────────
    # Clique-finding uses topology adj (binary mutual edges, weights irrelevant)
    strict = find_strict_cliques(mutual, adj, n)
    fuzzy  = find_fuzzy_cliques(strict, adj, mutual, enemy_set, n,
                                join_frac, max_size)
    raw    = merge_to_communities(fuzzy, mutual, enemy_set, max_community,
                                  directed_mip=directed_mip)

    # ── Pipeline 2: Louvain (runs on MIP-aligned weights) ─────────────────
    if use_louvain:
        louvain_raw = louvain_communities(
            mutual_mip_w, adj, n, enemy_set,
            max_community_size=max_community,
            resolution=louvain_resolution,
        )
        raw = raw + louvain_raw

    # ── Deduplicate (ranked by MIP value) ─────────────────────────────────
    raw = _dedup_communities(raw, mutual, directed_mip=directed_mip)

    # ── Quality filter ─────────────────────────────────────────────────────
    lockable = []
    for cq, _is_merged, _n_src in raw:
        trimmed = cq - ra_rows
        if len(trimmed) < 2:
            continue
        if clique_density(trimmed, mutual) < min_density:
            continue
        if mip_group_value(trimmed, directed_mip) < min_score:
            continue
        if block_coherence_min > 0 and \
                block_coherence(trimmed, df_prefs, s_pos) < block_coherence_min:
            continue
        if df_info is not None and not feasibility_check(trimmed, df_info):
            continue
        lockable.append(trimmed)

    # ── Precompute for stranding check ─────────────────────────────────────
    # out_neighbours[i] = row indices of friends student i listed (directed)
    from collections import defaultdict as _dd
    out_neighbours = _dd(set)
    for (si, sj) in directed:
        out_neighbours[si].add(sj)

    # has_block_pref: use s_pos to ensure consistent positional indexing
    has_block_pref = set()
    for _, row in df_prefs.iterrows():
        bp = row.get("block_request_1")
        if pd.notna(bp):
            has_block_pref.add(s_pos[int(row["student"])])

    # ── Pre-pass: protect one community per vulnerable student ─────────────
    # A vulnerable student has no block pref and ALL of their listed friends
    # appear somewhere in the full lockable list.  If we lock every one of
    # those friend-communities, the student will be stranded.
    # Fix: mark the lowest-scoring friend-community for each vulnerable student
    # as protected so disjointification skips it, leaving at least one friend free.
    all_lockable_members = set().union(*lockable) if lockable else set()
    protected = set()   # indices into lockable that must not be locked

    for i in range(len(df_prefs)):
        if i in ra_rows or i in has_block_pref:
            continue
        friends = out_neighbours[i]
        if not friends or not friends.issubset(all_lockable_members):
            continue   # at least one friend is already free — not at risk

        # Find which lockable communities contain this student's friends
        # lockable is score-sorted high→low, so last entry = lowest score
        friend_comms = [k for k, cq in enumerate(lockable) if cq & friends]
        if not friend_comms:
            continue

        # Protect the lowest-scoring one (minimises locking value lost)
        protected.add(friend_comms[-1])

    if protected:
        print(f"  Stranding pre-pass: protecting {len(protected)} communities "
              f"to keep at least one friend free per vulnerable student")

    # ── Disjointify (skipping protected communities) ───────────────────────
    assigned = set()
    disjoint = []
    for k, cq in enumerate(lockable):
        if k in protected:
            continue
        if cq & assigned:
            continue
        disjoint.append(cq)
        assigned |= cq

    return disjoint


def find_candidate_groups(
    df_prefs,
    df_info,
    df_ra           = None,
    join_frac       = 0.5,
    max_size        = 8,
    max_community   = 5,
    min_density     = 0.5,
    min_score       = 6.0,
    use_louvain          = True,
    louvain_resolution   = 1.0,
    block_bonus          = 2.0,
    block_coherence_min  = 0.6,
):
    """
    Same pipeline as find_lockable_communities but returns ALL quality
    communities without disjointification or stranding logic.

    Used by the group-partition formulation where the MIP itself decides
    which groups to use — singletons provide the fallback so no student
    can be stranded.

    Returns
    -------
    list of (frozenset, float)  — candidate groups (row indices) and their
                                   mip_group_value scores, may overlap
    """
    n = len(df_prefs)
    directed, mutual, enemy_set, s_pos, students, adj, directed_mip, mutual_mip = \
        build_graphs(df_prefs)
    mutual_mip_w = _apply_block_bonus(mutual_mip, df_prefs, s_pos, block_bonus)

    ra_rows = set()
    if df_ra is not None:
        for _, row in df_ra.iterrows():
            ra_id = int(row["ra"])
            if ra_id in s_pos:
                ra_rows.add(s_pos[ra_id])

    strict = find_strict_cliques(mutual, adj, n)
    fuzzy  = find_fuzzy_cliques(strict, adj, mutual, enemy_set, n,
                                join_frac, max_size)
    raw    = merge_to_communities(fuzzy, mutual, enemy_set, max_community,
                                  directed_mip=directed_mip)

    if use_louvain:
        louvain_raw = louvain_communities(
            mutual_mip_w, adj, n, enemy_set,
            max_community_size=max_community,
            resolution=louvain_resolution,
        )
        raw = raw + louvain_raw

    raw = _dedup_communities(raw, mutual, directed_mip=directed_mip)

    candidates = []   # list of (frozenset, mip_score)
    for cq, _is_merged, _n_src in raw:
        trimmed = cq - ra_rows
        if len(trimmed) < 2:
            continue
        if clique_density(trimmed, mutual) < min_density:
            continue
        sc = mip_group_value(trimmed, directed_mip)
        if sc < min_score:
            continue
        if block_coherence_min > 0 and \
                block_coherence(trimmed, df_prefs, s_pos) < block_coherence_min:
            continue
        if df_info is not None and not feasibility_check(trimmed, df_info):
            continue
        candidates.append((trimmed, sc))

    return candidates   # list of (frozenset, float)
