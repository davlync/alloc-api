-- Per-allocation quality fields computed at run time
alter table allocations
  add column if not exists block_matched   boolean,
  add column if not exists friend_score_pct integer;
