-- ============================================================
-- Migration 001: Add preference + constraint fields
-- Run in Supabase SQL Editor
-- ============================================================

-- Students: add preference and gender fields
alter table students
  add column if not exists male                boolean,
  add column if not exists small_room          boolean not null default false,
  add column if not exists friend_request_1    text,
  add column if not exists friend_request_2    text,
  add column if not exists friend_request_3    text,
  add column if not exists friend_request_4    text,
  add column if not exists enemy_request_1     text,
  add column if not exists enemy_request_2     text,
  add column if not exists enemy_request_3     text,
  add column if not exists enemy_request_4     text,
  add column if not exists block_request_1     text,
  add column if not exists block_request_2     text,
  add column if not exists ra_block_id         uuid references blocks(id);

-- Blocks: add capacity constraint parameters
alter table blocks
  add column if not exists block_cap_low   float not null default 0.3,
  add column if not exists block_cap_up    float not null default 0.9,
  add column if not exists male_cap_low    float not null default 0.4,
  add column if not exists male_cap_up     float not null default 0.6,
  add column if not exists small_room_cap  integer not null default 0;
