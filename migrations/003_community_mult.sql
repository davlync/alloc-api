-- Migration 003: add community_mult to students
-- Run in Supabase SQL Editor

alter table students
  add column if not exists community_mult float not null default 0.1;
