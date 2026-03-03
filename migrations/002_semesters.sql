-- ============================================================
-- Migration 002: Add semesters table and semester_id columns
-- Run in the Supabase SQL Editor
-- ============================================================

-- 1. Semesters table
create table if not exists semesters (
  id            uuid primary key default gen_random_uuid(),
  college_id    uuid references colleges(id) on delete cascade not null,
  name          text not null,
  academic_year text not null,
  start_date    date not null,
  created_at    timestamptz default now()
);

-- 2. Add semester_id to all dependent tables
alter table blocks          add column if not exists semester_id uuid references semesters(id) on delete cascade;
alter table rooms           add column if not exists semester_id uuid references semesters(id) on delete cascade;
alter table students        add column if not exists semester_id uuid references semesters(id) on delete cascade;
alter table rules           add column if not exists semester_id uuid references semesters(id) on delete cascade;
alter table allocation_runs add column if not exists semester_id uuid references semesters(id) on delete cascade;

-- 3. Replace old unique constraint on students with semester-aware one
--    NULLS NOT DISTINCT means NULL == NULL, preserving old (college_id, email) semantics
--    when semester_id is absent.
alter table students drop constraint if exists students_college_id_email_key;
alter table students add constraint students_college_semester_email_key
  unique nulls not distinct (college_id, semester_id, email);

-- 4. Add semester-aware unique constraint on blocks
alter table blocks add constraint blocks_college_semester_name_key
  unique nulls not distinct (college_id, semester_id, name);
