-- ============================================================
-- ChrisTreasurer — Supabase schema
-- Run this in the Supabase SQL editor (Project → SQL Editor → New query)
-- ============================================================

-- 1. Colleges (multi-tenancy anchor)
create table colleges (
  id uuid primary key default gen_random_uuid(),
  name text not null,
  created_at timestamptz default now()
);

-- 2. Semesters (one per allocation period, child of college)
create table semesters (
  id            uuid primary key default gen_random_uuid(),
  college_id    uuid references colleges(id) on delete cascade not null,
  name          text not null,
  academic_year text not null,
  start_date    date not null,
  created_at    timestamptz default now()
);

-- 3. Blocks (buildings / staircases within a college, scoped to a semester)
create table blocks (
  id uuid primary key default gen_random_uuid(),
  college_id  uuid references colleges(id) on delete cascade not null,
  semester_id uuid references semesters(id) on delete cascade,
  name text not null,
  block_cap_low   float not null default 0.3,
  block_cap_up    float not null default 0.9,
  male_cap_low    float not null default 0.4,
  male_cap_up     float not null default 0.6,
  small_room_cap  integer not null default 0,
  created_at timestamptz default now(),
  constraint blocks_college_semester_name_key unique nulls not distinct (college_id, semester_id, name)
);

-- 4. Rooms
create table rooms (
  id uuid primary key default gen_random_uuid(),
  college_id  uuid references colleges(id) on delete cascade not null,
  semester_id uuid references semesters(id) on delete cascade,
  block_id    uuid references blocks(id) on delete cascade not null,
  room_number text not null,
  floor int not null default 0,
  room_type text not null check (room_type in ('en-suite', 'shared-bathroom', 'studio')),
  is_accessible boolean not null default false,
  is_available boolean not null default true,
  created_at timestamptz default now()
);

-- 5. Students
create table students (
  id uuid primary key default gen_random_uuid(),
  college_id  uuid references colleges(id) on delete cascade not null,
  semester_id uuid references semesters(id) on delete cascade,
  name text not null,
  email text not null,
  year int not null check (year between 1 and 5),
  is_ra boolean not null default false,
  accessibility_required boolean not null default false,
  -- Algorithm input fields
  male               boolean,
  small_room         boolean not null default false,
  friend_request_1   text,
  friend_request_2   text,
  friend_request_3   text,
  friend_request_4   text,
  enemy_request_1    text,
  enemy_request_2    text,
  enemy_request_3    text,
  enemy_request_4    text,
  block_request_1    text,
  block_request_2    text,
  ra_block_id        uuid references blocks(id),
  community_mult     float not null default 0.1,
  preference_token uuid not null default gen_random_uuid(),
  created_at timestamptz default now(),
  constraint students_college_semester_email_key unique nulls not distinct (college_id, semester_id, email)
);

-- 6. Student preferences (table exists; form is disabled pending unique-link feature)
create table student_preferences (
  id uuid primary key default gen_random_uuid(),
  student_id uuid references students(id) on delete cascade not null unique,
  preferred_block_id uuid references blocks(id),
  preferred_room_type text check (preferred_room_type in ('en-suite', 'shared-bathroom', 'studio', 'no-preference')),
  friend_requests text[],
  additional_notes text,
  submitted_at timestamptz default now()
);

-- 7. Rules / constraints
create table rules (
  id uuid primary key default gen_random_uuid(),
  college_id  uuid references colleges(id) on delete cascade not null,
  semester_id uuid references semesters(id) on delete cascade,
  name text not null,
  rule_type text not null check (rule_type in ('hard', 'soft')),
  category text not null,
  config jsonb not null default '{}',
  is_active boolean not null default true,
  created_at timestamptz default now()
);

-- 8. Allocation runs
create table allocation_runs (
  id uuid primary key default gen_random_uuid(),
  college_id  uuid references colleges(id) on delete cascade not null,
  semester_id uuid references semesters(id) on delete cascade,
  cohort text not null check (cohort in ('first-years', 'all')),
  status text not null default 'pending' check (status in ('pending', 'running', 'complete', 'failed')),
  stats jsonb,
  warnings jsonb,
  created_at timestamptz default now(),
  completed_at timestamptz
);

-- 9. Allocations (results — one row per student per run)
create table allocations (
  id uuid primary key default gen_random_uuid(),
  run_id uuid references allocation_runs(id) on delete cascade not null,
  student_id uuid references students(id) not null,
  room_id uuid references rooms(id) not null,
  is_flagged boolean not null default false,
  flag_reason text,
  created_at timestamptz default now(),
  unique(run_id, student_id),
  unique(run_id, room_id)
);

-- ============================================================
-- Seed data
-- ============================================================

-- College
insert into colleges (id, name) values
  ('00000000-0000-0000-0000-000000000001', 'Christ''s College');

-- Blocks
insert into blocks (id, college_id, name) values
  ('10000000-0000-0000-0000-000000000001', '00000000-0000-0000-0000-000000000001', 'Block A'),
  ('10000000-0000-0000-0000-000000000002', '00000000-0000-0000-0000-000000000001', 'Block B'),
  ('10000000-0000-0000-0000-000000000003', '00000000-0000-0000-0000-000000000001', 'Block C');

-- Rooms: ~17 per block = 51 total, mix of types, a few accessible
insert into rooms (college_id, block_id, room_number, floor, room_type, is_accessible) values
-- Block A (en-suite heavy)
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A1',0,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A2',0,'en-suite',true),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A3',0,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A4',1,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A5',1,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A6',1,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A7',1,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A8',2,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A9',2,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A10',2,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A11',2,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A12',3,'studio',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A13',3,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A14',3,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A15',3,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A16',4,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000001','A17',4,'shared-bathroom',false),
-- Block B (mixed)
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B1',0,'shared-bathroom',true),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B2',0,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B3',0,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B4',1,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B5',1,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B6',1,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B7',1,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B8',2,'studio',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B9',2,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B10',2,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B11',2,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B12',3,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B13',3,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B14',3,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B15',3,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B16',4,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000002','B17',4,'studio',false),
-- Block C (shared-bathroom heavy)
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C1',0,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C2',0,'shared-bathroom',true),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C3',0,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C4',1,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C5',1,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C6',1,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C7',1,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C8',2,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C9',2,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C10',2,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C11',2,'studio',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C12',3,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C13',3,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C14',3,'shared-bathroom',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C15',3,'en-suite',false),
('00000000-0000-0000-0000-000000000001','10000000-0000-0000-0000-000000000003','C16',4,'shared-bathroom',false);

-- 50 Students (mix of years 1-3, 5 RAs, 4 accessibility)
insert into students (college_id, name, email, year, is_ra, accessibility_required) values
('00000000-0000-0000-0000-000000000001','Alice Hartley','a.hartley@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Ben Okafor','b.okafor@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Chloe Zhang','c.zhang@christs.cam.ac.uk',1,false,true),
('00000000-0000-0000-0000-000000000001','Daniel Ferreira','d.ferreira@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Emma Nwosu','e.nwosu@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Finn Gallagher','f.gallagher@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Grace Liu','g.liu@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Henry Osei','h.osei@christs.cam.ac.uk',1,false,true),
('00000000-0000-0000-0000-000000000001','Isla McPherson','i.mcpherson@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Jake Torres','j.torres@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Kira Patel','k.patel@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Luca Bianchi','l.bianchi@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Mia Andersen','m.andersen@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Noah Williams','n.williams@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Olivia Chan','o.chan@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Patrick Dube','p.dube@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Quinn Reyes','q.reyes@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Rosa Eriksson','r.eriksson@christs.cam.ac.uk',1,false,true),
('00000000-0000-0000-0000-000000000001','Sam Nakamura','s.nakamura@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Tara Singh','t.singh@christs.cam.ac.uk',1,false,false),
('00000000-0000-0000-0000-000000000001','Uma Johansson','u.johansson@christs.cam.ac.uk',2,true,false),
('00000000-0000-0000-0000-000000000001','Victor Mensah','v.mensah@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Wendy Park','w.park@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Xander Brooks','x.brooks@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Yara El-Amin','y.elamin@christs.cam.ac.uk',2,true,false),
('00000000-0000-0000-0000-000000000001','Zoe Kowalski','z.kowalski@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Aaron Diallo','a.diallo@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Beth Carlsson','b.carlsson@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Carlos Vega','c.vega@christs.cam.ac.uk',2,false,true),
('00000000-0000-0000-0000-000000000001','Diana Obi','d.obi@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Ethan Murphy','e.murphy@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Fatima Hassan','f.hassan@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','George Lam','g.lam@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Hannah Novak','h.novak@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Ibrahim Sow','i.sow@christs.cam.ac.uk',2,true,false),
('00000000-0000-0000-0000-000000000001','Jade Wilson','j.wilson@christs.cam.ac.uk',2,false,false),
('00000000-0000-0000-0000-000000000001','Kai Nguyen','k.nguyen@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Laura Schmidt','l.schmidt@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Marcus Adeyemi','m.adeyemi@christs.cam.ac.uk',3,true,false),
('00000000-0000-0000-0000-000000000001','Nina Petrov','n.petrov@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Oscar Flynn','o.flynn@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Priya Sharma','p.sharma@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Rajan Mehta','r.mehta@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Sofia Russo','s.russo@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Tom Okonkwo','t.okonkwo@christs.cam.ac.uk',3,true,false),
('00000000-0000-0000-0000-000000000001','Ursula Blanc','u.blanc@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Vladimir Kim','v.kim@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Willow Tang','w.tang@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Xavier Dubois','x.dubois@christs.cam.ac.uk',3,false,false),
('00000000-0000-0000-0000-000000000001','Yasmin Oduya','y.oduya@christs.cam.ac.uk',3,false,false);

-- Default rules
insert into rules (college_id, name, rule_type, category, config) values
('00000000-0000-0000-0000-000000000001',
 'Accessibility minimum',
 'hard', 'accessibility',
 '{"min_accessible_rooms": 8}'),
('00000000-0000-0000-0000-000000000001',
 'RA ratio',
 'hard', 'ra_ratio',
 '{"ra_per_n_students": 15}'),
('00000000-0000-0000-0000-000000000001',
 'Year mixing ratio',
 'soft', 'year_ratio',
 '{"returning_per_first_year": 4}'),
('00000000-0000-0000-0000-000000000001',
 'Gender floors',
 'soft', 'gender',
 '{"enabled": true}');
