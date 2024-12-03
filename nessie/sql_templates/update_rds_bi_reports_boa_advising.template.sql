/**
 * Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.
 *
 * Permission to use, copy, modify, and distribute this software and its documentation
 * for educational, research, and not-for-profit purposes, without fee and without a
 * signed licensing agreement, is hereby granted, provided that the above copyright
 * notice, this paragraph and the following two paragraphs appear in all copies,
 * modifications, and distributions.
 *
 * Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
 * Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
 * http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.
 *
 * IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
 * INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
 * THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
 * OF THE POSSIBILITY OF SUCH DAMAGE.
 *
 * REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
 * SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
 * "AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
 * ENHANCEMENTS, OR MODIFICATIONS.
 */


----------------------------------------------------------------------------------------------------
-- BEGIN script for creating and populating RDS schema/tables for Advising Notes Dashboard
----------------------------------------------------------------------------------------------------


----------------------------------------------------------------------------------------------------
-- CREATE SCHEMA: "rds_schema_bi_reports_boa_advising"
----------------------------------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS {rds_schema_bi_reports_boa_advising};
GRANT USAGE ON SCHEMA {rds_schema_bi_reports_boa_advising} TO {rds_app_tableau_user};
ALTER DEFAULT PRIVILEGES IN SCHEMA {rds_schema_bi_reports_boa_advising}
  GRANT SELECT ON TABLES TO {rds_app_tableau_user};


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: notes
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.notes CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.notes (
  note_id INTEGER,
  created_at TIMESTAMP WITH TIME ZONE,
  created_at_date_pst DATE,
  created_at_time_pst VARCHAR(12),
  set_date DATE,
  author_uid VARCHAR(255),
  note_author_name VARCHAR(255),
  author_dept_code VARCHAR(255),
  author_role VARCHAR(255),
  contact_type VARCHAR(40),
  is_private BOOLEAN,
  sid VARCHAR(80),
  subject VARCHAR(255)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.notes (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      note_id,
      created_at,
      created_at_date_pst,
      created_at_time_pst,
      set_date,
      author_uid,
      note_author_name,
      author_dept_code,
      author_role,
      contact_type,
      is_private,
      sid,
      subject
    FROM {redshift_schema_bi_reports_boa_advising}.notes
  $REDSHIFT$)
  AS notes (
    note_id INTEGER,
    created_at TIMESTAMP WITH TIME ZONE,
    created_at_date_pst DATE,
    created_at_time_pst VARCHAR(12),
    set_date DATE,
    author_uid VARCHAR(255),
    note_author_name VARCHAR(255),
    author_dept_code VARCHAR(255),
    author_role VARCHAR(255),
    contact_type VARCHAR(40),
    is_private BOOLEAN,
    sid VARCHAR(80),
    subject VARCHAR(255)
  )
);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: authors
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.authors CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.authors (
  author_uid VARCHAR(255),
  author_name VARCHAR(65535),
  author_aliases VARCHAR(65535)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.authors (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      author_uid,
      author_name,
      author_aliases
    FROM {redshift_schema_bi_reports_boa_advising}.authors
  $REDSHIFT$)
  AS authors (
    author_uid VARCHAR(255),
    author_name VARCHAR(65535),
    author_aliases VARCHAR(65535)
  )
);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: departments
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.departments CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.departments (
  dept_code VARCHAR(255),
  dept_name VARCHAR(255)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.departments (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      dept_code,
      dept_name
    FROM {redshift_schema_bi_reports_boa_advising}.departments
  $REDSHIFT$)
  AS departments (
    dept_code VARCHAR(255),
    dept_name VARCHAR(255)
  )
);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: note_topics
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.note_topics CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.note_topics (
  note_id INTEGER,
  topic VARCHAR(50)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.note_topics (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      note_id,
      topic
    FROM {redshift_schema_bi_reports_boa_advising}.note_topics
  $REDSHIFT$)
  AS note_topics (
    note_id INTEGER,
    topic VARCHAR(50)
  )
);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: student_groups
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.student_groups CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.student_groups (
  student_group_id INTEGER,
  student_group_name VARCHAR(255),
  sid VARCHAR(80)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.student_groups (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      student_group_id,
      student_group_name,
      sid
    FROM {redshift_schema_bi_reports_boa_advising}.student_groups
  $REDSHIFT$)
  AS student_groups (
    student_group_id INTEGER,
    student_group_name VARCHAR(255),
    sid VARCHAR(80)
  )
);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: student_cohorts
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.student_cohorts CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.student_cohorts (
  cohort_id INTEGER,
  cohort_name VARCHAR(255),
  sid VARCHAR(255)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.student_cohorts (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      cohort_id,
      cohort_name,
      sid
    FROM {redshift_schema_bi_reports_boa_advising}.student_cohorts
  $REDSHIFT$)
  AS student_cohorts (
    cohort_id INTEGER,
    cohort_name VARCHAR(255),
    sid VARCHAR(255)
  )
);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: students
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.students CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.students
(
  sid VARCHAR(80),
  student_name VARCHAR(513),
  is_manually_added BOOLEAN,
  cohort_list VARCHAR(65535),
  group_list VARCHAR(65535)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.students (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      sid,
      student_name,
      is_manually_added,
      cohort_list,
      group_list
    FROM {redshift_schema_bi_reports_boa_advising}.students
  $REDSHIFT$)
  AS students (
    sid VARCHAR(80),
    student_name VARCHAR(513),
    is_manually_added BOOLEAN,
    cohort_list VARCHAR(65535),
    group_list VARCHAR(65535)
  )
);


----------------------------------------------------------------------------------------------------
-- CREATE TABLE: bi_reports_boa_advising.student_degrees
----------------------------------------------------------------------------------------------------

DROP TABLE IF EXISTS {rds_schema_bi_reports_boa_advising}.student_degrees CASCADE;

CREATE TABLE IF NOT EXISTS {rds_schema_bi_reports_boa_advising}.student_degrees (
  sid VARCHAR(256),
  degree_date VARCHAR(65535),
  degree_awarded VARCHAR(65535),
  plan_type VARCHAR(65535),
  plan_group VARCHAR(65535)
);

INSERT INTO {rds_schema_bi_reports_boa_advising}.student_degrees (
  SELECT *
  FROM dblink('{rds_dblink_to_redshift}', $REDSHIFT$
    SELECT
      sid,
      degree_date,
      degree_awarded,
      plan_type,
      plan_group 
    FROM {redshift_schema_bi_reports_boa_advising}.student_degrees
  $REDSHIFT$)
  AS student_degrees (
    sid VARCHAR(256),
    degree_date VARCHAR(65535),
    degree_awarded VARCHAR(65535),
    plan_type VARCHAR(65535),
    plan_group VARCHAR(65535)
  )   
);


----------------------------------------------------------------------------------------------------
-- END script for creating and populating RDS schema/tables for Advising Notes Dashboard
----------------------------------------------------------------------------------------------------
