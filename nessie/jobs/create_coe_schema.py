"""
Copyright ©2024. The Regents of the University of California (Regents). All Rights Reserved.

Permission to use, copy, modify, and distribute this software and its documentation
for educational, research, and not-for-profit purposes, without fee and without a
signed licensing agreement, is hereby granted, provided that the above copyright
notice, this paragraph and the following two paragraphs appear in all copies,
modifications, and distributions.

Contact The Office of Technology Licensing, UC Berkeley, 2150 Shattuck Avenue,
Suite 510, Berkeley, CA 94720-1620, (510) 643-7201, otl@berkeley.edu,
http://ipira.berkeley.edu/industry-info for commercial licensing opportunities.

IN NO EVENT SHALL REGENTS BE LIABLE TO ANY PARTY FOR DIRECT, INDIRECT, SPECIAL,
INCIDENTAL, OR CONSEQUENTIAL DAMAGES, INCLUDING LOST PROFITS, ARISING OUT OF
THE USE OF THIS SOFTWARE AND ITS DOCUMENTATION, EVEN IF REGENTS HAS BEEN ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.

REGENTS SPECIFICALLY DISCLAIMS ANY WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE. THE
SOFTWARE AND ACCOMPANYING DOCUMENTATION, IF ANY, PROVIDED HEREUNDER IS PROVIDED
"AS IS". REGENTS HAS NO OBLIGATION TO PROVIDE MAINTENANCE, SUPPORT, UPDATES,
ENHANCEMENTS, OR MODIFICATIONS.
"""

from itertools import groupby
import json
import operator

from flask import current_app as app
from nessie.externals import rds, redshift, s3
from nessie.jobs.background_job import BackgroundJob, BackgroundJobError, verify_external_schema
from nessie.lib.util import encoded_tsv_row, get_s3_coe_daily_path, resolve_sql_template, resolve_sql_template_string
import psycopg2


"""Logic for COE schema creation job."""


external_schema = app.config['REDSHIFT_SCHEMA_COE_EXTERNAL']
internal_schema = app.config['REDSHIFT_SCHEMA_COE']
internal_schema_identifier = psycopg2.sql.Identifier(internal_schema)
rds_schema = app.config['RDS_SCHEMA_COE']
rds_dblink_to_redshift = app.config['REDSHIFT_DATABASE'] + '_redshift'


class CreateCoeSchema(BackgroundJob):

    def run(self):
        app.logger.info('Starting COE schema creation job...')
        redshift.drop_external_schema(external_schema)
        resolved_ddl = resolve_sql_template('create_coe_schema.template.sql')
        # TODO This DDL drops and recreates the internal schema before the external schema is verified. We
        # ought to set up proper staging in conjunction with verification. It's also possible that a persistent
        # external schema isn't needed.
        if redshift.execute_ddl_script(resolved_ddl):
            app.logger.info('COE external schema created.')
            verify_external_schema(external_schema, resolved_ddl)
        else:
            raise BackgroundJobError('COE external schema creation failed.')
        coe_rows = redshift.fetch(
            'SELECT * FROM {schema}.students ORDER by sid',
            schema=internal_schema_identifier,
        )

        profile_rows = []
        index = 1
        for sid, rows_for_student in groupby(coe_rows, operator.itemgetter('sid')):
            app.logger.info(f'Generating COE profile for SID {sid} ({index} of {len(coe_rows)})')
            index += 1
            row_for_student = list(rows_for_student)[0]
            coe_profile = {
                'advisorUid': row_for_student.get('advisor_ldap_uid'),
                'gender': row_for_student.get('gender'),
                'ethnicity': row_for_student.get('ethnicity'),
                'minority': row_for_student.get('minority'),
                'didPrep': row_for_student.get('did_prep'),
                'prepEligible': row_for_student.get('prep_eligible'),
                'didTprep': row_for_student.get('did_tprep'),
                'tprepEligible': row_for_student.get('tprep_eligible'),
                'sat1read': row_for_student.get('sat1read'),
                'sat1math': row_for_student.get('sat1math'),
                'sat2math': row_for_student.get('sat2math'),
                'inMet': row_for_student.get('in_met'),
                'probation': row_for_student.get('probation'),
                'status': row_for_student.get('status'),
                'acadStatus': row_for_student.get('acad_status'),
                'acadStatusTermId': row_for_student.get('acad_status_term_id'),
                'gradTermId': row_for_student.get('grad_term_id'),
            }
            profile_rows.append(encoded_tsv_row([sid, json.dumps(coe_profile)]))

        s3_key = f'{get_s3_coe_daily_path()}/coe_profiles.tsv'
        app.logger.info(f'Will stash {len(profile_rows)} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(profile_rows, s3_key):
            raise BackgroundJobError('Error on S3 upload: aborting job.')

        app.logger.info('Will copy S3 feeds into Redshift...')
        query = resolve_sql_template_string(
            """
            COPY {redshift_schema_coe}.student_profiles
                FROM '{loch_s3_coe_data_path}/coe_profiles.tsv'
                IAM_ROLE '{redshift_iam_role}'
                DELIMITER '\\t';
            """,
        )
        if not redshift.execute(query):
            raise BackgroundJobError('Error on Redshift copy: aborting job.')

        with rds.transaction() as transaction:
            if self.refresh_rds_indexes(coe_rows, transaction):
                transaction.commit()
                app.logger.info('Refreshed RDS indexes.')
            else:
                transaction.rollback()
                raise BackgroundJobError('Error refreshing RDS indexes.')

        return 'COE internal schema created.'

    def refresh_rds_indexes(self, coe_rows, transaction):
        if len(coe_rows):
            if not transaction.execute(f'TRUNCATE {rds_schema}.students'):
                return False
            columns = [
                'sid', 'advisor_ldap_uid', 'gender', 'ethnicity', 'minority',
                'did_prep', 'prep_eligible', 'did_tprep', 'tprep_eligible',
                'sat1read', 'sat1math', 'sat2math', 'in_met',
                'probation', 'status', 'acad_status', 'acad_status_term_id', 'grad_term_id',
            ]
            result = transaction.insert_bulk(
                f'INSERT INTO {rds_schema}.students ({", ".join(columns)}) VALUES %s',
                [tuple([r[c] for c in columns]) for r in coe_rows],
            )
            if not result:
                return False

            if not transaction.execute(f'TRUNCATE {rds_schema}.student_profiles'):
                return False
            result = transaction.execute(
                f"""INSERT INTO {rds_schema}.student_profiles (
                SELECT *
                    FROM dblink('{rds_dblink_to_redshift}',$REDSHIFT$
                        SELECT sid, profile
                        FROM {internal_schema}.student_profiles
                    $REDSHIFT$)
                AS redshift_profiles (
                    sid VARCHAR,
                    profile TEXT
                ));""",
            )
            if not result:
                return False

        return True
