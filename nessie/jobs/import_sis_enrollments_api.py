"""
Copyright ©2018. The Regents of the University of California (Regents). All Rights Reserved.

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


"""Logic for SIS enrollments API import job."""

import json

from flask import current_app as app
from nessie.externals import redshift, s3, sis_enrollments_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.berkeley import current_term_id
from nessie.lib.queries import get_all_student_ids
from nessie.lib.util import get_s3_sis_api_daily_path, resolve_sql_template_string


class ImportSisEnrollmentsApi(BackgroundJob):

    destination_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self, csids=None, term_id=None):
        if not csids:
            csids = [row['sid'] for row in get_all_student_ids()]
        if not term_id:
            term_id = current_term_id()
        app.logger.info(f'Starting SIS enrollments API import job for term {term_id}, {len(csids)} students...')

        rows = []
        success_count = 0
        no_enrollments_count = 0
        failure_count = 0
        index = 1
        for csid in csids:
            app.logger.info(f'Fetching SIS enrollments API for SID {csid}, term {term_id} ({index} of {len(csids)})')
            feed = sis_enrollments_api.get_drops_and_midterms(csid, term_id)
            if feed:
                success_count += 1
                rows.append('\t'.join([str(csid), str(term_id), json.dumps(feed)]))
            elif feed is False:
                app.logger.info(f'SID {csid} returned no enrollments for term {term_id}.')
                no_enrollments_count += 1
            else:
                failure_count += 1
                app.logger.error(f'SIS enrollments API import failed for CSID {csid}.')
            index += 1

        s3_key = f'{get_s3_sis_api_daily_path()}/drops_and_midterms_{term_id}.tsv'
        app.logger.info(f'Will stash {success_count} feeds in S3: {s3_key}')
        if not s3.upload_data('\n'.join(rows), s3_key):
            app.logger.error('Error on S3 upload: aborting job.')
            return False

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.execute(f"DELETE FROM {self.destination_schema}_staging.sis_api_drops_and_midterms WHERE term_id = '{term_id}'"):
            app.logger.error('Error truncating old staging rows: aborting job.')
            return False
        if not redshift.copy_tsv_from_s3(f'{self.destination_schema}_staging.sis_api_drops_and_midterms', s3_key):
            app.logger.error('Error on Redshift copy: aborting job.')
            return False
        staging_to_destination_query = resolve_sql_template_string(
            """
            DELETE FROM {redshift_schema_student}.sis_api_drops_and_midterms
                WHERE term_id = '{term_id}'
                AND sid IN
                (SELECT sid FROM {redshift_schema_student}_staging.sis_api_drops_and_midterms WHERE term_id = '{term_id}');
            INSERT INTO {redshift_schema_student}.sis_api_drops_and_midterms
                (SELECT * FROM {redshift_schema_student}_staging.sis_api_drops_and_midterms WHERE term_id = '{term_id}');
            DELETE FROM {redshift_schema_student}_staging.sis_api_drops_and_midterms
                WHERE term_id = '{term_id}';
            """,
            term_id=term_id,
        )
        if not redshift.execute(staging_to_destination_query):
            app.logger.error('Error on Redshift copy: aborting job.')
            return False

        return (
            f'SIS enrollments API import completed for term {term_id}: {success_count} succeeded, '
            f'{no_enrollments_count} returned no enrollments, {failure_count} failed.'
        )
