"""
Copyright ©2019. The Regents of the University of California (Regents). All Rights Reserved.

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

import json

from flask import current_app as app
from nessie.externals import redshift, s3, sis_degree_progress_api
from nessie.jobs.background_job import BackgroundJob
from nessie.lib.queries import get_all_student_ids
from nessie.lib.util import encoded_tsv_row, get_s3_sis_api_daily_path, resolve_sql_template_string

"""Logic for SIS degree progress API import job."""


class ImportDegreeProgress(BackgroundJob):

    redshift_schema = app.config['REDSHIFT_SCHEMA_STUDENT']

    def run(self, csids=None):
        if not csids:
            all_sids = get_all_student_ids()
            if all_sids:
                csids = [row['sid'] for row in all_sids]
        app.logger.info(f'Starting SIS degree progress API import job for {len(csids)} students...')

        rows = []
        success_count = 0
        no_information_count = 0
        failure_count = 0
        index = 1

        # TODO The SIS degree progress API will return useful data only for students with a UGRD current registration.
        # We get that registration from the SIS student API, which is imported concurrently with this job. Is there an
        # alternative way to filter out non-UGRD students?
        for csid in csids:
            app.logger.info(f'Fetching degree progress API for SID {csid} ({index} of {len(csids)})')
            feed = sis_degree_progress_api.parsed_degree_progress(csid)
            if feed:
                success_count += 1
                rows.append(encoded_tsv_row([csid, json.dumps(feed)]))
            elif feed == {}:
                app.logger.info(f'No degree progress information found for SID {csid}.')
                no_information_count += 1
            else:
                failure_count += 1
                app.logger.error(f'SIS get_degree_progress failed for SID {csid}.')
            index += 1

        s3_key = f'{get_s3_sis_api_daily_path()}/degree_progress.tsv'
        app.logger.info(f'Will stash {success_count} feeds in S3: {s3_key}')
        if not s3.upload_tsv_rows(rows, s3_key):
            app.logger.error('Error on S3 upload: aborting job.')
            return False

        app.logger.info('Will copy S3 feeds into Redshift...')
        if not redshift.execute(f'TRUNCATE {self.redshift_schema}_staging.sis_api_degree_progress'):
            app.logger.error('Error truncating old staging rows: aborting job.')
            return False
        if not redshift.copy_tsv_from_s3(f'{self.redshift_schema}_staging.sis_api_degree_progress', s3_key):
            app.logger.error('Error on Redshift copy: aborting job.')
            return False
        staging_to_destination_query = resolve_sql_template_string(
            """
            DELETE FROM {redshift_schema_student}.sis_api_degree_progress
                WHERE sid IN (SELECT sid FROM {redshift_schema_student}_staging.sis_api_degree_progress);
            INSERT INTO {redshift_schema_student}.sis_api_degree_progress
                (SELECT * FROM {redshift_schema_student}_staging.sis_api_degree_progress);
            TRUNCATE {redshift_schema_student}_staging.sis_api_profiles;
            """,
        )
        if not redshift.execute(staging_to_destination_query):
            app.logger.error('Error on Redshift copy: aborting job.')
            return False

        return (
            f'SIS degree progress API import job completed: {success_count} succeeded, '
            f'{no_information_count} returned no information, {failure_count} failed.'
        )
