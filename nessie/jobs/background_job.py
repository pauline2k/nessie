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


"""Simple parent class for background jobs."""


from datetime import datetime
import hashlib
import os
from threading import Thread

from flask import current_app as app
from nessie.jobs.queue import get_job_queue
from nessie.lib.util import localize_datetime


def get_s3_canvas_daily_path():
    # TODO Temporarily share Data Loch's existing hash algorithm, even if it doesn't match PDG standard practice.
    # today = localize_datetime(datetime.now()).strftime('%Y-%m-%d')
    today = localize_datetime(datetime.now()).strftime('%m-%d-%Y')
    today_hash = hashlib.md5(today.encode('utf-8')).hexdigest()
    return app.config['LOCH_S3_CANVAS_DATA_PATH_DAILY'] + '/' + today_hash + '-' + today


def get_s3_sis_daily_path():
    today = localize_datetime(datetime.now()).strftime('%Y-%m-%d')
    today_hash = hashlib.md5(today.encode('utf-8')).hexdigest()
    return app.config['LOCH_S3_SIS_DATA_PATH'] + '/daily/' + today_hash + '-' + today


def resolve_sql_template(sql_filename):
    """Our DDL template files are simple enough to use standard Python string formatting."""
    s3_prefix = 's3://' + app.config['LOCH_S3_BUCKET'] + '/'
    template_data = {
        'redshift_schema_boac': app.config['REDSHIFT_SCHEMA_BOAC'],
        'redshift_schema_canvas': app.config['REDSHIFT_SCHEMA_CANVAS'],
        'redshift_schema_intermediate': app.config['REDSHIFT_SCHEMA_INTERMEDIATE'],
        'redshift_schema_metadata': app.config['REDSHIFT_SCHEMA_METADATA'],
        'redshift_schema_sis': app.config['REDSHIFT_SCHEMA_SIS'],
        'redshift_iam_role': app.config['REDSHIFT_IAM_ROLE'],
        'loch_s3_canvas_data_path_today': s3_prefix + get_s3_canvas_daily_path(),
        'loch_s3_canvas_data_path_historical': s3_prefix + app.config['LOCH_S3_CANVAS_DATA_PATH_HISTORICAL'],
        'loch_s3_canvas_data_path_current_term': s3_prefix + app.config['LOCH_S3_CANVAS_DATA_PATH_CURRENT_TERM'],
        'loch_s3_sis_data_path': s3_prefix + app.config['LOCH_S3_SIS_DATA_PATH'],
    }
    with open(app.config['BASE_DIR'] + f'/nessie/sql_templates/{sql_filename}') as file:
        template_string = file.read()
    return template_string.format(**template_data)


class BackgroundJob(object):

    def __init__(self, **kwargs):
        self.job_args = kwargs

    def run(self, **kwargs):
        pass

    def run_async(self):
        queue = get_job_queue()
        if queue:
            app.logger.info(f'Current queue size {queue.qsize()}; adding new job.')
            queue.put(self)
            return True
        # If no queue is enabled, start a new thread.
        else:
            app.logger.info('About to start background thread.')
            app_arg = app._get_current_object()
            thread = Thread(target=self.run_in_app_context, args=[app_arg], kwargs=self.job_args, daemon=True)

            if os.environ.get('NESSIE_ENV') in ['test', 'testext']:
                app.logger.info('Test run in progress; will not muddy the waters by actually kicking off a background thread.')
                return True

            thread.start()
            return True

    def run_in_app_context(self, app_arg, **kwargs):
        with app_arg.app_context():
            self.run(**kwargs)


class ChainedBackgroundJob(BackgroundJob):

    def run(self, steps):
        for step in steps:
            if not step.run():
                app.logger.error('Component job returned an error; aborting remainder of chain.')
                break
