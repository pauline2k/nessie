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


"""Background job scheduling."""


from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
from apscheduler.schedulers.background import BackgroundScheduler
from nessie.models.util import try_advisory_lock


app = None
sched = None


# Postgres advisory locks require numeric ids.
PG_ADVISORY_LOCK_IDS = {
    'JOB_SYNC_CANVAS_SNAPSHOTS': 1000,
    'JOB_RESYNC_CANVAS_SNAPSHOTS': 2000,
    'JOB_GENERATE_ALL_TABLES': 3000,
}


def get_scheduler():
    return sched


def initialize_job_schedules(_app, force=False):
    from nessie.jobs.create_canvas_schema import CreateCanvasSchema
    from nessie.jobs.create_sis_schema import CreateSisSchema
    from nessie.jobs.generate_boac_analytics import GenerateBoacAnalytics
    from nessie.jobs.generate_intermediate_tables import GenerateIntermediateTables
    from nessie.jobs.resync_canvas_snapshots import ResyncCanvasSnapshots
    from nessie.jobs.sync_canvas_snapshots import SyncCanvasSnapshots

    global app
    app = _app

    global sched
    if app.config['JOB_SCHEDULING_ENABLED']:

        if try_advisory_lock(5000):
            app.logger.info(f'Granted advisory lock 5000.')
        else:
            app.logger.warn(f'Was not granted advisory lock 5000.')

        db_jobstore = SQLAlchemyJobStore(url=app.config['SQLALCHEMY_DATABASE_URI'], tablename='apscheduler_jobs')
        sched = BackgroundScheduler(jobstores={'default': db_jobstore})
        sched.start()
        schedule_job(sched, 'JOB_SYNC_CANVAS_SNAPSHOTS', SyncCanvasSnapshots, force)
        schedule_job(sched, 'JOB_RESYNC_CANVAS_SNAPSHOTS', ResyncCanvasSnapshots, force)
        schedule_chained_job(
            sched,
            'JOB_GENERATE_ALL_TABLES',
            [
                CreateCanvasSchema,
                CreateSisSchema,
                GenerateIntermediateTables,
                GenerateBoacAnalytics,
            ],
            force,
        )


def add_job(sched, job_func, job_arg, job_id, force=False):
    job_schedule = app.config.get(job_id)
    if job_schedule:
        existing_job = sched.get_job(job_id)
        if existing_job and (force is False):
            app.logger.info(f'Found existing cron trigger for job {job_id}, will not reschedule: {existing_job.next_run_time}')
            return False
        else:
            sched.add_job(job_func, 'cron', args=(job_arg, job_id), id=job_id, replace_existing=True, **job_schedule)
            return job_schedule


def schedule_job(sched, job_id, job_class, force=False):
    job_schedule = add_job(sched, start_background_job, job_class, job_id, force)
    if job_schedule:
        app.logger.info(f'Scheduled {job_class.__name__} job: {job_schedule}')


def schedule_chained_job(sched, job_id, job_components, force=False):
    job_schedule = add_job(sched, start_chained_job, job_components, job_id, force)
    if job_schedule:
        app.logger.info(f'Scheduled chained background job: {job_schedule}, ' + ', '.join([c.__name__ for c in job_components]))


def start_background_job(job_class, job_id):
    lock_id = PG_ADVISORY_LOCK_IDS[job_id]
    app.logger.info(f'Starting scheduled {job_class.__name__} job')
    with app.app_context():
        job_class().run_async(lock_id=lock_id)


def start_chained_job(job_components, job_id):
    from nessie.jobs.background_job import ChainedBackgroundJob
    lock_id = PG_ADVISORY_LOCK_IDS[job_id]
    app.logger.info(f'Starting chained background job: ' + ', '.join([c.__name__ for c in job_components]))
    with app.app_context():
        initialized_components = [c() for c in job_components]
        ChainedBackgroundJob(steps=initialized_components).run_async(lock_id=lock_id)
