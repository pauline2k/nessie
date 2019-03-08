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

import math

from flask import current_app as app
from numpy import nan
import pandas
from scipy.stats import percentileofscore


def merge_analytics_for_course(canvas_site_element):
    enrollments = canvas_site_element.get('enrollments')
    if enrollments is None:
        _error = {'error': 'Redshift query returned no results'}
        return {'currentScore': _error, 'lastActivity': _error}
    advisee_enrollments = canvas_site_element.get('adviseeEnrollments')
    if not advisee_enrollments:
        return None

    canvas_course_id = canvas_site_element.get('canvasCourseId')
    df = pandas.DataFrame(enrollments, columns=['canvas_user_id', 'current_score', 'last_activity_at'])
    student_rows = {}

    for (advisee_user_id, advisee_enrollment) in advisee_enrollments.items():
        student_row = df.loc[df['canvas_user_id'].values == int(advisee_user_id)]
        if enrollments and student_row.empty:
            app.logger.warning(f'Canvas user {advisee_user_id} not found in Data Loch for course site {canvas_course_id}')
            student_row = pandas.DataFrame({
                'canvas_user_id': [int(advisee_user_id)],
                'current_score': [None],
                'last_activity_at': [None],
            })
            df = df.append(student_row, ignore_index=True)
            # Fetch newly appended row, mostly for the sake of its properly set-up index.
            student_row = df.loc[df['canvas_user_id'].values == int(advisee_user_id)]
        student_rows[advisee_user_id] = student_row

    course_data_slices = fillet_course_analytics(df, ['current_score', 'last_activity_at'])
    course_current_score_analytics = analytics_for_course(course_data_slices['current_score'])
    course_last_activity_analytics = analytics_for_course(course_data_slices['last_activity_at'])

    for (advisee_user_id, student_row) in student_rows.items():
        if 'student' not in course_current_score_analytics:
            student_current_score_analytics = analytics_for_student(course_data_slices['current_score'], student_row, 'current_score')
            student_current_score_analytics.update(course_current_score_analytics)
        else:
            student_current_score_analytics = course_current_score_analytics

        if 'student' not in course_last_activity_analytics:
            student_last_activity_analytics = analytics_for_student(course_data_slices['last_activity_at'], student_row, 'last_activity_at')
            student_last_activity_analytics.update(course_last_activity_analytics)
        else:
            student_last_activity_analytics = course_last_activity_analytics

        advisee_enrollments[advisee_user_id]['analytics'] = advisee_enrollments[advisee_user_id].get('analytics') or {}
        advisee_enrollments[advisee_user_id]['analytics'].update({
            'currentScore': student_current_score_analytics,
            'lastActivity': student_last_activity_analytics,
            'courseEnrollmentCount': len(enrollments),
        })


def merge_assignment_submissions_for_user(user_courses, canvas_user_id, relative_submission_counts):
    if not user_courses:
        return
    for course in user_courses:
        course['analytics'] = course.get('analytics') or {}
        canvas_course_id = course['canvasCourseId']
        course_rows = relative_submission_counts.get(canvas_course_id, [])
        df = pandas.DataFrame(course_rows, columns=['canvas_user_id', 'submissions_turned_in'])
        student_row = df.loc[df['canvas_user_id'].values == int(canvas_user_id)]

        if course_rows and student_row.empty:
            app.logger.warn(f'Canvas user id {canvas_user_id}, course id {canvas_course_id} not found in Data Loch assignments; will assume 0 score')
            student_row = pandas.DataFrame({'canvas_user_id': [int(canvas_user_id)], 'submissions_turned_in': [0]})
            df = df.append(student_row, ignore_index=True)
            # Fetch newly appended row, mostly for the sake of its properly set-up index.
            student_row = df.loc[df['canvas_user_id'].values == int(canvas_user_id)]

    course_data_slices = fillet_course_analytics(df, ['submissions_turned_in'])
    course['analytics']['assignmentsSubmitted'] = analytics_for_course(course_data_slices['submissions_turned_in'])
    if 'student' not in course['analytics']['assignmentsSubmitted']:
        course['analytics']['assignmentsSubmitted'].update(analytics_for_student(course_data_slices['submissions_turned_in'], student_row, 'submissions_turned_in'))


def fillet_course_analytics(df, metrics):
    course_data_slices = {}
    for metric in metrics:
        course_data_slices[metric] = {}
        course_data_slices[metric]['dfcol'] = df[metric]
        # If no data exists for a column, the Pandas 'nunique' function reports zero unique values.
        # However, some feeds (such as Canvas student summaries) return (mostly) zero values rather than empty lists,
        # and we've also seen some Canvas feeds which mix nulls and zeroes.
        # Setting non-numbers to zero works acceptably for most current analyzed feeds, apart from lastActivity (see below).
        course_data_slices[metric]['dfcol'].fillna(0, inplace=True)
        course_data_slices[metric]['unique_scores'] = course_data_slices[metric]['dfcol'].unique().tolist()
        course_data_slices[metric]['original_values'] = course_data_slices[metric]['dfcol'].tolist()

        # When calculating z-scores and means for lastActivity, zeroed-out "no activity" values must be dropped, since zeros
        # and Unix timestamps don't play well in the same distribution. However, the mean's intuitive-percentile
        # must match that of any real student who happens to have the same raw value as the mean.
        if metric == 'last_activity_at':
            course_data_slices[metric]['dfcol_normalized'] = course_data_slices[metric]['dfcol'].replace(0, nan).dropna()
        else:
            course_data_slices[metric]['dfcol_normalized'] = course_data_slices[metric]['dfcol']
    return course_data_slices


def analytics_for_course(sliver):
    nunique = sliver['dfcol'].nunique()
    if nunique == 0 or (nunique == 1 and sliver['dfcol'].max() == 0.0):
        return {
            'boxPlottable': False,
            'student': {
                'percentile': None,
                'raw': None,
                'roundedUpPercentile': None,
            },
            'courseDeciles': None,
            'courseMean': None,
            'displayPercentile': None,
        }

    # If only ten or fewer values are shared across the student population, the 'universal' percentile figure and the
    # box-and-whisker graph will usually look odd. With such sparse data sets, a text summary and an (optional)
    # histogram are more readable.
    box_plottable = (nunique > 10)

    column_quantiles = quantiles(sliver['dfcol'], 10)

    course_mean = sliver['dfcol_normalized'].mean()
    if course_mean and not math.isnan(course_mean):
        # Spoiler: this will be '50.0'.
        comparative_percentile_of_mean = zptile(zscore(sliver['dfcol_normalized'], course_mean))
        intuitive_percentile_of_mean = int(percentileofscore(sliver['original_values'], course_mean, kind='weak'))
        matrixy_comparative_percentile_of_mean = percentileofscore(sliver['unique_scores'], course_mean, kind='strict')
    else:
        comparative_percentile_of_mean = None
        matrixy_comparative_percentile_of_mean = None
        intuitive_percentile_of_mean = None

    return {
        'boxPlottable': box_plottable,
        'courseDeciles': column_quantiles,
        'courseMean': {
            'matrixyPercentile': matrixy_comparative_percentile_of_mean,
            'percentile': comparative_percentile_of_mean,
            'raw': course_mean,
            'roundedUpPercentile': intuitive_percentile_of_mean,
        },
    }


def analytics_for_student(sliver, student_row, column_name):
    student_row = student_row.fillna(0)
    intuitive_percentile = rounded_up_percentile(sliver['dfcol'], student_row)
    # The intuitive percentile is our best option for display, whether or not the distribution is boxplottable.
    # Note, however, that if all students have the same score, then all students are in the "100th percentile."
    display_percentile = ordinal(intuitive_percentile)

    column_value = student_row[column_name].values[0]
    raw_value = round(column_value.item())

    column_zscore = zscore(sliver['dfcol_normalized'], column_value)
    comparative_percentile = zptile(column_zscore)
    # For purposes of matrix plotting, improve visual spread by calculating percentile against a range of unique scores.
    matrixy_comparative_percentile = percentileofscore(sliver['unique_scores'], column_value, kind='strict')

    return {
        'student': {
            'matrixyPercentile': matrixy_comparative_percentile,
            'percentile': comparative_percentile,
            'raw': raw_value,
            'roundedUpPercentile': intuitive_percentile,
        },
        'displayPercentile': display_percentile,
    }


def ordinal(nbr):
    rounded = round(nbr)
    mod_ten = rounded % 10
    if (mod_ten == 1) and (rounded != 11):
        suffix = 'st'
    elif (mod_ten == 2) and (rounded != 12):
        suffix = 'nd'
    elif (mod_ten == 3) and (rounded != 13):
        suffix = 'rd'
    else:
        suffix = 'th'
    return f'{rounded}{suffix}'


def quantiles(series, count):
    """Return a given number of evenly spaced quantiles for a given series."""
    return [round(series.quantile(n / count)) for n in range(0, count + 1)]


def rounded_up_percentile(dataframe, student_row):
    """Given a dataframe and an individual student row, return a more easily understood meaning of percentile.

    Z-score percentile is useful in a scatterplot to spot outliers in the overall population across contexts.
    (If 90% of the course's students received a score of '5', then one student with a '5' is not called out.)
    Rounded-up matches what non-statisticians would expect when viewing one particular student in one
    particular course context. (If only 10% of the course's students did better than '5', then this student
    with a '5' is in the 90th percentile.)
    """
    percentile = dataframe.rank(pct=True, method='max')[student_row.index].values[0]
    percentile = int(percentile * 100)
    return percentile


def zptile(z_score):
    """Derive percentile from zscore."""
    if z_score is None:
        return None
    else:
        return round(50 * (math.erf(z_score / 2 ** .5) + 1))


def zscore(dataframe, value):
    """Given a dataframe and an individual value, return a zscore."""
    if dataframe.std(ddof=0) == 0:
        return None
    else:
        return (value - dataframe.mean()) / dataframe.std(ddof=0)
