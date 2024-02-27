DROP TABLE IF EXISTS symmetric_narrow_evaluation;

CREATE TABLE symmetric_narrow_evaluation AS
SELECT
    admittime,
    anchor_year_group,
    subject_id,
    hadm_id,
    predicted_los + (actual_los - predicted_los) / 2 AS predicted_los,
    actual_los
FROM
    symmetric_evaluation;

COPY (
    SELECT
        error,
        count(error)
    FROM
        (
            SELECT
                round(actual_los - predicted_los) AS error
            FROM
                symmetric_narrow_evaluation
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_narrow_los_error_diff.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS symmetric_narrow_predicted_new;

CREATE TABLE symmetric_narrow_predicted_new AS
SELECT
    subject_id,
    hadm_id,
    admittime,
    predicted_dischtime,
    sequence :: date
FROM
    (
        SELECT
            subject_id,
            hadm_id,
            admittime :: date,
            (
                make_interval(days => round(predicted_los) :: int) + admittime
            ) :: date AS predicted_dischtime
        FROM
            symmetric_narrow_evaluation
    ) times,
    (
        SELECT
            generate_series(
                '1970-01-01' :: date,
                '1982-01-01' :: date,
                '1 day' :: interval
            ) AS sequence
    ) borders
WHERE
    times.admittime <= borders.sequence
    AND borders.sequence <= times.predicted_dischtime;

DROP TABLE IF EXISTS symmetric_narrow_actual_new;

CREATE TABLE symmetric_narrow_actual_new AS
SELECT
    subject_id,
    hadm_id,
    admittime,
    actual_dischtime,
    sequence :: date
FROM
    (
        SELECT
            subject_id,
            hadm_id,
            admittime :: date,
            (
                make_interval(days => round(actual_los) :: int) + admittime
            ) :: date AS actual_dischtime
        FROM
            symmetric_narrow_evaluation
    ) times,
    (
        SELECT
            generate_series(
                '1970-01-01' :: date,
                '1982-01-01' :: date,
                '1 day' :: interval
            ) AS sequence
    ) borders
WHERE
    times.admittime <= borders.sequence
    AND borders.sequence <= times.actual_dischtime;

DROP TABLE IF EXISTS symmetric_narrow_occupied_by_count_new;

CREATE TABLE symmetric_narrow_occupied_by_count_new AS
SELECT
    timeline,
    count(predicted_hadm_id) AS predicted_count,
    count(actual_hadm_id) AS actual_count
FROM
    (
        SELECT
            DISTINCT coalesce(predicted.sequence, actual.sequence) AS timeline,
            predicted.hadm_id AS predicted_hadm_id,
            actual.hadm_id AS actual_hadm_id
        FROM
            (
                SELECT
                    sequence,
                    hadm_id
                FROM
                    symmetric_narrow_predicted_new
            ) predicted FULL
            OUTER JOIN (
                SELECT
                    sequence,
                    hadm_id
                FROM
                    symmetric_narrow_actual_new
            ) actual ON (
                predicted.sequence = actual.sequence
                AND predicted.hadm_id = actual.hadm_id
            )
    )
GROUP BY
    timeline;

COPY (
    SELECT
        error,
        count(error)
    FROM
        (
            SELECT
                actual_count - predicted_count AS error
            FROM
                symmetric_narrow_occupied_by_count_new
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_narrow_occupied_error_diff_new.csv' DELIMITER ',' CSV;

COPY (
    SELECT
        row_number() OVER (
            ORDER BY
                timeline
        ),
        timeline,
        actual_count - predicted_count AS error
    FROM
        symmetric_narrow_occupied_by_count_new
    WHERE
        extract(
            year
            FROM
                timeline
        ) = 1972
    ORDER BY
        timeline
) TO '/tmp/symmetric_narrow_occupied_error_within_year_new.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS symmetric_narrow_predicted_old;

CREATE TABLE symmetric_narrow_predicted_old AS
SELECT
    subject_id,
    hadm_id,
    admittime,
    predicted_dischtime,
    sequence :: date
FROM
    (
        SELECT
            e.subject_id,
            e.hadm_id,
            admi.admittime :: date,
            (
                make_interval(days => round(e.predicted_los) :: int) + admi.admittime
            ) :: date AS predicted_dischtime
        FROM
            symmetric_narrow_evaluation e,
            admissions admi
        WHERE
            e.subject_id = admi.subject_id
            AND e.hadm_id = admi.hadm_id
    ) times,
    (
        SELECT
            generate_series(
                '2110-01-11' :: date,
                '2212-04-10' :: date,
                '1 day' :: interval
            ) AS sequence
    ) borders
WHERE
    times.admittime <= borders.sequence
    AND borders.sequence <= times.predicted_dischtime;

DROP TABLE IF EXISTS symmetric_narrow_actual_old;

CREATE TABLE symmetric_narrow_actual_old AS
SELECT
    subject_id,
    hadm_id,
    admittime,
    actual_dischtime,
    sequence :: date
FROM
    (
        SELECT
            e.subject_id,
            e.hadm_id,
            admi.admittime :: date,
            (
                make_interval(days => round(e.actual_los) :: int) + admi.admittime
            ) :: date AS actual_dischtime
        FROM
            symmetric_narrow_evaluation e,
            admissions admi
        WHERE
            e.subject_id = admi.subject_id
            AND e.hadm_id = admi.hadm_id
    ) times,
    (
        SELECT
            generate_series(
                '2110-01-11' :: date,
                '2212-04-12' :: date,
                '1 day' :: interval
            ) AS sequence
    ) borders
WHERE
    times.admittime <= borders.sequence
    AND borders.sequence <= times.actual_dischtime;

DROP TABLE IF EXISTS symmetric_narrow_occupied_by_count_old;

CREATE TABLE symmetric_narrow_occupied_by_count_old AS
SELECT
    timeline,
    count(predicted_hadm_id) AS predicted_count,
    count(actual_hadm_id) AS actual_count
FROM
    (
        SELECT
            DISTINCT coalesce(predicted.sequence, actual.sequence) AS timeline,
            predicted.hadm_id AS predicted_hadm_id,
            actual.hadm_id AS actual_hadm_id
        FROM
            (
                SELECT
                    sequence,
                    hadm_id
                FROM
                    symmetric_narrow_predicted_old
            ) predicted FULL
            OUTER JOIN (
                SELECT
                    sequence,
                    hadm_id
                FROM
                    symmetric_narrow_actual_old
            ) actual ON (
                predicted.sequence = actual.sequence
                AND predicted.hadm_id = actual.hadm_id
            )
    )
GROUP BY
    timeline;

COPY (
    SELECT
        error,
        count(error)
    FROM
        (
            SELECT
                actual_count - predicted_count AS error
            FROM
                symmetric_narrow_occupied_by_count_old
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_narrow_occupied_error_diff_old.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS symmetric_narrow_timeline_new;

CREATE TABLE symmetric_narrow_timeline_new AS
SELECT
    DISTINCT coalesce(predicted.sequence, actual.sequence) AS timeline,
    predicted.admittime AS predicted_admittime,
    predicted.hadm_id AS predicted_hadm_id,
    actual.admittime AS actual_admittime,
    actual.hadm_id AS actual_hadm_id
FROM
    (
        SELECT
            sequence,
            admittime,
            hadm_id
        FROM
            symmetric_narrow_predicted_new
    ) predicted FULL
    OUTER JOIN (
        SELECT
            sequence,
            admittime,
            hadm_id
        FROM
            symmetric_narrow_actual_new
    ) actual ON (
        predicted.sequence = actual.sequence
        AND predicted.hadm_id = actual.hadm_id
    )
ORDER BY
    timeline;

DROP TABLE IF EXISTS symmetric_narrow_timeline_filtered;

CREATE TABLE symmetric_narrow_timeline_filtered AS
SELECT
    timeline,
    coalesce(predicted_admittime, actual_admittime) AS admittime,
    coalesce(predicted_hadm_id, actual_hadm_id) AS hadm_id,
    predicted_hadm_id IS NOT NULL AS predicted,
    actual_hadm_id IS NOT NULL AS actual
FROM
    symmetric_narrow_timeline_new;

DROP TABLE IF EXISTS symmetric_narrow_timeline_prediction;

CREATE TABLE symmetric_narrow_timeline_prediction AS
SELECT
    now.timeline AS time,
    future.predicted
FROM
    symmetric_narrow_timeline_filtered now
    LEFT JOIN symmetric_narrow_timeline_filtered future ON (
        now.hadm_id = future.hadm_id
        AND now.timeline = future.timeline - interval '3 days'
    )
WHERE
    now.actual
ORDER BY
    now.timeline;

DROP TABLE IF EXISTS symmetric_narrow_timeline_actual;

CREATE TABLE symmetric_narrow_timeline_actual AS
SELECT
    timeline,
    count(DISTINCT actual_hadm_id)
FROM
    symmetric_narrow_timeline_new
GROUP BY
    timeline;

COPY (
    SELECT
        error,
        count(error)
    FROM
        (
            SELECT
                (a.count - (p.count + 92)) AS error
            FROM
                (
                    SELECT
                        time,
                        count(predicted :: int)
                    FROM
                        symmetric_narrow_timeline_prediction
                    GROUP BY
                        time
                ) p,
                symmetric_narrow_timeline_actual a
            WHERE
                p.time = a.timeline
        ) sub
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_narrow_occ.csv' DELIMITER ',' CSV;