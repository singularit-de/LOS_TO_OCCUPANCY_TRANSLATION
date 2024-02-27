DROP VIEW IF EXISTS error CASCADE;

CREATE VIEW error AS
SELECT
    error,
    count(error) AS count
FROM
    (
        SELECT
            round(evaluation.actual_los - evaluation.predicted_los) AS error
        FROM
            evaluation
    ) sub
WHERE
    error < '-2' :: integer
    OR error > 2
GROUP BY
    error;

DROP VIEW IF EXISTS verschieben CASCADE;

CREATE VIEW verschieben AS
SELECT
    error,
    verschieben
FROM
    (
        SELECT
            COALESCE(b.error, - a.error) AS "coalesce",
            COALESCE(b.count, 0 :: bigint) AS "coalesce",
            a.error,
            a.count,
            ((a.count - COALESCE(b.count, 0 :: bigint)) / 2) :: numeric / a.count :: numeric AS verschieben
        FROM
            error a
            LEFT JOIN error b ON (- a.error) = b.error
        WHERE
            a.error > 2
        ORDER BY
            a.error
    ) unnamed_subquery(
        "coalesce",
        coalesce_1,
        error,
        count,
        verschieben
    );

DROP TABLE IF EXISTS symmetric_evaluation;

CREATE TABLE symmetric_evaluation AS
SELECT
    admittime,
    anchor_year_group,
    subject_id,
    hadm_id,
    CASE
        WHEN random() < verschieben THEN predicted_los + 2 * error
        ELSE predicted_los
    END AS predicted_los,
    actual_los
FROM
    evaluation
    LEFT JOIN verschieben ON (
        round(evaluation.actual_los - evaluation.predicted_los) = verschieben.error
    );

COPY (
    SELECT
        error,
        count(error)
    FROM
        (
            SELECT
                round(actual_los - predicted_los) AS error
            FROM
                symmetric_evaluation
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_los_error_diff.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS symmetric_predicted_new;

CREATE TABLE symmetric_predicted_new AS
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
            symmetric_evaluation
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

DROP TABLE IF EXISTS symmetric_actual_new;

CREATE TABLE symmetric_actual_new AS
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
            symmetric_evaluation e
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

DROP TABLE IF EXISTS symmetric_occupied_by_count_new;

CREATE TABLE symmetric_occupied_by_count_new AS
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
                    symmetric_predicted_new
            ) predicted FULL
            OUTER JOIN (
                SELECT
                    sequence,
                    hadm_id
                FROM
                    symmetric_actual_new
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
                symmetric_occupied_by_count_new
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_occupied_error_diff_new.csv' DELIMITER ',' CSV;

COPY (
    SELECT
        row_number() OVER (
            ORDER BY
                timeline
        ),
        timeline,
        actual_count - predicted_count AS error
    FROM
        symmetric_occupied_by_count_new
    WHERE
        extract(
            year
            FROM
                timeline
        ) = 1972
    ORDER BY
        timeline
) TO '/tmp/symmetric_occupied_error_within_year_new.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS symmetric_predicted_old;

CREATE TABLE symmetric_predicted_old AS
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
            symmetric_evaluation e,
            admissions admi
        WHERE
            e.subject_id = admi.subject_id
            AND e.hadm_id = admi.hadm_id
    ) times,
    (
        SELECT
            generate_series(
                '2110-01-11' :: date,
                '2212-04-08' :: date,
                '1 day' :: interval
            ) AS sequence
    ) borders
WHERE
    times.admittime <= borders.sequence
    AND borders.sequence <= times.predicted_dischtime;

CREATE TABLE symmetric_actual_old AS
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
            symmetric_evaluation e,
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

CREATE TABLE symmetric_occupied_by_count_old AS
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
                    symmetric_predicted_old
            ) predicted FULL
            OUTER JOIN (
                SELECT
                    sequence,
                    hadm_id
                FROM
                    symmetric_actual_old
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
                symmetric_occupied_by_count_old
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_occupied_error_diff_old.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS symmetric_timeline_new;

CREATE TABLE symmetric_timeline_new AS
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
            symmetric_predicted_new
    ) predicted FULL
    OUTER JOIN (
        SELECT
            sequence,
            admittime,
            hadm_id
        FROM
            symmetric_actual_new
    ) actual ON (
        predicted.sequence = actual.sequence
        AND predicted.hadm_id = actual.hadm_id
    )
ORDER BY
    timeline;

DROP TABLE IF EXISTS symmetric_timeline_filtered;

CREATE TABLE symmetric_timeline_filtered AS
SELECT
    timeline,
    coalesce(predicted_admittime, actual_admittime) AS admittime,
    coalesce(predicted_hadm_id, actual_hadm_id) AS hadm_id,
    predicted_hadm_id IS NOT NULL AS predicted,
    actual_hadm_id IS NOT NULL AS actual
FROM
    symmetric_timeline_new;

DROP TABLE IF EXISTS symmetric_timeline_prediction;

CREATE TABLE symmetric_timeline_prediction AS
SELECT
    now.timeline AS time,
    future.predicted
FROM
    symmetric_timeline_filtered now
    LEFT JOIN symmetric_timeline_filtered future ON (
        now.hadm_id = future.hadm_id
        AND now.timeline = future.timeline - interval '3 days'
    )
WHERE
    now.actual
ORDER BY
    now.timeline;

DROP TABLE IF EXISTS symmetric_timeline_actual;

CREATE TABLE symmetric_timeline_actual AS
SELECT
    timeline,
    count(DISTINCT actual_hadm_id)
FROM
    symmetric_timeline_new
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
                        symmetric_timeline_prediction
                    GROUP BY
                        time
                ) p,
                symmetric_timeline_actual a
            WHERE
                p.time = a.timeline
        ) sub
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/symmetric_occ.csv' DELIMITER ',' CSV;