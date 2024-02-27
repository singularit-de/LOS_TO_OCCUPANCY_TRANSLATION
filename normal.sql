COPY (
    SELECT
        error,
        count(error)
    FROM
        (
            SELECT
                round(actual_los - predicted_los) AS error
            FROM
                evaluation
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/los_error_diff.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS predicted_new;

CREATE TABLE predicted_new AS
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
                make_interval(days => predicted_los :: int) + admittime
            ) :: date AS predicted_dischtime
        FROM
            evaluation
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

DROP TABLE IF EXISTS actual_new;

CREATE TABLE actual_new AS
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
            evaluation
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

DROP TABLE IF EXISTS occupied_by_count_new;

CREATE TABLE occupied_by_count_new AS
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
                    predicted_new
            ) predicted FULL
            OUTER JOIN (
                SELECT
                    sequence,
                    hadm_id
                FROM
                    actual_new
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
                occupied_by_count_new
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/occupied_error_diff_new.csv' DELIMITER ',' CSV;

COPY (
    SELECT
        row_number() OVER (
            ORDER BY
                timeline
        ),
        timeline,
        actual_count - predicted_count AS error
    FROM
        occupied_by_count_new
    WHERE
        extract(
            year
            FROM
                timeline
        ) = 1972
    ORDER BY
        timeline
) TO '/tmp/occupied_error_within_year_new.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS predicted_old;

CREATE TABLE predicted_old AS
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
            evaluation e,
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
    AND borders.sequence <= times.predicted_dischtime;

DROP TABLE IF EXISTS actual_old;

CREATE TABLE actual_old AS
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
            evaluation e,
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

DROP TABLE IF EXISTS occupied_by_count;

CREATE TABLE occupied_by_count_old AS
SELECT
    DISTINCT coalesce(predicted.sequence, actual.sequence) AS timeline,
    predicted.sequence AS predicted_day,
    predicted.count AS predicted_count,
    actual.sequence AS actual_day,
    actual.count AS actual_count
FROM
    (
        SELECT
            sequence,
            count(hadm_id)
        FROM
            predicted_old
        GROUP BY
            sequence
    ) predicted FULL
    OUTER JOIN (
        SELECT
            sequence,
            count(hadm_id)
        FROM
            actual_old
        GROUP BY
            sequence
    ) actual ON (predicted.sequence = actual.sequence);

COPY (
    SELECT
        error,
        count(error)
    FROM
        (
            SELECT
                actual_count - predicted_count AS error
            FROM
                occupied_by_count_old
        )
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/occupied_error_diff_old.csv' DELIMITER ',' CSV;

DROP TABLE IF EXISTS timeline_new;

CREATE TABLE timeline_new AS
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
            predicted_new
    ) predicted FULL
    OUTER JOIN (
        SELECT
            sequence,
            admittime,
            hadm_id
        FROM
            actual_new
    ) actual ON (
        predicted.sequence = actual.sequence
        AND predicted.hadm_id = actual.hadm_id
    )
ORDER BY
    timeline;

DROP TABLE IF EXISTS timeline_filtered;

CREATE TABLE timeline_filtered AS
SELECT
    timeline,
    coalesce(predicted_admittime, actual_admittime) AS admittime,
    coalesce(predicted_hadm_id, actual_hadm_id) AS hadm_id,
    predicted_hadm_id IS NOT NULL AS predicted,
    actual_hadm_id IS NOT NULL AS actual
FROM
    timeline_new;

DROP TABLE IF EXISTS timeline_prediction;

CREATE TABLE timeline_prediction AS
SELECT
    now.timeline AS time,
    future.predicted
FROM
    timeline_filtered now
    LEFT JOIN timeline_filtered future ON (
        now.hadm_id = future.hadm_id
        AND now.timeline = future.timeline - interval '3 days'
    )
WHERE
    now.actual
ORDER BY
    now.timeline;

DROP TABLE IF EXISTS timeline_actual;

CREATE TABLE timeline_actual AS
SELECT
    timeline,
    count(DISTINCT actual_hadm_id)
FROM
    timeline_new
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
                        timeline_prediction
                    GROUP BY
                        time
                ) p,
                timeline_actual a
            WHERE
                p.time = a.timeline
        ) sub
    GROUP BY
        error
    ORDER BY
        error
) TO '/tmp/occ.csv' DELIMITER ',' CSV;