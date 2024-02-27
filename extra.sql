COPY (
    SELECT
        row_number() OVER (
            ORDER BY
                sequence
        ),
        sequence,
        count(DISTINCT hadm_id)
    FROM
        actual_new
    WHERE
        extract(
            year
            FROM
                sequence
        ) = 1972
    GROUP BY
        sequence
    ORDER BY
        sequence
) TO '/tmp/bed_occupancy.csv' DELIMITER ',' CSV;

COPY (
    SELECT
        row_number() OVER (
            ORDER BY
                actual_count
        ),
        actual_count,
        count(actual_count)
    FROM
        occupied_by_count_new
    GROUP BY
        actual_count
) TO '/tmp/occupancy.csv' DELIMITER ',' CSV;