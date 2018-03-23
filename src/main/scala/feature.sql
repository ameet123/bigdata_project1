-- noinspection SqlResolveForFile

CREATE TABLE subject_death_dayrange
  AS
    SELECT
      subject_id,
      death_day_range
    FROM (
           SELECT
             subject_id,
             CASE
             WHEN death_days <= 0.5
               THEN '0 - 1/2'
             WHEN death_days <= 1 AND death_days > 0.5
               THEN '0 - 1'
             WHEN death_days > 1 AND death_days <= 3
               THEN '1 - 3'
             ELSE 'over 3'
             END death_day_range
           FROM (
                  SELECT
                    subject_id,
                    extract(EPOCH FROM deathtime - admittime) / 86400 death_days
                  FROM admissions
                  WHERE hospital_expire_flag = 1
                ) AS death_t
         ) AS death_histogram;

-- topic death count
SELECT
  topic,
  count(*) dead_cnt
FROM (SELECT a.*
      FROM subject_topic a, admissions b
      WHERE a.subject_id = b.subject_id AND b.hospital_expire_flag = 1) AS a
GROUP BY topic;
-- topic and death range count
SELECT
  topic,
  death_day_range,
  count(*) topic_cnt
FROM (
       SELECT
         topic,
         death_day_range
       FROM subject_death_dayrange a, subject_topic b
       WHERE a.subject_id = b.subject_id) AS a
GROUP BY topic, death_day_range ORDER BY 1;
