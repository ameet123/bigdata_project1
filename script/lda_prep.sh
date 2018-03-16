#!/usr/bin/env bash

# This script describes the prep work required to run LDA

# Following is run in postgresql
psql -d mimic <<EOF
copy (select subject_id, string_agg (regexp_replace(text,E'[\\n\\r]+', ' ', 'g' ), ' ') as text
      from noteevents group by subject_id)
to  '/home/af55267/script/learning/w2/proj/data/consolidated_notes.csv' with csv delimiter ',' ;
EOF

# Transfer the file to HDFS
hdfs dfs -put consolidated_notes.csv project/notes/

