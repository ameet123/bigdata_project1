### Sepsis Prediction

#### Identification
Sepsis is designated by 3 codes - `sepsis`, `severe sepsis`, `septic shock`
We can tie these codes to the patients to determine affliction of sepsis.
These codes are represented by the `icd9` codes - *99591,99592,78552*

#### Preparation
Before we can run k-means analysis, we need to generate a file of patient-labels. 
This file contains a mapping of each patient to a sepsis diagnosis. Based on the data loaded
in `Postgres`, we can generate this file as follows,
```postgresql
COPY (
SELECT a.subject_id,
          CASE '{99591,99592,78552}':: character varying[] && codes
              WHEN 't' THEN 1
              ELSE 0
          END AS sepsis_yn
   FROM   subject_topic a
   LEFT   JOIN
    	  (SELECT subject_id, array_Agg(icd9_code) codes
      	    FROM diagnoses_icd
      	   GROUP BY subject_id) b 
     ON  a.subject_id=b.subject_id
) to '/home/af55267/script/learning/w2/proj/data/subjectSepsis.csv'
With CSV DELIMITER ',';
```

#### Run K-means analysis
Assuming we have the subject-topic and subject-sepsis files ready, the clustering algorithm can be
run as follows,
1. copy both files to HDFS
2. run `kmeans` as follows,
```bash
bash kmeans.sh \
-s yarn -x \
-t project/kmeans/subjectMultiTopic.csv \
-m project/kmeans/subjectSepsis.csv \
-k ameet.keytab \
-p AF55267@DEVAD.WELLPOINT.COM
```

#### Results

For 20 topic output, the purity results are as follows,

K-means purity = **0.8996857731065121**