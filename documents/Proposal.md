### Sepsis Prediction

#### Proposal

- *Motivation*: Why is this problem important? Why do we care of this problem?

  Sepsis is important on two main fronts - human life and financial cost

  *Human Life*:

  ![leading_Cause](image/leading_Cause.jpg)

  ​*Financial Cost*:

![cost](image/cost.jpg)

​	Sepsis is the most expensive condition to treat in the US hospitals, with an expense of $23.7 billion in the year 2013.

​	*Readmission Rate*

​	Sepsis has a very high re-admission rate after the initial diagnosis of 62%.

- Literature Survey: What kind of approaches have been used? What are the related works?
  + `Ghassemi et al`:
    + leveraged unstructured notes in the ER to predict Sepsis. 
    + Additionally, they weighted the early symptoms more in comparison to the latter ones.
  + `Desautels et al`:
    + Focused on data elements readily available at bedside rather than laboratory tests.
    + Considered a few vital readings rather than a cornucopia of information points.

  ​

- Approach: How will you solve the problem?

  We define the problem as follows,

  `Problem Definition`:

  1. Identification of Sepsis based on early symptoms in the first phase of ICU admission within a few hours.
  2. Detection of readmission probability based on current symptoms.

  `Solution Approaches`:

  1. **Categorize** the dataset for **early symptoms** and **comprehensive symptoms** for all available hours in the ICU
  2. Run **multiple algorithms** for similarity analysis,
     + ***K-means*** analysis for smaller number of vital features
     + ***Random Walk*** analysis with comprehensive set of features.
  3. **Intersection** of multiple prediction lists: Take common patients from the above analyses
  4. Predict he **probability of re-admission**: Use `linear regression` to predict Sepsis related readmission for patients classified as Sepsis affected.

- Data: Describe the dataset you use. e.g. descriptive stats, preliminary results, etc.

  <TBD>

- Experimental Setup: Describe your project environment. e.g. software stack you use, on which hardware spec, etc.

   We plan to use `Spark` on `yarn/Hadoop` for data processing, feature construction as well as some analysis.

  Specifically, analyses such as `Random Walk` shall be done using `GraphX` on Spark. *K-means* and *linear-regression* shall be done using python on the dataset prepared using Spark.

  Additional similarity analysis shall be done using Neural Network via `PyTorch`



#### Sources

+ *Intermedix*: 