# Spring 2018 INF553 - HW3
This assignment aims to implement two algorithms with Spark. The first is Locality-Sensitive Hashing (LSH) to find the similarity of products in the datasets. Jaccard similarity and Cosine similarity are calculated respectively to select the candidate pairs whose similarity exceeds the threshold. The second one is Collaborative Filtering with model-based and user-based algorithms respectively. The program predicts the ratings of specific user and product, and calculate RMSE to compare the predictions with the testing data.
## 1. Environment Requirements
* Java: 1.8
* Scala: 2.11.12
* Spark: 2.2.1 
## 2. Datasets
* Total datasets: /video_small_num.csv
* Testing datasets: /video_small_testing_num.csv
* Ground-truth datasets of LSH:
  /video_small_ground_truth_cosine.csv
  
  /video_small_ground_truth_jaccard.csv
## 3. Implementation of algorithm


## 4. Usage Example
Open your terminal, using following command line in the top-level Spark directory to launch the applications. 
`$SPARK_HOME/bin/spark-submit --class <main-class> <application-jar> args(0) args(1) args(2)`

args(0): case number; args(1):input data; args(2):min support
For example,

`.bin/spark-submit --class JaccardLSH Tong_Lyu_hw3.jar /video_small_num.csv /Tong_Lyu_SimilarProducts_Jaccard.txt`

`.bin/spark-submit --class ModelBasedCF Tong_Lyu_hw3.jar /video_small_num.csv /video_small_testing_num.csv /Tong_Lyu_ModelBasedCF.txt` 

`.bin/spark-submit --class UserBasedCF Tong_Lyu_hw3.jar /video_small_num.csv /video_small_testing_num.csv /Tong_Lyu_UserBasedCF.txt`

Tips:
1)	The command line should be executed under the directory of `$SPARK_HOME/bin`

2)	The path of jar file and input data is relative, please make sure the jar file and input data are also under the current directory where you run the command line.

3)	The program will automatically generate an output file for each execution under the same current folder. 

## 5. Execution result
### 1. Jaccard LSH
* Precision: 0.9999782920158035
* Recall: 0.90129133242027
* Time: 45s
### 2. Cosine LSH
* Precision: 0.9407345864921938
* Recall: 0.7131235730968064
* Time: 572s
### 3. Model-based CF
|Range of errors|Count
| :------: | :------: |
|>= 0 and < 1|3957
|>= 1 and < 2|2927
|>= 2 and < 3|646
|>= 3 and < 4|167
|>= 4|3
|RMSE|1.2784529343163713
|Time|72s

### 4. User-based CF
|Range of errors|Count
| :------: | :------: |
|>= 0 and < 1|2238
|>= 1 and < 2|1318
|>= 2 and < 3|466
|>= 3 and < 4|175
|>= 4|20
|RMSE|1.4148421639785427
|Time|41s
