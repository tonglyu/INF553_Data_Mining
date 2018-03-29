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
### Locality-Sensitive Hashing (LSH)
#### 1) Minhash signatures
The aim of minhashing is to reduce the large sets of features (e.g. user sets of each products) into small signatures.
The original characteristic matrix (S) of sets is as follows:
|Element|product1|product2|product3|product4
| :------: | :------: | :------: | :------: | :------: |
|user1|1|0|0|1
|user2|0|0|1|0
|user3|1|0|1|1
|user4|0|0|1|0
Pick a permutation of rows, (i.e. f(userId) = (a * userId + b) % m, m is the number of users), Minhash(Sj) = no. of 1st row i where S[i, j] = 1, then we can get the signature matrix of each product:
|Element|product1|product2|product3|product4
| :------: | :------: | :------: | :------: | :------: |
|minHash1|1|3|0|1
|minHash2|1|2|0|0
#### 2) Locality-Sensitive Hashing
Now we have reduced large user sets into small signatures, but we still have a large number of product pairs to compare. As a result, LSH hashes the signatures instead to reduce the pairs we need to compare. And we just estimate similarity for sets in the same bucket.
To make it easier, according to the fact that the sample rows of similar signatures should also be similar, we divide signatures into “bands”, each band has same rows. Hash each band individually, if each signature of one pair are hashed into the same bucket (it happends only if they are identical) in at least one band, the pair will be considered as a candidate pair.
### 3) Jaccard similarity & Cosine similarity
Now we have candidate product pairs, and we can calculate the similarity of each pair from the original characteristic matrix.
* Jaccard similarity = size of intersection / size of union
* Cosine similarity = 
cos(\theta ) = \frac{A\cdot B}{\left \| A \right \|\left \| B \right \|} = \frac{\sum_{i=1}^{n}{A}_i\times{B}_i}{\sqrt{\sum_{i=1}^{n}({A}_i)^2}\times \sqrt{\sum_{i=1}^{n}({B}_i)^2}}

## 4. Usage Example
Open your terminal, using following command line in the top-level Spark directory to launch the applications. 
`$SPARK_HOME/bin/spark-submit --class <main-class> <application-jar> args(0) [args(1)] args(2)`
args(0): <input_path> (total datasets)
args(1)[optional]: <input_path> testing datasets (only for two collaborative filtering codes)
args(2): <output_path>

For example,

`.bin/spark-submit --class JaccardLSH Tong_Lyu_hw3.jar /video_small_num.csv /Tong_Lyu_SimilarProducts_Jaccard.txt`

`.bin/spark-submit --class ModelBasedCF Tong_Lyu_hw3.jar /video_small_num.csv /video_small_testing_num.csv /Tong_Lyu_ModelBasedCF.txt` 

`.bin/spark-submit --class UserBasedCF Tong_Lyu_hw3.jar /video_small_num.csv /video_small_testing_num.csv /Tong_Lyu_UserBasedCF.txt`

Tips:
1)	The command line should be executed under the directory of `$SPARK_HOME/bin`

2)	The path of jar file and input data is relative, please make sure the jar file and input data are also under the current directory where you run the command line.

3)	The program will automatically generate an output file for each execution under the same current folder. 

4)  The code of Jaccard LSH reads ground-truth data automatically from the current folder, please make sure the file /video_small_ground_truth_jaccard.csv exists.

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
|>= 0 and < 1|2247
|>= 1 and < 2|1314
|>= 2 and < 3|462
|>= 3 and < 4|175
|>= 4|19
|RMSE|1.3996232727594942
|Time|45s
