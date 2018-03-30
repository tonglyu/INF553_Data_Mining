# Spring 2018 INF553 - HW3
This assignment aims to implement two algorithms with Spark. The first is Locality-Sensitive Hashing (LSH),  which calculates the similarity of products in the datasets. Jaccard similarity and Cosine similarity are calculated respectively to select the candidate pairs whose similarity exceeds the threshold. 

The second one is Collaborative Filtering with model-based and user-based algorithms respectively. The program predicts the ratings of specific user and product, and calculate RMSE to compare the predictions with the testing data.
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

Pick a permutation of rows, (i.e. f(userId) = (a * userId + b) % m, m is the number of users), 

Minhash(Sj) = no. of 1st row i where S[i, j] = 1, then we can get the signature matrix of each product:

|Element|product1|product2|product3|product4
| :------: | :------: | :------: | :------: | :------: |
|minHash1|1|3|0|1
|minHash2|1|2|0|0

#### 2) Locality-Sensitive Hashing
Now we have reduced large user sets into small signatures, but we still have a large number of product pairs to compare. As a result, LSH hashes the signatures instead to reduce the pairs we need to compare. And we just estimate similarity for sets in the same bucket.

To make it easier, according to the fact that the sample rows of similar signatures should also be similar, we divide signatures into “bands”, each band has same rows. Hash each band individually, if each signature of one pair are hashed into the same bucket (it happends only if they are identical) in at least one band, the pair will be considered as a candidate pair.
#### 3) Jaccard similarity & Cosine similarity
Now we have candidate product pairs, and we can calculate the similarity of each pair from the original characteristic matrix, output the pairs whose similarity exceeds the threshold.
* Jaccard similarity = size of intersection / size of union
* Cosine similarity = 

<div align=center><a  href="https://www.codecogs.com/eqnedit.php?latex=cos(\theta&space;)&space;=&space;\frac{A\cdot&space;B}{\left&space;\|&space;A&space;\right&space;\|\left&space;\|&space;B&space;\right&space;\|}&space;=&space;\frac{\sum_{i=1}^{n}{A}_i\times{B}_i}{\sqrt{\sum_{i=1}^{n}({A}_i)^2}\times&space;\sqrt{\sum_{i=1}^{n}({B}_i)^2}}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?cos(\theta&space;)&space;=&space;\frac{A\cdot&space;B}{\left&space;\|&space;A&space;\right&space;\|\left&space;\|&space;B&space;\right&space;\|}&space;=&space;\frac{\sum_{i=1}^{n}{A}_i\times{B}_i}{\sqrt{\sum_{i=1}^{n}({A}_i)^2}\times&space;\sqrt{\sum_{i=1}^{n}({B}_i)^2}}" title="cos(\theta ) = \frac{A\cdot B}{\left \| A \right \|\left \| B \right \|} = \frac{\sum_{i=1}^{n}{A}_i\times{B}_i}{\sqrt{\sum_{i=1}^{n}({A}_i)^2}\times \sqrt{\sum_{i=1}^{n}({B}_i)^2}}" /></a></div>

where A and B are feature(user) vectors of each product.
The code of Cosine-LSH refers to https://github.com/soundcloud/cosine-lsh-join-spark, which needs to use its library to implement.
####  4) Precision and Recall
The ground truth dataset contains all the products pairs that have cosine similarity above or equal to 0.5. The precision and recall can be calculated with the execution result and ground truth datasets.

Confusion Matrix

| |Postive(predicted)|Negative(predicted)
| :------: | :------: | :------: | 
|Postive(actual)|True positive (TP)|False Negative(FN)
|Negative(actual)|False positive (FP)|True Negative(TN)

Precison = TP / (TP + FP)

Recall = TP / (TP + FN)

### Collaborative Filtering
The /video_small_testing_num.csv datasets are a subset of the video_small_num.csv, each having the same columns as its parent. The programs predict the ratings of every <userId> and <productId> combination in the test files. 

#### 1) Model-based CF
The code joins Spark MLlib and use ALS model to implement the model-based CF.

More about Spark MLlib from: http://spark.apache.org/docs/latest/mllib-collaborative-filtering.html
#### 2) User-based CF
A subset of other users is chosen based on their similarity to the active user. A weighted combination of their ratings is used to make predictions for the active user.

Steps:

1. Assign a weight to all users w.r.t. similarity with the active user.

2. Select k users that have the highest similarity with active user (the neighborhood).

3. Compute a prediction from a weighted combination of the selected neighbors’ ratings.

In this program, we don't take the top-k users, and just use all other users' ratings instead.

* Pearson correlation 
  For users (u, v), Pearson correlation is

<div align=center><a href="https://www.codecogs.com/eqnedit.php?latex=w_{u,v}&space;=&space;\frac{\sum_{i&space;\in&space;I}&space;(r_{u,i}&space;-&space;\bar{r}_{u})(r_{v,i}&space;-&space;\bar{r}_{v})}{\sqrt{\sum_{i&space;\in&space;I}&space;(r_{u,i}&space;-&space;\bar{r}_{u})^2}\sqrt{\sum_{i&space;\in&space;I}&space;(r_{v,i}&space;-&space;\bar{r}_{v})^2}}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?w_{u,v}&space;=&space;\frac{\sum_{i&space;\in&space;I}&space;(r_{u,i}&space;-&space;\bar{r}_{u})(r_{v,i}&space;-&space;\bar{r}_{v})}{\sqrt{\sum_{i&space;\in&space;I}&space;(r_{u,i}&space;-&space;\bar{r}_{u})^2}\sqrt{\sum_{i&space;\in&space;I}&space;(r_{v,i}&space;-&space;\bar{r}_{v})^2}}" title="w_{u,v} = \frac{\sum_{i \in I} (r_{u,i} - \bar{r}_{u})(r_{v,i} - \bar{r}_{v})}{\sqrt{\sum_{i \in I} (r_{u,i} - \bar{r}_{u})^2}\sqrt{\sum_{i \in I} (r_{v,i} - \bar{r}_{v})^2}}" /></a></div>

  where I summations are over the items that both user u and v have rated, and <a href="https://www.codecogs.com/eqnedit.php?latex=\bar{r}_{u}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\bar{r}_{u}" title="\bar{r}_{u}" /></a> is the average rating of the co-rated items of the uth user.

  Note: When calculating these similarities, look only at the co-rated items of user (u, v).

* Predictions
  Weighted average of their ratings is used to generate predictions. 

  To make a prediction for an active user a on an item i:

<div align=center><a href="https://www.codecogs.com/eqnedit.php?latex=P_{a,i}&space;=&space;\bar{r}_a&space;&plus;&space;\frac{\sum_{u&space;\in&space;U}(r_{u,i}&space;-&space;\bar{r}_u)\cdot&space;w_{a,u}}{\sum_{u&space;\in&space;U}\left&space;|&space;w_{a,u}&space;\right&space;|}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?P_{a,i}&space;=&space;\bar{r}_a&space;&plus;&space;\frac{\sum_{u&space;\in&space;U}(r_{u,i}&space;-&space;\bar{r}_u)\cdot&space;w_{a,u}}{\sum_{u&space;\in&space;U}\left&space;|&space;w_{a,u}&space;\right&space;|}" title="P_{a,i} = \bar{r}_a + \frac{\sum_{u \in U}(r_{u,i} - \bar{r}_u)\cdot w_{a,u}}{\sum_{u \in U}\left | w_{a,u} \right |}" /></a></div>

  where <a href="https://www.codecogs.com/eqnedit.php?latex=\bar{r}_{a}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\bar{r}_{a}" title="\bar{r}_{a}" /></a> and  <a href="https://www.codecogs.com/eqnedit.php?latex=\bar{r}_{u}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?\bar{r}_{u}" title="\bar{r}_{u}" /></a> are the average ratings of user a and u on all other rated items, and is the weight between the user a and user u. The summations are over all the users who have rated the item i.

  Note: When making predictions, calculate average of all rated items except item i for users a and u. 

#### 3) Evaluation 
After achieving the prediction for ratings, we compare the result to the correspond ground truth and compute the absolute differences. The absolute differences are divided into 5 levels, and the numbers of the prediction for each level are counted as following:

\>=0 and <1: 12345 //there are 12345 predictions with a < 1 difference from the ground truth

\>=1 and <2: 123

\>=2 and <3: 1234

\>=3 and <4: 1234

\>=4: 12

Additionally, RMSE (Root Mean Squared Error) is computed to evaluate the model.

<div align=center><a href="https://www.codecogs.com/eqnedit.php?latex=RMSE&space;=&space;\sqrt{\frac{1}{n}\sum_{i}({Prediction}_i&space;-&space;{Rate}_i)^2}" target="_blank"><img src="https://latex.codecogs.com/gif.latex?RMSE&space;=&space;\sqrt{\frac{1}{n}\sum_{i}({Prediction}_i&space;-&space;{Rate}_i)^2}" title="RMSE = \sqrt{\frac{1}{n}\sum_{i}({Prediction}_i - {Rate}_i)^2}" /></a></div>
  
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

