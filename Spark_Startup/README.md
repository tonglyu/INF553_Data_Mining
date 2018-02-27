# Spring 2018 INF553 - HW1
This assignment aims to get familiar with Spark and analyze bigdata using Spark. The datasets are from Amazon Product data, including the data of individual product category: Toys and Games ratings and the metadata of the products. The programs calculate average rating of each product and each brand.

## Environment Requirements
* Java: 1.8
* Scala: 2.11.12
* Spark: 2.2.1 
  
## Datasets
For Task1, only dataset1 is required. And for Task2, both dataset1 and dataset2 is required.
* Dataset1:  _reviews_Toys_and_Games.json_, please decompress the folder after downloading.
http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Toys_and_Games_5.json.gz
* Dataset2: _metadata.json_, please decompress the folder after downloading.
http://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz

## Code Example
Testing first requires Open your terminal, using `.bin/rspark-submit --class <main-class> <application-jar> [application-arguments]` in the top-level Spark directory to launch the applications. The program only allows absolute path to access the file. Please don't access to a nonexistent file or directory, especially for the output file.
For example,

* Task1
  
 ` .bin/spark-submit --class task1 absolute_path/to/Tong_Lyu_task1.jar absolute_path/to/reviews_Toys_and_Games.json absolute_ path/to/Tong_Lyu_task1.csv`
* Task2 

 ` .bin/spark-submit --class task2 absolute_path/to/Tong_Lyu_task2.jar absolute_path/to/reviews_Toys_and_Games.json absolute_path/to/metadata.json absolute_path/to/Tong_Lyu_task2.csv`

## Tests
These are the results running on the author's laptop:
* Task1

<img src = "https://github.com/tonglyu/INF553/blob/master/task1.png" height="30%" width=30%>

* Task2

<img src = "https://github.com/tonglyu/INF553/blob/master/task2.png" height=30% width=30%>
