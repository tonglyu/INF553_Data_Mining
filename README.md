# Spring 2018 INF553 - HW1
This assignment aims to get familiar with Spark and analyze bigdata using Spark. The datasets are from Amazon Product data, including the data of individual product category: Toys and Games ratings and the metadata of the products. The programs calculate average rating of each product and each brand.

## Environment Requirements
* Scala: 2.11.12
* Spark: 2.2.1 

## Installation
This installation is for Mac OS X.

Open your terminal, install sbt 1.1.0:
  `brew install sbt`
  
Download the Spark 2.2.1 with Hadoop 2.7 from： http://spark.apache.org/downloads.html
  
## Datasets
For Task1, only dataset1 is required. And for Task2, both dataset1 and dataset2 is required.
* Dataset1:  _reviews_Toys_and_Games.json_, please decompress the folder after downloading.
http://snap.stanford.edu/data/amazon/productGraph/categoryFiles/reviews_Toys_and_Games.json.gz
* Dataset2: _metadata.json_, please decompress the folder after downloading.
http://snap.stanford.edu/data/amazon/productGraph/metadata.json.gz

## Code Example
Testing first requires Open your terminal, using `.bin/rspark-submit --class <main-class> <application-jar> [application-arguments]` in the top-level Spark directory to launch the applications.
For example,

* Task1
  
 ` .bin/spark-submit --class Task1 path/to/Tong_Lyu_task1.jar path/to/reviews_Toys_and_Games.json path/to/Tong_Lyu_task1.csv`
* Task2 

 ` .bin/spark-submit --class Task2 path/to/Tong_Lyu_task2.jar path/to/reviews_Toys_and_Games.json path/to/metadata.json path/to/Tong_Lyu_task2.csv`

## Tests
This are the results running in the author's laptop:
* Task1
! [Task1] 
* Task2
