# Spring 2018 INF553 - HW4
This assignment aims to implement Girvan-Newman algorithm with Spark. The program calculates the betweenness of each edge in the graph, and then sequentially remove the edge with the maximum betweenness to construct community. Each time, the program computes the modularity of the entire graph, and finally output the community with the maximum modularity.
## 1. Environment Requirements
* Java: 1.8
* Scala: 2.11.12
* Spark: 2.2.1 
## 2. Datasets
* Total datasets: /video_small_num.csv
## 3. Usage Example
Open your terminal, using following command line in the top-level Spark directory to launch the applications. 
`$SPARK_HOME/bin/spark-submit --class <main-class> <application-jar> <input_file> <output_file>`  in the top-level Spark directory to launch the applications. 

For example,
`.bin/ spark-submit --class Betweenness Tong_Lyu_hw4.jar /video_small_num.csv /Tong_Lyu_Betweenness.txt `

`.bin/ spark-submit --class Community Tong_Lyu_hw4.jar /video_small_num.csv /Tong_Lyu_Community.txt `

Tips:
1)	The command line should be executed under the directory of `$SPARK_HOME/bin`

2)	The path of jar file and input data is relative, please make sure the jar file and input data are also under the current directory where you run the command line.

3)	The program will automatically generate an output file for each execution under the same current folder.
