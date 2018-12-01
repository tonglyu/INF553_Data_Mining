# Spring 2018 INF553 - HW5
This assignment aims to implement some streaming algorithms. One is some analysis of Twitter stream. The other runs on a simulated data stream.

## Environment Requirements
* Java: 1.8
* Scala: 2.11.12
* Spark: 2.2.1 
  
## Datasets
* Streaming Simulation: userId.txt

## Using Example
Open your terminal, using
`$SPARK_HOME/bin/spark-submit --class <main-class> <application-jar> <arguments>` in the top-level Spark directory to launch the applications.

For example,

`.bin/ spark-submit --class TrackHashTags Tong_Lyu_hw5.jar JuqoMsFuPiTekYhOBb5gLdgA2 Gp5QwAl3D2bVdgVct5vWudmyS5W7Dg2rzXIZ8LCST7c5aGjZb7 4911824923-NgkjTAAinuLGOQ1HAQGoNMvwUS2ZlkkcNPiKrHH 8JDJ6Q2I6bZxPY6kypNrJvUByzaz3w3vy24McEwfTqQoa` 

`.bin/ spark-submit --class AveTweetLength Tong_Lyu_hw5.jar JuqoMsFuPiTekYhOBb5gLdgA2 Gp5QwAl3D2bVdgVct5vWudmyS5W7Dg2rzXIZ8LCST7c5aGjZb7 4911824923-NgkjTAAinuLGOQ1HAQGoNMvwUS2ZlkkcNPiKrHH 8JDJ6Q2I6bZxPY6kypNrJvUByzaz3w3vy24McEwfTqQoa` 

`.bin/ spark-submit --class UniqueUserCount Tong_Lyu_hw5.jar localhost 9999`

Tips:

1) The command line should be executed under the directory of `$SPARK_HOME/bin`

2) The path of jar file and input data is relative, please make sure the jar file and input data are also under the current directory where you run the command line.

3) The program will automatically generate an output file for each execution under the same current folder.
