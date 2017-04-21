# Analysis-of-Flight-Dataset
Analysis of Flight Dataset (MapReduce, Spark):  Analyzed flight dataset to retrieve average delay, pair of cities having highest cancellation rate, most frequent destination and deployed the application on yarn cluster.

Dataset:
The flight dataset (for years 1987-2000) from the following source: 
http://stat-computing.org/dataexpo/2009/the-data.html 
Download files 1987-2000 one by one.
Format of the dataset: Each data file is comma separated CSV file and contain flight information for a year.

1.	Average delay using SCALA:
Take the flight dataset for year 2001 and write a spark program which returns the top 10 flights with the highest departure delay in year 2001.
Your output should be in the following form: 
Unique_Carrier 		Source 		Dest 	year/month/day 	depDelay

2.	Average delay using Combiners:
Writing Custom Combiner MapReduce program with a custom combiner to compute the average departure delay per unique carrier. Copy the flight data to hdfs, create a jar file and run the program on your three-node cluster. Once your job is completed, record the job elapsed and reduce shuffle size (The reduce shuffle size is printed on the terminal once the job is completed. You can find the job elapsed in Yarn GUI, the application history). Then go back to your program and comment the line for using the combiner and run your program again without combiner on the cluster. Record the job elapsed time and the reduce shuffle size again. 
Program run faster when using combiners after analyzing the reduce shuffle size and job elapsed time.

3.	Maximum cancellation flight pair of cities with Unique Carrier:
MapReduce programs to find a pair of cities (airport) with the highest cancellation rate for each unique carrier. 

4.	Most Frequent Destination
MapReduce programs to compute the most frequent destination travelled for the flight dataset.
