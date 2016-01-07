# bigdata-sort
Data Generation, Sorting and Validation MR jobs

## **DataGen**

`hadoop jar <full path to the MR jar file> org.itu.bigdata.sort.DataGen -Dmapreduce.job.maps=<number of mappers you want to run> <number of 100byte rows needed to be created> <output directory>`

### Example:
below command will run a MR job with 4 map tasks and creates 1G(100bytes*10000000) of data

`hadoop jar /tmp/gensort-1.0-SNAPSHOT-jar-with-dependencies.jar org.itu.bigdata.sort.DataGen -Dmapreduce.job.maps=4 10000000 /user/hdfs/datagen-output`

## **DataSort**

`hadoop jar <full path to the MR jar file> org.itu.bigdata.sort.DataSort -Dmapred.reduce.tasks=<number of reducers you want to run> <input data directory> <output directory>`

### Example:
below command will run a MR job with 4 reduce tasks and sorts the data outputted from DataGen

`hadoop jar /tmp/gensort-1.0-SNAPSHOT-jar-with-dependencies.jar org.itu.bigdata.sort.DataSort -Dmapred.reduce.tasks=4 /user/hdfs/datagen-output /user/hdfs/datasort-output`

## **DataValidate**

`hadoop jar <full path to the MR jar file> org.itu.bigdata.sort.DataValidate <input data directory> <output report directory>`

### Example:
below command will run a MR job and takes sorted data as input and generates a report where the data is not sorted

`hadoop jar /tmp/gensort-1.0-SNAPSHOT-jar-with-dependencies.jar org.itu.bigdata.sort.DataValidate /user/hdfs/datasort-output /user/hdfs/dataval-report`
