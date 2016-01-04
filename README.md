# bigdata-sort
Data Generation, Sorting and Validation MR jobs

## **DataGen**

`hadoop jar <full path to the MR jar file> org.itu.bigdata.sort.DataGen -Dmapreduce.job.maps=<number of mappers you want to run> <number of 100byte rows needed to be created> <output directory>`

### Example:
below command will run a MR job with 4 map tasks and creates 1G(100bytes*10000000) of data

`hadoop jar /tmp/gensort-1.0-SNAPSHOT-jar-with-dependencies.jar org.itu.bigdata.sort.DataGen -Dmapreduce.job.maps=4 10000000 /user/hdfs/datagen-output`

## **DataSort**
-- to be updated

## **DataValidate**

-- to be updated
