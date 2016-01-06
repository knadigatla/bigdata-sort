package org.itu.bigdata.sort;

/**
 * Created by kiran on 1/3/16.
 */
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataSort extends Configured implements Tool {

    static class DataSortMapper extends Mapper<LongWritable, Text, Text, Text> {

        @Override
        protected void map(LongWritable key, Text val, Context context)
                throws IOException, InterruptedException {
            context.write(val,new Text());
        }
    }

    static class CustomPartitioner extends Partitioner<Text, Text> {

        @Override
        public int getPartition(Text key, Text val, int noOfPartitions) {

            int mid = noOfPartitions/2;
            int incrA = (int)Math.ceil(26.0/mid);
            int incra = (int)Math.ceil(26.0/(noOfPartitions-mid));

            for(int i=0;i<mid;i++) {
                int firstChar = key.charAt(0);
                if(firstChar >= 65+(i*incrA) && firstChar < Math.min((65+((i+1)*incrA)),91)) {
                    return i;
                }
            }
            for(int i=mid;i<noOfPartitions;i++) {
                int firstChar = key.charAt(0);
                if(firstChar >= 97+((i-mid)*incra) && firstChar < Math.min((97+((i-mid+1)*incra)),123)) {
                    return i;
                }
            }

            return 100;
        }

    }

    static class DataSortReducer extends Reducer<Text,Text,Text,Text> {

        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException  {
            context.write(key, new Text());

        }

    }





    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        if(args.length != 2) {
            System.out.printf("Usage: %s [generic options] <input dir> <output dir>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }


        Job job = new Job(getConf());
        job.setJarByClass(DataSort.class);
        job.setJobName(getClass().getName());
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DataSortMapper.class);
        job.setReducerClass(DataSortReducer.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(TextInputFormat.class);
//        job.setOutputFormatClass(TextOutputFormat.class);
        job.setPartitionerClass(CustomPartitioner.class);
//        job.setNumReduceTasks(0);
        if(job.waitForCompletion(true)) {
            return 0;
        }
        return 1;
    }

    /**
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {

        System.exit(ToolRunner.run(new DataSort(), args));

    }

}