package org.itu.bigdata.wc;

/**
 * Created by kiran on 1/3/16.
 */
import java.io.IOException;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class WCMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        String[] words = value.toString().split("\\W+");
        for(String word:words) {
            if(word.length() > 0)
                context.write(new Text(word), new IntWritable(1));
        }

    }
}