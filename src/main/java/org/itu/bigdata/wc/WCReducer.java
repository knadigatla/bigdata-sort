package org.itu.bigdata.wc;

/**
 * Created by kiran on 1/3/16.
 */
import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * @author Kiran
 *
 */
public class WCReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> valList, Context context)
            throws IOException, InterruptedException {
        int sum=0;

        for(IntWritable val:valList) {
            sum += val.get();
        }
        System.out.println("the sum of the reducer is :"+sum);
        context.write(key, new IntWritable(sum));
    }


}
