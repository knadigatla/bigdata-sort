package org.itu.bigdata.sort;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.IOException;

/**
 * this class is to validate the data, it compare row and its previous row whether they are in order.
 * if it is not sorted it will make an entry in the output report else nothing will be written to report.
 * It also compares the last row of part file to the first row of next part file.
 */
public class DataValidate extends Configured implements Tool {
    private static final Text ERROR = new Text("error");

    private static final Logger LOG = LoggerFactory.getLogger(DataValidate.class);

    static class ValidateMapper extends Mapper<LongWritable,Text,Text,Text> {
        private Text lastVal;
        private String filename;

        /**
         * Get the final part of the input name
         * @param split the input split
         * @return the "part-r-00000" for the input
         */
        private String getFilename(FileSplit split) {
            return split.getPath().getName();
        }

        public void map(LongWritable key, Text value, Context context)
                throws IOException, InterruptedException {
            if (lastVal == null) {
                FileSplit fs = (FileSplit) context.getInputSplit();
                filename = getFilename(fs);
                context.write(new Text(filename + ":begin"), value);
                lastVal = new Text();
            } else {
                if (value.compareTo(lastVal) < 0) {
                    context.write(ERROR, new Text("misorder in " + filename +
                            " between " + lastVal +
                            " and " + value));
                }
            }

            lastVal.set(value);
        }

        public void cleanup(Context context)
                throws IOException, InterruptedException  {
            if (lastVal != null) {
                context.write(new Text(filename + ":end"), lastVal);
            }
        }
    }

    /**
     * Check the boundaries between the output files by making sure that the
     * boundary keys are always increasing.
     * Also passes any error reports along intact.
     */
    static class ValidateReducer extends Reducer<Text,Text,Text,Text> {
        private boolean firstKey = true;
        private Text lastKey = new Text();
        private Text lastValue = new Text();
        public void reduce(Text key, Iterable<Text> values,
                           Context context) throws IOException, InterruptedException  {
            if (ERROR.equals(key)) {
                for (Text val : values) {
                    context.write(key, val);
                }
            } else {
                Text value = values.iterator().next();
                if (firstKey) {
                    firstKey = false;
                } else {
                    if (value.compareTo(lastValue) < 0) {
                        context.write(ERROR,
                                new Text("bad key partitioning:\n  file " +
                                        lastKey + " key " +
                                        lastValue +
                                        "\n  file " + key + " key " +
                                        value));
                    }
                }
                lastKey.set(key);
                lastValue.set(value);
            }
        }

    }


    public int run(String[] args) throws Exception {
        Job job = Job.getInstance(getConf());
        if (args.length != 2) {
            System.out.printf("Usage: %s [generic options] <input data dirs> <output report dir>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }
        TextInputFormat.setInputPaths(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setJobName("DataValidate");
        job.setJarByClass(DataValidate.class);
        job.setMapperClass(ValidateMapper.class);
        job.setReducerClass(ValidateReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        // force a single reducer
        job.setNumReduceTasks(1);
        // force a single split
        FileInputFormat.setMinInputSplitSize(job, Long.MAX_VALUE);
        job.setInputFormatClass(TextInputFormat.class);
        return job.waitForCompletion(true) ? 0 : 1;
    }

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
        int res = ToolRunner.run(new Configuration(), new DataValidate(), args);
        System.exit(res);
    }
}
