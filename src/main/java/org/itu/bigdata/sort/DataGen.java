package org.itu.bigdata.sort;

/**
 * Created by kiran on 1/3/16.
 */
import org.apache.commons.lang.RandomStringUtils;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class DataGen extends Configured implements Tool {

    public static final String NUM_ROWS = "mapreduce.datasort.num-rows";

    static class DataInputFormat
            extends InputFormat<LongWritable, NullWritable> {

        /**
         * An input split consisting of a range on numbers.
         */
        static class DataInputSplit extends InputSplit implements Writable {
            long firstRow;
            long rowCount;

            public DataInputSplit() { }

            public DataInputSplit(long offset, long length) {
                firstRow = offset;
                rowCount = length;
            }

            public long getLength() throws IOException {
                return 0;
            }

            public String[] getLocations() throws IOException {
                return new String[]{};
            }

            public void readFields(DataInput in) throws IOException {
                firstRow = WritableUtils.readVLong(in);
                rowCount = WritableUtils.readVLong(in);
            }

            public void write(DataOutput out) throws IOException {
                WritableUtils.writeVLong(out, firstRow);
                WritableUtils.writeVLong(out, rowCount);
            }
        }

        /**
         * A record reader that will generate a range of numbers.
         */
        static class DataRecordReader
                extends RecordReader<LongWritable, NullWritable> {
            long startRow;
            long finishedRows;
            long totalRows;
            LongWritable key = null;

            public DataRecordReader() {
            }

            public void initialize(InputSplit split, TaskAttemptContext context)
                    throws IOException, InterruptedException {
                startRow = ((DataInputSplit)split).firstRow;
                finishedRows = 0;
                totalRows = ((DataInputSplit)split).rowCount;
            }

            public void close() throws IOException {
                // NOTHING
            }

            public LongWritable getCurrentKey() {
                return key;
            }

            public NullWritable getCurrentValue() {
                return NullWritable.get();
            }

            public float getProgress() throws IOException {
                return finishedRows / (float) totalRows;
            }

            public boolean nextKeyValue() {
                if (key == null) {
                    key = new LongWritable();
                }
                if (finishedRows < totalRows) {
                    key.set(startRow + finishedRows);
                    finishedRows += 1;
                    return true;
                } else {
                    return false;
                }
            }

        }

        public RecordReader<LongWritable, NullWritable>
        createRecordReader(InputSplit split, TaskAttemptContext context)
                throws IOException {
            return new DataRecordReader();
        }

        /**
         * Create the desired number of splits, dividing the number of rows
         * between the mappers.
         */
        public List<InputSplit> getSplits(JobContext job) {
            long totalRows = getNumberOfRows(job);
            int numSplits = job.getConfiguration().getInt("mapreduce.job.maps", 1);
//            LOG.info("Generating " + totalRows + " using " + numSplits);
            List<InputSplit> splits = new ArrayList<InputSplit>();
            long currentRow = 0;
            for(int split = 0; split < numSplits; ++split) {
                long goal =
                        (long) Math.ceil(totalRows * (double)(split + 1) / numSplits);
                splits.add(new DataInputSplit(currentRow, goal - currentRow));
                currentRow = goal;
            }
            return splits;
        }

    }

    static long getNumberOfRows(JobContext job) {
        return job.getConfiguration().getLong(NUM_ROWS, 0);
    }

    static void setNumberOfRows(Job job, long numRows) {
        job.getConfiguration().setLong(NUM_ROWS, numRows);
    }




    public static class DataGenMapper extends Mapper<LongWritable, NullWritable, Text, Text> {

        @Override
        protected void map(LongWritable key, NullWritable ignore, Context context)
                throws IOException, InterruptedException {
            String randomData = RandomStringUtils.random(100, true, false);

            context.write(new Text(randomData),new Text());

        }
    }







    public int run(String[] args) throws Exception {
        // TODO Auto-generated method stub
        if(args.length != 2) {
            System.out.printf("Usage: %s [generic options] <number of 100 byte rows> <output dir>\n",
                    getClass().getSimpleName());
            ToolRunner.printGenericCommandUsage(System.out);
            return -1;
        }


        Job job = new Job(getConf());
        job.setJarByClass(DataGen.class);
        job.setJobName(getClass().getName());

        setNumberOfRows(job, Long.parseLong(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        job.setMapperClass(DataGenMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setInputFormatClass(DataInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);
        job.setNumReduceTasks(0);
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

        System.exit(ToolRunner.run(new DataGen(), args));

    }

}