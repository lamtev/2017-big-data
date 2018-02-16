package lamtev;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class DescendingWordCount extends Configured implements Tool {

    public static void main(final String[] args) throws Exception {
        final int wcReturnCode = ToolRunner.run(new WordCount(), args);
        final int dwcReturnCode = ToolRunner.run(new DescendingWordCount(), args);

        if (wcReturnCode == dwcReturnCode) {
            System.exit(wcReturnCode);
        } else if (wcReturnCode != 0) {
            System.exit(wcReturnCode);
        } else {
            System.exit(dwcReturnCode);
        }
    }

    @Override
    public int run(String[] args) throws Exception {
        final Job job = Job.getInstance(this.getConf(), "Descending Word Count");
        job.setJarByClass(DescendingWordCount.class);

        job.setMapperClass(DescendingWCMapper.class);
        job.setReducerClass(DescendingWCReducer.class);
        job.setSortComparatorClass(DescendingComparator.class);

        TextInputFormat.addInputPath(job, new Path(args[0]));
        TextOutputFormat.setOutputPath(job, new Path(args[1]));

        job.setInputFormatClass(TextInputFormat.class);

        job.setMapOutputKeyClass(IntWritable.class);
        job.setMapOutputValueClass(Text.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    static class DescendingWCMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] entry = value.toString().split("\\s+");
            context.write(new IntWritable(Integer.valueOf(entry[1])), new Text((entry[0])));
        }
    }

    static class DescendingWCReducer extends Reducer<IntWritable, Text, IntWritable, Text> {
        @Override
        protected void reduce(IntWritable key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            for (final Text value : values) {
                context.write(key, value);
            }
        }
    }

    static class DescendingComparator extends WritableComparator {
        @Override
        public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
            return -Integer.compare(readInt(b1, s1), readInt(b2, s2));
        }
    }

}
