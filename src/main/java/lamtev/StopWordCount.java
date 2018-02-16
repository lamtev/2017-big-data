package lamtev;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.Set;

import static lamtev.StopWordCount.Counters.STOP_WORDS_COUNTER;
import static lamtev.StopWordCount.Counters.WORDS_COUNTER;

public class StopWordCount extends Configured implements Tool {

    private static final Set<String> STOP_WORDS = new HashSet<>();

    public static void main(String[] args) throws Exception {
        final int returnCode = ToolRunner.run(new StopWordCount(), args);
        System.exit(returnCode);
    }

    @Override
    public int run(String[] args) throws Exception {
        final Job job = Job.getInstance(this.getConf(), "Stop Word Count");
        final java.nio.file.Path pathToStopWordsFile = Paths.get(args[1], "stop_words_en.txt");
        Files.lines(pathToStopWordsFile).forEach(STOP_WORDS::add);

        job.setJarByClass(StopWordCount.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        job.setMapperClass(StopWCMapper.class);
        job.setReducerClass(StopWCReducer.class);
        job.setCombinerClass(WordCount.WCReducer.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        final long stopWordCount = job.getCounters().findCounter(STOP_WORDS_COUNTER).getValue();
        final long wordCount = job.getCounters().findCounter(WORDS_COUNTER).getValue();
        final long percents = Math.round((double) stopWordCount / wordCount * 100);
        final File file = new File(args[1], "output");
        Files.write(file.toPath(), String.valueOf(percents).getBytes());

        return job.waitForCompletion(true) ? 0 : 1;
    }

    enum Counters {
        WORDS_COUNTER,
        STOP_WORDS_COUNTER
    }

    static class StopWCMapper extends Mapper<LongWritable, Text, IntWritable, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            final String[] entry = value.toString().split("\\s+");
            context.write(new IntWritable(Integer.valueOf(entry[1])), new Text(entry[0]));
        }
    }

    static class StopWCReducer extends Reducer<Text, IntWritable, IntWritable, Text> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) {
            final boolean keyIsStopWord = STOP_WORDS.contains(key.toString());
            values.forEach(it -> {
                context.getCounter(WORDS_COUNTER).increment(it.get());
                if (keyIsStopWord) {
                    context.getCounter(STOP_WORDS_COUNTER).increment(it.get());
                }
            });
        }
    }

}
