package main.java;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.*;

import java.io.IOException;
import java.util.Iterator;
import java.util.StringTokenizer;

public class MIT805exam {

    public static class Map extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {
        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();

        public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            String line = value.toString();
            String[] columns = line.split(",");
            String payment_type = columns[0];
            StringTokenizer tokenizer = new StringTokenizer(payment_type, ",");
            while (tokenizer.hasMoreTokens()) {
                word.set(tokenizer.nextToken());
                output.collect(word, one);
            }
        }
    }

    public static class Reduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, IntWritable> {
        public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
            int sum = 0;
            while (values.hasNext()) {
                sum += values.next().get();
            }
            output.collect(key, new IntWritable(sum));
        }
    }

    public static void main(String[] args) throws Exception {
        JobConf conf = new JobConf(MIT805exam.class);
        conf.setJobName("MIT805exam");

        conf.setOutputKeyClass(Text.class);
        conf.setOutputValueClass(IntWritable.class);

        conf.setMapperClass(Map.class);
        conf.setCombinerClass(Reduce.class);
        conf.setReducerClass(Reduce.class);

        conf.setInputFormat(TextInputFormat.class);
        conf.setOutputFormat(TextOutputFormat.class);

//        Path where you have saved the yellow_tripdata_2020-06.csv input file for processing
        FileInputFormat.setInputPaths(conf, new Path("/Users/michielvstaden/IdeaProjects/MIT805exam/src/main/java/input/train.csv"));
//        Path where you want the output file with a summary of payment type distribution for latest month
//        Make sure to delete this output folder before re-running the code to prevent error message
        FileOutputFormat.setOutputPath(conf, new Path("/Users/michielvstaden/IdeaProjects/MIT805exam/src/main/java/output"));

        JobClient.runJob(conf);
    }
}