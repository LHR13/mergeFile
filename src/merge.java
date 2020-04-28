import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class merge {
    public merge() {
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.out.println("Usage:mergeFile<in>[<in>...]<out>");
            System.exit(2);
        }

        Job job = Job.getInstance(conf, "mergeFile");
        job.setJarByClass(merge.class);
        job.setMapperClass(merge.mergeMap.class);
        job.setReducerClass(merge.mergeReduce.class);
        job.setMapOutputKeyClass(LongWritable.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        for (int i = 0; i < otherArgs.length - 1; i++) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        Path path = new Path(otherArgs[1]);
        System.exit(job.waitForCompletion(true) ? 0:1);

    }

    public static class mergeMap extends Mapper<Object, Text, Text, Text> {
        private Text line = new Text();

        public mergeMap() {
        }

        public void MergeMap(Object key, Text value, Context context) throws IOException, InterruptedException {
            this.line = value;
            context.write(line, new Text(""));
        }
    }

    public static class mergeReduce extends Reducer<Text, Text, Text, Text> {
        public void MergeReducer(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            context.write(key, new Text(""));
        }
    }
}