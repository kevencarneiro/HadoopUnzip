import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class UnzipDriver {
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: unzip <in> <out> [deleteOriginal] [verbose]");
            System.exit(2);
        }
        // Check if the deleteOriginal argument is provided
        if (otherArgs.length >= 3 && otherArgs[2].equalsIgnoreCase("deleteOriginal")) {
            conf.setBoolean("unzip.deleteOriginal", true);
        }
        // Check if the verbose argument is provided
        if (otherArgs.length >= 4 && otherArgs[3].equalsIgnoreCase("verbose")) {
            conf.setBoolean("unzip.verbose", true);
        }

        Job job = Job.getInstance(conf, "Unzip Files");
        job.setJarByClass(UnzipDriver.class);
        job.setMapperClass(UnzipMapper.class);
        job.setReducerClass(UnzipReducer.class);
        job.setInputFormatClass(ZipFileInputFormat.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        // Allow the job to use as many mappers as needed
        job.setNumReduceTasks(0);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
