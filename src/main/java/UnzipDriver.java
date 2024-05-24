import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

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
        job.setInputFormatClass(ZipFileInputFormat.class); // Specify the custom InputFormat
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(BytesWritable.class);
        job.setOutputFormatClass(SequenceFileOutputFormat.class);

        FileInputFormat.addInputPath(job, new Path(otherArgs[0]));
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[1]));

        // Start a separate thread to monitor progress
        Thread progressMonitor = new Thread(new ProgressMonitor(job));
        progressMonitor.start();

        // Wait for job completion
        boolean jobCompleted = job.waitForCompletion(true);

        // Wait for the progress monitor thread to finish
        progressMonitor.join();

        System.exit(jobCompleted ? 0 : 1);
    }

    private static class ProgressMonitor implements Runnable {
        private final Job job;

        public ProgressMonitor(Job job) {
            this.job = job;
        }

        @Override
        public void run() {
            try {
                while (!job.isComplete()) {
                    // Print the progress
                    float mapProgress = job.mapProgress();
                    float reduceProgress = job.reduceProgress();
                    System.out.printf("Map progress: %.2f%%, Reduce progress: %.2f%%\n", mapProgress * 100, reduceProgress * 100);

                    // Sleep for a while before checking again
                    Thread.sleep(5000); // Check every 5 seconds
                }
            } catch (InterruptedException | IOException e) {
                e.printStackTrace();
            }
        }
    }
}
