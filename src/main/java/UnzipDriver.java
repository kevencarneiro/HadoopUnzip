import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.util.logging.Logger;

public class UnzipDriver {
    private static final Logger LOGGER = Logger.getLogger(UnzipDriver.class.getName());
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: unzip <in> <out> [deleteOriginal]");
            System.exit(2);
        }

        // Check if the deleteOriginal argument is provided
        if (otherArgs.length == 3 && otherArgs[2].equals("deleteOriginal")) {
            conf.setBoolean("unzip.deleteOriginal", true);
        }

        LOGGER.info("mapreduce.map.java.opts: " + conf.get("mapreduce.map.java.opts"));
        LOGGER.info("mapreduce.reduce.java.opts: " + conf.get("mapreduce.reduce.java.opts"));

        Job job = Job.getInstance(conf, "Unzip Files");
        // Set the Jar by finding where a given class came from
        job.setJarByClass(UnzipDriver.class);

        // Set the mapper class
        job.setMapperClass(UnzipMapper.class);

        // No need for a reducer in this job
        job.setNumReduceTasks(0);

        // Set the output key and value classes
        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(NullWritable.class);

        // Set the input format class
        job.setInputFormatClass(CombineFileInputFormat.class);
        CombineFileInputFormat.addInputPath(job, new Path(args[0]));

        // Set the output format class
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        // Set the maximum split size (optional)
        CombineFileInputFormat.setMaxInputSplitSize(job, 256 * 1024 * 1024); // 256 MB

        // Submit the job
        boolean jobCompletedSuccessfully = job.waitForCompletion(true);
        // Ensure the job is complete
        if (jobCompletedSuccessfully) {
            System.out.println("Job completed successfully.");
        } else {
            System.out.println("Job failed.");
            System.exit(1);
        }
    }
}
