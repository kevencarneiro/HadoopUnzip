import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.CombineFileRecordReader;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;

public class CustomCombineFileInputFormat extends CombineFileInputFormat<NullWritable, NullWritable> {

    @Override
    protected boolean isSplitable(JobContext context, Path file) {
        return false;
    }

    @Override
    public RecordReader<NullWritable, NullWritable> createRecordReader(InputSplit split, TaskAttemptContext context)
            throws IOException {
        return new CombineFileRecordReader<>((CombineFileSplit) split, context, CustomRecordReader.class);
    }

    public static class CustomRecordReader extends RecordReader<NullWritable, NullWritable> {
        private CombineFileSplit split;
        private TaskAttemptContext context;
        private int currentIndex;
        private boolean processed;
        private Path[] paths;

        public CustomRecordReader(CombineFileSplit split, TaskAttemptContext context, Integer idx) {
            this.split = split;
            this.context = context;
            this.currentIndex = idx;
            this.processed = false;
            this.paths = split.getPaths();
        }

        @Override
        public void initialize(InputSplit split, TaskAttemptContext context) throws IOException, InterruptedException {
            // No specific initialization required
        }

        @Override
        public boolean nextKeyValue() throws IOException, InterruptedException {
            if (currentIndex < paths.length) {
                // Each call processes one file
                processed = false;
                return true;
            }
            return false;
        }

        @Override
        public NullWritable getCurrentKey() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public NullWritable getCurrentValue() throws IOException, InterruptedException {
            return NullWritable.get();
        }

        @Override
        public float getProgress() throws IOException, InterruptedException {
            return (float) currentIndex / paths.length;
        }

        @Override
        public void close() throws IOException {
            // Clean up resources if needed
        }
    }
}
