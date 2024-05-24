import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class ZipFileRecordReader extends RecordReader<NullWritable, Text> {
    private FSDataInputStream fileIn;
    private ZipInputStream zipIn;
    private Text currentValue = new Text();
    private boolean isFinished = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        FileSplit fileSplit = (FileSplit) split;
        Path filePath = fileSplit.getPath();
        FileSystem fs = filePath.getFileSystem(context.getConfiguration());
        fileIn = fs.open(filePath);
        zipIn = new ZipInputStream(fileIn);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        ZipEntry entry = zipIn.getNextEntry();
        if (entry == null) {
            isFinished = true;
            return false;
        }
        currentValue.set(entry.getName());
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() {
        return isFinished ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
        zipIn.close();
        fileIn.close();
    }
}
