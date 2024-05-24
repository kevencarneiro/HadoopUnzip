import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

public class CombineZipFileRecordReader extends RecordReader<NullWritable, Text> {
    private CombineFileSplit split;
    private int currentFileIndex;
    private Text currentKey = new Text();
    private boolean isDone = false;

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        this.split = (CombineFileSplit) split;
        this.currentFileIndex = -1;
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        if (isDone) {
            return false;
        }
        currentFileIndex++;
        if (currentFileIndex < split.getNumPaths()) {
            currentKey.set(split.getPath(currentFileIndex).toString());
            return true;
        }
        isDone = true;
        return false;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public Text getCurrentValue() {
        return currentKey;
    }

    @Override
    public float getProgress() throws IOException {
        return isDone ? 1.0f : Math.min(1.0f, currentFileIndex / (float) split.getNumPaths());
    }

    @Override
    public void close() {
        // Nothing to close since we're not opening files here
    }
}

