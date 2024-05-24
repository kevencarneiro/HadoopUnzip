import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.InputStream;
import java.io.ByteArrayOutputStream;

public class CombineZipFileRecordReader extends RecordReader<NullWritable, BytesWritable> {
    private CombineFileSplit split;
    private TaskAttemptContext context;
    private int currentFileIndex = 0;
    private ZipInputStream zipIn;
    private BytesWritable currentValue = new BytesWritable();

    public CombineZipFileRecordReader(CombineFileSplit split, TaskAttemptContext context) throws IOException {
        this.split = split;
        this.context = context;
        initialize(split, context);
    }

    @Override
    public void initialize(InputSplit split, TaskAttemptContext context) throws IOException {
        Configuration conf = context.getConfiguration();
        Path filePath = ((CombineFileSplit) split).getPath(currentFileIndex);
        FileSystem fs = filePath.getFileSystem(conf);
        InputStream is = fs.open(filePath);
        zipIn = new ZipInputStream(is);
    }

    @Override
    public boolean nextKeyValue() throws IOException {
        ZipEntry entry;
        while ((entry = zipIn.getNextEntry()) == null) {
            currentFileIndex++;
            if (currentFileIndex >= split.getNumPaths()) {
                return false;
            }
            Path filePath = split.getPath(currentFileIndex);
            FileSystem fs = filePath.getFileSystem(context.getConfiguration());
            InputStream is = fs.open(filePath);
            zipIn = new ZipInputStream(is);
        }
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024 * 1024]; // 1 MB buffer
        int len;
        while ((len = zipIn.read(buffer)) > 0) {
            baos.write(buffer, 0, len);
        }
        currentValue.set(baos.toByteArray(), 0, baos.size());
        return true;
    }

    @Override
    public NullWritable getCurrentKey() {
        return NullWritable.get();
    }

    @Override
    public BytesWritable getCurrentValue() {
        return currentValue;
    }

    @Override
    public float getProgress() throws IOException {
        return (float) currentFileIndex / split.getNumPaths();
    }

    @Override
    public void close() throws IOException {
        if (zipIn != null) {
            zipIn.close();
        }
    }
}
