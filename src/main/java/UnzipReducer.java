import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnzipReducer extends Reducer<Text, Text, Text, BytesWritable> {
    private boolean deleteOriginal;
    private boolean verbose;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        deleteOriginal = conf.getBoolean("unzip.deleteOriginal", false);
        verbose = conf.getBoolean("unzip.verbose", false);
    }

    @Override
    protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
        Path zipFilePath = new Path(key.toString());
        FileSystem fs = zipFilePath.getFileSystem(context.getConfiguration());
        ZipInputStream zipIn = new ZipInputStream(fs.open(zipFilePath));

        for (Text value : values) {
            ZipEntry entry = zipIn.getNextEntry();
            while (entry != null && !entry.getName().equals(value.toString())) {
                entry = zipIn.getNextEntry();
            }
            if (entry == null) continue;

            if (verbose) {
                System.out.println("Unzipping: " + entry.getName());
            }

            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            byte[] buffer = new byte[4096];
            int len;
            while ((len = zipIn.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
            context.write(new Text(entry.getName()), new BytesWritable(baos.toByteArray()));
            baos.close();
        }
        zipIn.close();

        // Delete the original file if the parameter is set
        if (deleteOriginal) {
            fs.delete(zipFilePath, false);
            if (verbose) {
                System.out.println("Deleted original zip file: " + zipFilePath);
            }
        }
    }
}
