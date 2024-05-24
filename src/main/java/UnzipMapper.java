import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class UnzipMapper extends Mapper<NullWritable, Text, Text, Text> {
    private boolean verbose;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        verbose = conf.getBoolean("unzip.verbose", false);
    }

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the file path from the context
        FileSplit fileSplit = (FileSplit) context.getInputSplit();
        Path zipFilePath = fileSplit.getPath();

        FileSystem fs = zipFilePath.getFileSystem(context.getConfiguration());
        ZipInputStream zipIn = new ZipInputStream(fs.open(zipFilePath));

        ZipEntry entry;
        while ((entry = zipIn.getNextEntry()) != null) {
            String fileName = entry.getName();
            if (verbose) {
                System.out.println("Listing: " + fileName);
            }
            context.write(new Text(zipFilePath.toString()), new Text(fileName));
        }
        zipIn.close();
    }
}
