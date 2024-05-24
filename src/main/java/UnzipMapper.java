import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.zip.ZipInputStream;
import java.io.InputStream;

public class UnzipMapper extends Mapper<NullWritable, BytesWritable, NullWritable, BytesWritable> {
    private boolean deleteOriginal;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        deleteOriginal = conf.getBoolean("unzip.deleteOriginal", false);
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context) throws IOException, InterruptedException {
        // Get the file path from the context
        CombineFileSplit split = (CombineFileSplit) context.getInputSplit();
        Path[] paths = split.getPaths();
        System.out.println("Processing " + paths.length + " zip files");

        for (Path filePath : paths) {
            // Process only if the file ends with .zip
            if (!filePath.toString().endsWith(".zip")) {
                continue;
            }

            System.out.println("Processing zip file: " + filePath);

            // Read the zip file content from HDFS
            FileSystem fs = FileSystem.get(context.getConfiguration());
            InputStream is = fs.open(filePath);
            ZipInputStream zis = new ZipInputStream(is);
            for (var entry = zis.getNextEntry(); entry != null; entry = zis.getNextEntry()) {
                if (!entry.isDirectory()) {
                    context.write(NullWritable.get(), new BytesWritable(zis.readAllBytes()));
                }
            }
            zis.close();

            // Delete the original file if the parameter is set
            if (deleteOriginal) {
                fs.delete(filePath, false);
                System.out.println("Deleted original zip file: " + filePath);
            }
        }
    }
}
