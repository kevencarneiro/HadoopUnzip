import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;

import java.io.IOException;
import java.util.zip.ZipEntry;
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
        FileSystem fs = FileSystem.get(context.getConfiguration());

        for (Path filePath : paths) {
            if (!filePath.toString().endsWith(".zip")) {
                continue;
            }

            processZipFile(fs, filePath, context);

            // Optionally delete the original file
            if (deleteOriginal) {
                fs.delete(filePath, false);
            }
        }
    }

    private void processZipFile(FileSystem fs, Path filePath, Context context) throws IOException, InterruptedException {
        System.out.println("Opening zip file: " + filePath);

        try (InputStream is = fs.open(filePath); ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    System.out.println("Processing zip entry: " + entry.getName());

                    // Stream the contents of the file directly
                    byte[] buffer = new byte[4096];
                    int bytesRead;
                    while ((bytesRead = zis.read(buffer)) != -1) {
                        BytesWritable writable = new BytesWritable(buffer, bytesRead);
                        context.write(NullWritable.get(), writable);
                    }
                }
                zis.closeEntry();
            }
        } // Automatically close InputStream and ZipInputStream
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        // Perform any necessary cleanup
        super.cleanup(context);
    }
}
