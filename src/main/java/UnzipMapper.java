import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.CombineFileSplit;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.InputStream;
import java.util.logging.Logger;

public class UnzipMapper extends Mapper<NullWritable, Text, NullWritable, BytesWritable> {
    private boolean deleteOriginal;
    private static final Logger LOGGER = Logger.getLogger(UnzipMapper.class.getName());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        deleteOriginal = conf.getBoolean("unzip.deleteOriginal", false);
    }

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Get the file path from the context
        CombineFileSplit split = (CombineFileSplit) context.getInputSplit();
        Path[] paths = split.getPaths();
        FileSystem fs = FileSystem.get(context.getConfiguration());

        LOGGER.info("Current split contains " + paths.length + " files.");

        for (Path filePath : new Path[]{new Path(value.toString())}) {  // Process only the current file path in value
            if (!filePath.toString().endsWith(".zip")) {
                LOGGER.warning("Skipped non-zip file: " + filePath);
                continue;
            }

            LOGGER.info("Opening zip file: " + filePath);
            processZipFile(fs, filePath, context);

            // Optionally delete the original file
            if (deleteOriginal) {
                fs.delete(filePath, false);
                LOGGER.info("Deleted original zip file: " + filePath);
            }
        }
    }

    private void processZipFile(FileSystem fs, Path filePath, Context context) throws IOException {
        try (InputStream is = fs.open(filePath); ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    LOGGER.info("Processing zip entry: " + entry.getName());

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
        } catch (Exception e) {
            LOGGER.severe("Error processing zip file: " + filePath + "; Error: " + e.getMessage());
            throw new IOException("Error processing zip file: " + filePath, e);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
