import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.io.IOUtils;

import java.io.IOException;
import java.io.OutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.io.InputStream;
import java.util.logging.Logger;

public class UnzipMapper extends Mapper<LongWritable, Text, NullWritable, NullWritable> {
    private boolean deleteOriginal;
    private static final Logger LOGGER = Logger.getLogger(UnzipMapper.class.getName());

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        deleteOriginal = conf.getBoolean("unzip.deleteOriginal", false);
    }

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        Path filePath = new Path(value.toString());
        FileSystem fs = FileSystem.get(context.getConfiguration());

        if (!filePath.toString().endsWith(".zip")) {
            LOGGER.warning("Skipped non-zip file: " + filePath);
            return;
        }

        LOGGER.info("Opening zip file: " + filePath);
        processZipFile(fs, filePath, context);

        if (deleteOriginal) {
            boolean isDeleted = fs.delete(filePath, false);
            if (isDeleted) {
                LOGGER.info("Deleted original zip file: " + filePath);
            } else {
                LOGGER.warning("Failed to delete original zip file: " + filePath);
            }
        }
    }

    private void processZipFile(FileSystem fs, Path filePath, Context context) throws IOException {
        try (InputStream is = fs.open(filePath); ZipInputStream zis = new ZipInputStream(is)) {
            ZipEntry entry;
            while ((entry = zis.getNextEntry()) != null) {
                if (!entry.isDirectory()) {
                    LOGGER.info("Processing zip entry: " + entry.getName());

                    Path outputPath = new Path(FileOutputFormat.getOutputPath(context), entry.getName());
                    try (OutputStream os = fs.create(outputPath, true)) {
                        IOUtils.copyBytes(zis, os, 65536, false); // 64 KB buffer
                    }
                }
                zis.closeEntry();
            }
        } catch (IOException e) {
            LOGGER.severe("Error processing zip file: " + filePath + "; Error: " + e.getMessage());
            throw new IOException("Error processing zip file: " + filePath, e);
        }
    }

    @Override
    public void cleanup(Context context) throws IOException, InterruptedException {
        super.cleanup(context);
    }
}
