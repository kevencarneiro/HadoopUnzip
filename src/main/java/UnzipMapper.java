import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class UnzipMapper extends Mapper<NullWritable, Text, Text, NullWritable> {
    private boolean deleteOriginal;

    @Override
    protected void setup(Context context) throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        deleteOriginal = conf.getBoolean("unzip.deleteOriginal", false);
    }

    @Override
    protected void map(NullWritable key, Text value, Context context) throws IOException, InterruptedException {
        // Output the unzipped content
        context.write(value, NullWritable.get());

        // Get the file path from the context
        String filePath = context.getInputSplit().toString();

        // Delete the original file if the parameter is set
        if (deleteOriginal) {
            FileSystem fs = FileSystem.get(context.getConfiguration());
            fs.delete(new Path(filePath), false);
        }
    }
}
