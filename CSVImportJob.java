import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CSVImportJob {

  public static class CSVImportMapper extends Mapper<LongWritable, Text, Text, Text> {

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      // Split the CSV line by commas
      String[] fields = value.toString().split(",");

      // Use the first field as the key and the remaining fields as the value
      context.write(new Text(fields[0]), new Text(String.join(",", Arrays.copyOfRange(fields, 1, fields.length))));
    }
  }

  public static void main(String[] args) throws Exception {
    if (args.length != 2) {
      System.err.println("Usage: CSVImportJob <input path> <output path>");
      System.exit(-1);
    }

    Configuration conf = new Configuration();

    Job job = Job.getInstance(conf, "CSVImportJob");
    job.setJarByClass(CSVImportJob.class);

    // Set the input format class to read CSV files
    job.setInputFormatClass(TextInputFormat.class);

    // Set the mapper class
    job.setMapperClass(CSVImportMapper.class);

    // Set the output key and value classes
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    // Set the output format class to write the key-value pairs as text
    job.setOutputFormatClass(TextOutputFormat.class);

    // Set the input and output paths
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}
