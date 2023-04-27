import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class StudentDataAnalysisJob {

  public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {

    if (args.length != 2) {
      System.err.println("Usage: StudentDataAnalysisJob <input path> <output path>");
      System.exit(-1);
    }

    // Create a new job
    Configuration conf = new Configuration();
    Job job = Job.getInstance(conf, "Student Data Analysis");
    job.setJarByClass(StudentDataAnalysisJob.class);

    // Set the mapper and reducer classes
    job.setMapperClass(StudentDataAnalysisMapper.class);
    job.setReducerClass(StudentDataAnalysisReducer.class);

    // Set the output key and value classes for the reducer
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(StudentDataAnalysisWritable.class);

    // Set the input and output paths
    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    // Wait for the job to complete
    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

//
//You will need to replace StudentDataAnalysisMapper and StudentDataAnalysisReducer
// with the actual names of your mapper and reducer classes.