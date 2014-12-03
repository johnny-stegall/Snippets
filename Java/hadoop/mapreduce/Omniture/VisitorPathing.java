package hadoop.mapreduce.omniture;

import java.io.IOException;
import java.util.ArrayList;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import hadoop.mapreduce.omniture.models.Visit;

/******************************************************************************
* Takes the parsed raw Omniture click-stream data and sorts the data by
* Session ID and Page Sequence. This is an intermediary job used by other
* jobs.
******************************************************************************/
public class VisitorPathing
{
  /****************************************************************************
  * The Mapper.
  ****************************************************************************/
  public static class Map extends Mapper<NullWritable, Visit, Visit, NullWritable>
  {
    public void map(NullWritable key, Visit value, Mapper<NullWritable, Visit, Visit, NullWritable>.Context context) throws IOException, InterruptedException
    {
      context.write(value, NullWritable.get());
    }
  }

  /****************************************************************************
  * The Hadoop entry point.
  ****************************************************************************/
  public static void main(String[] args) throws Exception
  {
    GenericOptionsParser hadoopOptions = new GenericOptionsParser(args);
    String[] appArguments = hadoopOptions.getRemainingArgs();
    
    if (appArguments.length < 2)
    {
      System.err.println("Input and Output paths are required.");
      return;
    }
    
    Job visitorPathing = setupJob(appArguments[0], appArguments[1] + "/" + VisitorPathing.class.getSimpleName());
    visitorPathing.waitForCompletion(true);
  }

  /****************************************************************************
  * Configures the job to run.
  ****************************************************************************/
  public static Job setupJob(String inputPath, String outputPath) throws Exception
  {
    //Create the MapReduce job and tell Hadoop about the classes
    Job visitorPathing = new Job(new Configuration());
    visitorPathing.setJobName("Prepare Visitor Pathing");
    visitorPathing.setJarByClass(VisitorPathing.class);
    
    //Tell Hadoop about the Mapper/Reducer
    visitorPathing.setMapperClass(VisitorPathing.Map.class);
    visitorPathing.setNumReduceTasks(visitorPathing.getConfiguration().getInt("mapred.tasktracker.reduce.tasks.maximum", 4));
    
    //Tell Hadoop about the output
    visitorPathing.setOutputKeyClass(Visit.class);
    visitorPathing.setOutputValueClass(NullWritable.class);
    visitorPathing.setInputFormatClass(SequenceFileInputFormat.class);
    visitorPathing.setOutputFormatClass(TextOutputFormat.class);

    //Set the input/output paths
    FileInputFormat.addInputPath(visitorPathing, new Path(inputPath));
    FileOutputFormat.setOutputPath(visitorPathing, new Path(outputPath));
    
    return visitorPathing;
  }
  
  /****************************************************************************
  * Wraps the Job in a ControlledJob, specifying dependent jobs.
  ****************************************************************************/
  public static ControlledJob chainJob(ArrayList<ControlledJob> dependentJobs, String inputPath, String outputPath) throws Exception
  {
    return new ControlledJob(setupJob(inputPath, outputPath), dependentJobs);
  }
}