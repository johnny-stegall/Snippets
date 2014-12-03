package hadoop.mapreduce.omniture;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import hadoop.mapreduce.omniture.models.Visit;

/******************************************************************************
* Converts the binary-serialized sequential files into text files. Useful for
* debugging the parsed raw data.
******************************************************************************/
public class VisitToText
{
  /****************************************************************************
  * The Mapper.
  ****************************************************************************/
  public static class Map extends Mapper<NullWritable, Visit, NullWritable, Visit>
  {
    public void map(NullWritable key, Visit value, Mapper<NullWritable, Visit, NullWritable, Visit>.Context context) throws IOException, InterruptedException
    {
      context.write(NullWritable.get(), value);
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
    
    Job seequentialToText = setupJob(appArguments[0], appArguments[1] + "/" + VisitToText.class.getSimpleName());
    seequentialToText.waitForCompletion(true);
  }

  /****************************************************************************
  * Configures the job to run.
  ****************************************************************************/
  public static Job setupJob(String inputPath, String outputPath) throws Exception
  {
    //Create the MapReduce job and tell Hadoop about the classes
    Job textualVisit = new Job(new Configuration());
    textualVisit.setJobName("Visit To Text");
    textualVisit.setJarByClass(VisitToText.class);
    
    //Tell Hadoop about the Mapper/Reducer
    textualVisit.setMapperClass(VisitToText.Map.class);
    textualVisit.setNumReduceTasks(textualVisit.getConfiguration().getInt("mapred.tasktracker.reduce.tasks.maximum", 4));
    
    //Tell Hadoop about the output
    textualVisit.setOutputKeyClass(NullWritable.class);
    textualVisit.setOutputValueClass(Visit.class);
    textualVisit.setInputFormatClass(SequenceFileInputFormat.class);
    textualVisit.setOutputFormatClass(TextOutputFormat.class);

    //Set the input/output paths
    FileInputFormat.addInputPath(textualVisit, new Path(inputPath));
    FileOutputFormat.setOutputPath(textualVisit, new Path(outputPath));
    
    return textualVisit;
  }
}