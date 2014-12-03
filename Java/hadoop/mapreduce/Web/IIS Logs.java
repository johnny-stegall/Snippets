package hadoop.mapreduce.web;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import hadoop.mapreduce.web.models.ClientStatistics;
import platform.TextParsing;

/******************************************************************************
* The standard Hadoop pattern is to use nested static classes for the Mapper
* and Reducer inside an encapsulating class.
******************************************************************************/
public class IisLogs
{
  //Constants that represent field/column placement
  private static final int COLUMN_DATE = 0;
  private static final int COLUMN_TIME = 1;
  private static final int COLUMN_SERVER_IP = 2;
  private static final int COLUMN_HTTP_METHOD = 3;
  private static final int COLUMN_URL = 4;
  private static final int COLUMN_QUERYSTRING = 5;
  private static final int COLUMN_PORT = 6;
  private static final int COLUMN_USERNAME = 7;
  private static final int COLUMN_CLIENT_IP = 8;
  private static final int COLUMN_USER_AGENT = 9;
  private static final int COLUMN_REFERER = 10;
  private static final int COLUMN_STATUS = 11;
  private static final int COLUMN_SUBSTATUS = 12;
  private static final int COLUMN_WIN32_STATUS = 13;
  private static final int COLUMN_BYTES = 14;
  private static final int COLUMN_TIME_TAKEN = 15;
  private static final String IIS_DATE = "yyyy-MM-dd";
  private static final String IIS_TIME = " HH:mm:ss";

  /****************************************************************************
  * The Mapper.
  ****************************************************************************/
  public static class Map extends Mapper<LongWritable, Text, ClientStatistics, IntWritable>
  {
    //Object creation/destruction are expensive; instantiate objects here
    private ClientStatistics clientStatistics = new ClientStatistics();
    private static final IntWritable ONE = new IntWritable(1);

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, ClientStatistics, IntWritable>.Context context) throws IOException, InterruptedException
    {
      //Read a line of text and split it into columns by spaces
      String[] dataColumns = value.toString().split(" ");
      
      //Parse the user agent and write it to output
      if (dataColumns.length >= COLUMN_REFERER)
      {
        if (!TextParsing.isNullOrEmpty(dataColumns[COLUMN_USER_AGENT]))
        {
          java.util.Map<String, String> userAgent = TextParsing.parseUserAgent(dataColumns[COLUMN_USER_AGENT]);
          clientStatistics.set(userAgent.get("Browser"),
            TextParsing.tryParseFloat(userAgent.get("Browser Version")),
            userAgent.get("Operating System"),
            TextParsing.tryParseFloat(userAgent.get("OS Version")),
            dataColumns[COLUMN_REFERER]);
        }
        context.write(clientStatistics, ONE);
      }
    }
  }

  /****************************************************************************
  * The Reducer.
  ****************************************************************************/
  public static class Reduce extends Reducer<ClientStatistics, IntWritable, ClientStatistics, IntWritable>
  {
    public void reduce(ClientStatistics key, Iterable<IntWritable> values, Reducer<ClientStatistics, IntWritable, ClientStatistics, IntWritable>.Context context) throws IOException, InterruptedException
    {
      //Enumerate and sum the count
      int userAgentSum = 0;
      for (IntWritable val : values)
        userAgentSum += val.get();

      //Write the reduce results
      context.write(key, new IntWritable(userAgentSum));
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

    //Create the MapReduce job and tell Hadoop about the classes
    Job countUserAgents = new Job(new Configuration());
    countUserAgents.setJobName("Client Statistics");
    countUserAgents.setJarByClass(ClientStatistics.class);
    
    //Tell Hadoop about the Mapper, but no Reducer for this job
    //(Hadoop has an IdentityReducer that will output the Mapper output)
    countUserAgents.setMapperClass(Map.class);
    countUserAgents.setReducerClass(Reduce.class);
    countUserAgents.setNumReduceTasks(countUserAgents.getConfiguration().getInt("mapred.tasktracker.reduce.tasks.maximum", 4));
    
    //Tell Hadoop about the output
    countUserAgents.setOutputKeyClass(Text.class);
    countUserAgents.setOutputValueClass(IntWritable.class);
    countUserAgents.setInputFormatClass(TextInputFormat.class);
    countUserAgents.setOutputFormatClass(TextOutputFormat.class);

    //Set the input/output paths
    FileInputFormat.addInputPath(countUserAgents, new Path(appArguments[0]));
    FileOutputFormat.setOutputPath(countUserAgents, new Path(appArguments[1] + "/" + ClientStatistics.class.getSimpleName()));
    
    countUserAgents.waitForCompletion(true);
  }
}
