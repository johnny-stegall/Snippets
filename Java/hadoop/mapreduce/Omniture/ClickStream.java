package hadoop.mapreduce.omniture;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

//import com.hadoop.mapreduce.LzoTextInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.jobcontrol.ControlledJob;
import org.apache.hadoop.mapreduce.lib.jobcontrol.JobControl;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import hadoop.mapreduce.omniture.models.*;
import platform.TextParsing;

/******************************************************************************
* Parses the Omniture click-stream data and extracts information from columns
* with useful data so less traffic is written to disk and sent over the
* network. This is an intermediary job used by other jobs.
******************************************************************************/
public class ClickStream
{
  //Constants that represent field/column placement
  private static final int COLUMN_HIT_DATE = 3;
  private static final int COLUMN_IP_ADDRESS = 8;
  private static final int COLUMN_PAGE_URL = 13;
  private static final int COLUMN_PAGE_NAME = 14;
  private static final int COLUMN_SECTION = 18;
  private static final int COLUMN_CATEGORY = 20;
  private static final int COLUMN_YEAR = 21;
  private static final int COLUMN_ZIP_CODE = 29;
  private static final int COLUMN_STATE = 30;
  private static final int COLUMN_COUNTY = 31;
  private static final int COLUMN_TRAFFIC_SOURCE = 38;
  private static final int COLUMN_SESSION_ID = 58;
  private static final int COLUMN_REFERER = 69;
  private static final int COLUMN_USER_AGENT = 71;
  private static final int COLUMN_VISIT_NUMBER = 108;
  private static final int COLUMN_PAGE_SEQUENCE = 109;
  private static final int COLUMN_GEOCITY = 111;
  private static final int COLUMN_GEOSTATE = 113;
  private static final String OMNITURE_DATE = "yyyy-MM-dd HH:mm:ss";
  
  //An instance of Log4J, alternatively logging can be done to the console
  //using System.err.println() and System.out.println() for logging output
  //Logs can be viewed in $HADOOP_LOG_DIR/userlogs or http://<jobtracker>:50030
  private static final Log LOG = LogFactory.getLog(ClickStream.class);

  /****************************************************************************
  * The Mapper.
  ****************************************************************************/
  public static class Map extends Mapper<LongWritable, Text, NullWritable, Visit>
  {
    //Object creation/destruction are expensive; instantiate objects here
    private String visitorIntent = "";
    private Visit visitData = new Visit();
    private Location visitorLocation = new Location();

    public void map(LongWritable key, Text value, Mapper<LongWritable, Text, NullWritable, Visit>.Context context) throws IOException, InterruptedException
    {
      //Read the line of text and split it into columns by tabs
      String[] dataColumns = value.toString().split("\t");

      try
      {
        //Make sure the page URL is forced lower-case
        String pageUrl = dataColumns[COLUMN_PAGE_URL].toLowerCase();
        
        //Validate required fields
        if (hasRequiredFields(dataColumns))
        {
          //Populate the objects and write the output
          parseVisitorLocation(dataColumns);
          parsePageHit(dataColumns, pageUrl);

          context.write(NullWritable.get(), visitData);
        }
        else
          context.progress();
      }
      catch (ArrayIndexOutOfBoundsException aiobe)
      {
        //Incomplete data, just report progress
        LOG.warn("Incomplete record in Omniture data: " + value.toString(), aiobe);
        context.progress();
      }
    }

    /**************************************************************************
    * Ensures required fields are populated with valid data.
    ***************************************************************************/
    private boolean hasRequiredFields(String[] dataColumns)
    {
      Date hitDate = TextParsing.tryParseDate(OMNITURE_DATE, dataColumns[COLUMN_HIT_DATE]);
      if (hitDate == null)
        return false;
      else if (TextParsing.isNullOrEmpty(dataColumns[COLUMN_IP_ADDRESS]))
        return false;
      else if (TextParsing.isNullOrEmpty(dataColumns[COLUMN_PAGE_URL]))
        return false;
      else if (TextParsing.isNullOrEmpty(dataColumns[COLUMN_USER_AGENT]))
        return false;
      
      return true;
    }
   
    /**************************************************************************
    * Parses the visitor's location.
    **************************************************************************/
    private void parseVisitorLocation(String[] dataColumns)
    {
      if (dataColumns.length >= COLUMN_GEOSTATE)
      {
        visitorLocation.set(dataColumns[COLUMN_ZIP_CODE],
          dataColumns[COLUMN_COUNTY],
          dataColumns[COLUMN_STATE],
          dataColumns[COLUMN_GEOCITY],
          dataColumns[COLUMN_GEOSTATE]);
      }
      else
        visitorLocation.set("", "", "", "", "");      
    }

    /**************************************************************************
    * Parses basic information about a page hit.
    **************************************************************************/
    private void parsePageHit(String[] dataColumns, String pageUrl)
    {
      //Get the visitor's intent
      int intentIndex = pageUrl.indexOf("intent="); 
      if (intentIndex > -1)
      {
        int nextParameterIndex = pageUrl.indexOf('&', intentIndex);
        if (nextParameterIndex == -1)
          visitorIntent = pageUrl.substring(intentIndex + 7);
        else
          visitorIntent = pageUrl.substring(intentIndex + 7, nextParameterIndex);
      }

      java.util.Map<String, String> browserInfo = TextParsing.parseUserAgent(dataColumns[COLUMN_USER_AGENT]);
      
      visitData.set(TextParsing.tryParseDate(OMNITURE_DATE, dataColumns[COLUMN_HIT_DATE]),
        dataColumns[COLUMN_IP_ADDRESS],
        dataColumns[COLUMN_PAGE_URL],
        dataColumns[COLUMN_REFERER],
        browserInfo.get("Browser") + " " + browserInfo.get("Browser Version"),
        dataColumns[COLUMN_PAGE_NAME],
        TextParsing.tryParseInt(dataColumns[COLUMN_PAGE_SEQUENCE]),
        dataColumns[COLUMN_SESSION_ID],
        dataColumns[COLUMN_TRAFFIC_SOURCE],
        TextParsing.tryParseInt(dataColumns[COLUMN_VISIT_NUMBER]),
        dataColumns[COLUMN_SECTION],
        visitorIntent,
        visitorLocation);
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

    ControlledJob parseClickStream = setupParsingJob(appArguments[0], appArguments[1] + "/" + ClickStream.class.getSimpleName());

    //Set the above job up as a dependency
    ArrayList<ControlledJob> dependentJobs = new ArrayList<ControlledJob>();
    dependentJobs.add(parseClickStream);
    
    //Run subsequent jobs
    ControlledJob leadReferralsJob = LeadReferrals.chainJob(dependentJobs, appArguments[1] + "/" + ClickStream.class.getSimpleName(), appArguments[1] + "/" + LeadReferrals.class.getSimpleName());
    ControlledJob visitorPathingJob = VisitorPathing.chainJob(dependentJobs, appArguments[1] + "/" + ClickStream.class.getSimpleName(), appArguments[1] + "/" + VisitorPathing.class.getSimpleName());

    //Setup the Omniture jobs and their dependencies    
    JobControl omnitureJobs = new JobControl("Omniture Click-Stream");
    omnitureJobs.addJob(parseClickStream);
    omnitureJobs.addJob(leadReferralsJob);
    omnitureJobs.addJob(visitorPathingJob);
    
    //Start the jobs on a separate background thread
    Thread jobThread = new Thread(omnitureJobs);
    jobThread.start();
    waitForCompletion(omnitureJobs, Arrays.asList(appArguments).indexOf("-status") > -1);
    
    //Clean up after the Click Stream intermediary files
    if (Arrays.asList(appArguments).indexOf("-nocleanup") < 0)
    {
      DistributedFileSystem hdfs = new DistributedFileSystem();
      hdfs.delete(new Path(appArguments[1] + "/" + ClickStream.class.getSimpleName()), true);
    }
    
    System.exit(0);
  }
  
  /****************************************************************************
  * Poll all jobs every minute and reports status.
  ****************************************************************************/
  private static void waitForCompletion(JobControl omnitureJobs, boolean reportStatus) throws Exception
  {
    List<String> jobNames = new ArrayList<String>();
    while (!omnitureJobs.allFinished())
    {
      if (reportStatus)
      {
        //Output the timestamp of the status report
        System.out.println();
        System.out.println("Omniture Map/Reduce Status Update: " + new java.util.Date().toString());
        
        //List the completed jobs
        jobNames.clear();
        for (ControlledJob successfulJob : omnitureJobs.getSuccessfulJobList())
          jobNames.add(successfulJob.getJobName());
  
        System.out.println("Successful Jobs: " + TextParsing.join(jobNames, ", "));
        
        //List failed jobs
        jobNames.clear();
        for (ControlledJob successfulJob : omnitureJobs.getFailedJobList())
          jobNames.add(successfulJob.getJobName());
  
        System.out.println("Failed Jobs: " + TextParsing.join(jobNames, ", "));
        
        //List waiting jobs
        jobNames.clear();
        for (ControlledJob successfulJob : omnitureJobs.getWaitingJobList())
          jobNames.add(successfulJob.getJobName());
  
        System.out.println("Waiting Jobs: " + TextParsing.join(jobNames, ", "));
        
        //List running jobs and their map/reduce progress
        System.out.println("Running Jobs:");
        for (ControlledJob runningJob : omnitureJobs.getRunningJobList())
        {
          System.out.println("\t" + runningJob.getJobName()
            + ": mapper " + Math.round(runningJob.getJob().mapProgress() * 100)
            + "%\t reducer " + Math.round(runningJob.getJob().reduceProgress() * 100) + "%");
        }
      }

      Thread.sleep(60000);
    }
  }
  
  /****************************************************************************
  * Runs the job that processes Omniture click-stream data.
  ****************************************************************************/
  private static ControlledJob setupParsingJob(String inputPath, String outputPath) throws Exception
  {
    //Create the MapReduce job and tell Hadoop about the classes
    Job parseClickStream = new Job(new Configuration());
    parseClickStream.setJobName("Parse Omniture Click-Stream Data");
    parseClickStream.setJarByClass(ClickStream.class);
    
    //Tell Hadoop about the Mapper, but no Reducer for this job
    //(Hadoop has an IdentityReducer that will output the Mapper output)
    parseClickStream.setMapperClass(Map.class);
    parseClickStream.setNumReduceTasks(parseClickStream.getConfiguration().getInt("mapred.tasktracker.reduce.tasks.maximum", 4));
    
    //Tell Hadoop about the output
    parseClickStream.setOutputKeyClass(NullWritable.class);
    parseClickStream.setOutputValueClass(Visit.class);
    //parseClickStream.setInputFormatClass(LzoTextInputFormat.class);
    parseClickStream.setInputFormatClass(TextInputFormat.class);
    parseClickStream.setOutputFormatClass(SequenceFileOutputFormat.class);

    //Set the input/output paths
    FileInputFormat.addInputPath(parseClickStream, new Path(inputPath));
    FileOutputFormat.setOutputPath(parseClickStream, new Path(outputPath));
    
    return new ControlledJob(parseClickStream, null);
  }
}