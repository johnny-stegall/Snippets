package hadoop.platform;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class TextParsing
{
  //Constants for frequently used Regular Expression patterns
  public static final Pattern REGEX_IP_V4 = Pattern.compile("^\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.\\d{1,3}\\.$");
  public static final Pattern REGEX_IP_V6 = Pattern.compile("^((([0-9a-f]{1,4}:){7}([0-9a-f]{1,4}|:))|(([0-9a-f]{1,4}:){6}(:[0-9a-f]{1,4}|((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9a-f]{1,4}:){5}((:[0-9a-f]{1,4}){1,2}|:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:))|(([0-9a-f]{1,4}:){4}((:[0-9a-f]{1,4}){1,3})|((:[0-9a-f]{1,4})?:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)|(([0-9a-f]{1,4}:){3}|(((:[0-9a-f]{1,4}){0,2}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|(([0-9a-f]{1,4}:){2}((:[0-9a-f]{1,4}){1,5})|((:[0-9a-f]{1,4}){0,3}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:)|(([0-9a-f]{1,4}:){1}(((:[0-9a-f]{1,4}){1,6})|((:[0-9a-f]{1,4}){0,4}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3}))|:))|((:[0-9a-f]{1,4}){1,7}|((:[0-9a-f]{1,4}){0,5}:((25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)(\\.(25[0-5]|2[0-4]\\d|1\\d\\d|[1-9]?\\d)){3})|:)))$", Pattern.CASE_INSENSITIVE);
  public static final Pattern REGEX_STATE = Pattern.compile("^[A-Z]{2}$", Pattern.CASE_INSENSITIVE);
  public static final Pattern REGEX_USER_AGENT = Pattern.compile("((Opera)/(\\d+)).*(Android|iPad|iPhone|Linux|Windows+NT|Windows) \\d+.*((Chrome|Firefox|Opera|Safari)/\\d+|MSIE \\d+)", Pattern.CASE_INSENSITIVE);
  public static final Pattern REGEX_ZIP_CODE = Pattern.compile("\\d{1,5}$");
  
  //An instance of Log4J
  private static final Log LOG = LogFactory.getLog(TextParsing.class);

  /****************************************************************************
  * Private constructor to emulate a static class.
  ****************************************************************************/
  private TextParsing()
  {
  }
  
  /****************************************************************************
  * Tests if a string is null or empty; returns true if not, false otherwise.
  ****************************************************************************/
  public static boolean isNullOrEmpty(String text)
  {
    if (text.equals(null))
      return true;
    else if (text.length() < 1)
      return true;
    else
      return false;
  }
  
  /****************************************************************************
  * Loops through an array and joins the elements together in a string
  * delimited by the specified string-delimiter.
  ****************************************************************************/
  public static String join(Object[] array, String delimiter)
  {
    return join(Arrays.asList(array), delimiter);
  }
  
  /****************************************************************************
  * Loops through a collection and joins the elements together in a string
  * delimited by the specified string-delimiter.
  ****************************************************************************/
  public static String join(Collection<?> iterable, String delimiter)
  {
    if (delimiter == null || delimiter.length() < 1)
      delimiter = ", ";
    
    StringBuilder concatenator = new StringBuilder();
    Iterator<?> loop = iterable.iterator();
    while (loop.hasNext())
    {
      concatenator.append(loop.next());
      
      if (loop.hasNext())
        concatenator.append(delimiter);
    }

    return concatenator.toString();
  }
  
  /****************************************************************************
  * Parses the browser and operating system information from the user agent.
  ****************************************************************************/
  public static Map<String, String> parseUserAgent(String userAgent)
  {
    //Replace + signs with spaces
    userAgent = userAgent.replace('+', ' ');
    
    Map<String, String> userAgentData = new HashMap<String, String>();
    Matcher browserInfo = REGEX_USER_AGENT.matcher(userAgent); 
    if (browserInfo.find())
    {
      //Opera 9+ rides the short bus and needs special care
      if (!isNullOrEmpty(browserInfo.group(0)))
      {
        userAgentData.put("Browser", browserInfo.group(1));
        userAgentData.put("Browser Version", browserInfo.group(2));
      }
      
      //Parse the operating system
      if (!isNullOrEmpty(browserInfo.group(3)))
        userAgentData.put("Operating System", browserInfo.group(3));
      
      //Parse the operating system version
      if (!isNullOrEmpty(browserInfo.group(4)))
        userAgentData.put("OS Version", browserInfo.group(4));
      
      //Parse browser and version
      if (!isNullOrEmpty(browserInfo.group(5)))
      {
        userAgentData.put("Browser", browserInfo.group(6));
        userAgentData.put("Browser Version", browserInfo.group(7));
      }
    }
    
    return userAgentData;
  }
  
  /****************************************************************************
  * Parses the string argument as a date using the specified format pattern.
  * Returns null if a date can't be parsed from the string.
  ****************************************************************************/
  public static Date tryParseDate(String formatPattern, String text)
  {
    if (formatPattern == null || formatPattern.length() < 1)
      return null;
    else if (text == null || text.length() < 1)
      return null;
    
    try
    {
      return new SimpleDateFormat(formatPattern).parse(text);
    }
    catch (ParseException pe)
    {
      LOG.warn("Failed to parse date (" + formatPattern + ") from: " + text, pe);
      return null;
    }
  }

  /****************************************************************************
  * Parses the string argument as a double-precision floating point number.
  * Returns null if the string contains non-numeric characters.
  ****************************************************************************/
  public static Double tryParseDouble(String text)
  {
    if (text == null || text.length() < 1)
      return null;
    
    try
    {
      return Double.parseDouble(text);
    }
    catch (NumberFormatException nfe)
    {
      LOG.warn("Failed to parse double from: " + text, nfe);
      return null;
    }
  }

  /****************************************************************************
  * Parses the string argument as a floating point number. Returns null if the
  * string contains non-numeric characters.
  ****************************************************************************/
  public static Float tryParseFloat(String text)
  {
    if (text == null || text.length() < 1)
      return null;
    
    try
    {
      return Float.parseFloat(text);
    }
    catch (NumberFormatException nfe)
    {
      LOG.warn("Failed to parse float from: " + text, nfe);
      return null;
    }
  }

  /****************************************************************************
  * Parses the string argument as an integer. Returns null if the string
  * contains non-numeric characters.
  ****************************************************************************/
  public static Integer tryParseInt(String text)
  {
    if (text == null || text.length() < 1)
      return null;
    
    try
    {
      return Integer.parseInt(text);
    }
    catch (NumberFormatException nfe)
    {
      LOG.warn("Failed to parse int from: " + text, nfe);
      return null;
    }
  }
}