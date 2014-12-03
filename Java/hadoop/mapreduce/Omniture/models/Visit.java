package hadoop.mapreduce.omniture.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Date;

/******************************************************************************
* This class encapsulates all of the data to be parsed from the Omniture
* click-stream data.
******************************************************************************/
public class Visit extends PageHit
{
  //Property variable declarations
  private String _Intent;
  private String _Section;
  private Location _Location = new Location();
  
  //###########################################################################
  // Public Properties
  //###########################################################################
  /****************************************************************************
  * Gets the visitor's location.
  ****************************************************************************/
  public Location getLocation()
  {
    return _Location;
  }
  
  /****************************************************************************
  * Gets the intent of the visitor.
  ****************************************************************************/
  public String getIntent()
  {
    return (_Intent != null) ? _Intent : "";
  }
  
  /****************************************************************************
  * Gets the section of the web site.
  ****************************************************************************/
  public String getSection()
  {
    return (_Section != null) ? _Section : "";
  }
  
  /****************************************************************************
  * Sets the intent of the visitor.
  ****************************************************************************/
  public void setIntent(String value)
  {
    _Intent = value;
  }
  
/****************************************************************************
  * Sets the visitor's location.
  ****************************************************************************/
  public void setLocation(Location value)
  {
    _Location = value;
  }
  
  /****************************************************************************
  * Sets the section of the web site.
  ****************************************************************************/
  public void setSection(String value)
  {
    _Section = value;
  }
  
  //###########################################################################
  // Constructors
  //###########################################################################
  /****************************************************************************
  * Initializes an empty instance of the object.
  ****************************************************************************/
  public Visit()
  {
  }
  
  /****************************************************************************
  * Initializes a populated instance of the object.
  ****************************************************************************/
  public Visit(Date hitDate, String ipAddress, String pageUrl, String referrer, String browser, String pageName, int pageSequence, String sessionId, String trafficSource, int visitNumber, String siteSection, String visitorIntent, Location visitorLocation)
  {
    set(hitDate, ipAddress, pageUrl, referrer, browser, pageName, pageSequence, sessionId, trafficSource, visitNumber, siteSection, visitorIntent, visitorLocation);
  }
  
  //###########################################################################
  // Overridden PageView Methods
  //###########################################################################
  @Override
  public void readFields(DataInput input) throws IOException
  {
    super.readFields(input);
    
    _Intent = input.readUTF();
    _Section = input.readUTF();
    
    _Location = new Location();
    _Location.readFields(input);
  }

  @Override
  public void write(DataOutput output) throws IOException
  {
    super.write(output);
    
    output.writeUTF(this.getIntent());
    output.writeUTF(this.getSection());
    _Location.write(output);
  }
  
  @Override
  public boolean equals(Object equalityTarget)
  {
    if (equalityTarget instanceof Visit)
    {
      Visit forEquality = (Visit)equalityTarget; 
      return super.equals(forEquality)
          && _Intent.equals(forEquality.getIntent())
          && _Section.equals(forEquality.getSection())
          && _Location.equals(forEquality.getLocation()));
    }
    
    return false;
  }

  @Override
  public String toString()
  {
    return super.toString() + "\t"
      + _Intent + "\t"
      + _Section + "\t"
      + _Location.toString() + "\t";
  }
  
  //###########################################################################
  // Public Methods
  //###########################################################################
  /****************************************************************************
  * Populates the properties with the given values; used by the Mapper
  * so the object can be re-populated instead of instantiated each time.
  ****************************************************************************/
  public void set(Date hitDate, String ipAddress, String pageUrl, String referrer, String browser, String pageName, int pageSequence, String sessionId, String trafficSource, int visitNumber, String siteSection, String visitorIntent, Location visitorLocation)
  {
    super.set(hitDate, ipAddress, pageUrl, referrer, browser, pageName, pageSequence, sessionId, trafficSource, visitNumber);

    _Intent = visitorIntent;
    _Location = visitorLocation;
  }
}
