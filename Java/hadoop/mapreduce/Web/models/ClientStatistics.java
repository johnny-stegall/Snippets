package hadoop.mapreduce.web.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.WritableComparable;

/******************************************************************************
* This class encapsulates the basic data for a user's browser and the
* referring page.
******************************************************************************/
public class ClientStatistics implements WritableComparable<ClientStatistics>
{
  //Property variable declarations
  private String _Browser;
  private float _BrowserVersion;
  private String _OperatingSystem;
  private float _OsVersion;
  private String _Referer;
  
  //###########################################################################
  // Public Properties
  //###########################################################################
  /****************************************************************************
  * Gets the client's browser.
  ****************************************************************************/
  public String getBrowser()
  {
    return (_Browser != null) ? _Browser : "";
  }
  
  /****************************************************************************
  * Gets the client's browser version.
  ****************************************************************************/
  public float getBrowserVersion()
  {
    return _BrowserVersion;
  }
  
  /****************************************************************************
  * Gets client's operating system.
  ****************************************************************************/
  public String getOperatingSystem()
  {
    return (_OperatingSystem != null) ? _OperatingSystem : "";
  }

  /****************************************************************************
  * Gets the client's OS version.
  ****************************************************************************/
  public float getOsVersion()
  {
    return _OsVersion;
  }
  
  /****************************************************************************
  * Gets the referring page.
  ****************************************************************************/
  public String getReferer()
  {
    return (_Referer != null) ? _Referer : "";
  }
  
  /****************************************************************************
  * Sets the client's browser.
  ****************************************************************************/
  public void setBrowser(String value)
  {
    _Browser = value;
  }

  /****************************************************************************
  * Sets the client's browser version.
  ****************************************************************************/
  public void setBrowserVersion(float value)
  {
    _BrowserVersion = value;
  }
  
  /****************************************************************************
  * Sets the client's operating system.
  ****************************************************************************/
  public void setOperatingSystem(String value)
  {
    _OperatingSystem = value;
  }
  
  /****************************************************************************
  * Sets the client's OS version.
  ****************************************************************************/
  public void setOsVersion(float value)
  {
    _OsVersion = value;
  }
  
  /****************************************************************************
  * Sets the referring page.
  ****************************************************************************/
  public void setReferer(String value)
  {
    _Referer = value;
  }

  //###########################################################################
  // Constructors
  //###########################################################################
  /****************************************************************************
  * Initializes an empty instance of the object.
  ****************************************************************************/
  public ClientStatistics()
  {
  }
  
  /****************************************************************************
  * Initializes a populated instance of the object.
  ****************************************************************************/
  public ClientStatistics(String browserName, float browserVersion, String operatingSystem, float osVersion, String referringPage)
  {
    set(browserName, browserVersion, operatingSystem, osVersion, referringPage);
  }
  
  //###########################################################################
  // WritableComparable Implementation
  //###########################################################################
  @Override
  public void readFields(DataInput input) throws IOException
  {
    _Browser = input.readUTF();
    _BrowserVersion = input.readFloat();
    _OperatingSystem = input.readUTF();
    _OsVersion = input.readFloat();
    _Referer = input.readUTF();
  }

  @Override
  public void write(DataOutput output) throws IOException
  {
    output.writeUTF(this.getBrowser());
    output.writeFloat(_BrowserVersion);
    output.writeUTF(this.getOperatingSystem());
    output.writeFloat(_OsVersion);
    output.writeUTF(this.getReferer());
  }
  
  @Override
  public int compareTo(ClientStatistics targetClientStatistics)
  {
    return _Browser.compareTo(targetClientStatistics.getBrowser())
        + new Float(_BrowserVersion).compareTo(targetClientStatistics.getBrowserVersion())
        + _OperatingSystem.compareTo(targetClientStatistics.getOperatingSystem())
        + new Float(_OsVersion).compareTo(targetClientStatistics.getOsVersion())
        + _Referer.compareTo(targetClientStatistics.getReferer());
  }

  //###########################################################################
  // Overridden Object Methods
  //###########################################################################
  @Override
  public boolean equals(Object equalityTarget)
  {
    if (equalityTarget instanceof ClientStatistics)
    {
      ClientStatistics forEquality = (ClientStatistics)equalityTarget; 
      return _Browser.equals(forEquality.getBrowser())
        && _BrowserVersion == forEquality.getBrowserVersion()
        && _OperatingSystem.equals(forEquality.getOperatingSystem())
        && _OsVersion == forEquality.getOsVersion()
        && _Referer.equals(forEquality.getReferer());
    }
    
    return false;
  }
  
  @Override
  public int hashCode()
  {
    return _Browser.hashCode()
      + new Float(_BrowserVersion).hashCode()
      + _OperatingSystem.hashCode()
      + new Float(_OsVersion).hashCode()
      + _Referer.hashCode();
  }

  @Override
  public String toString()
  {
    return _Browser + "\t"
      + _BrowserVersion + "\t"
      + _OperatingSystem + "\t"
      + _OsVersion + "\t"
      + _Referer;
  }
  
  //###########################################################################
  // Public Methods
  //###########################################################################
  /****************************************************************************
  * Populates the properties with the given values; used by the Mapper
  * so the object can be re-populated instead of instantiated each time.
  ****************************************************************************/
  public void set(String browserName, float browserVersion, String operatingSystem, float osVersion, String referringPage)
  {
    _Browser = browserName;
    _BrowserVersion = browserVersion;
    _OperatingSystem = operatingSystem;
    _OsVersion = osVersion;
    _Referer = referringPage;
  }
}
