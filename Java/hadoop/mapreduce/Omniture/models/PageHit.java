package hadoop.mapreduce.omniture.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.io.WritableComparable;

import platform.TextParsing;

/******************************************************************************
* This class encapsulates the basic data representing a page hit parsed from
* the Omniture click-stream data.
******************************************************************************/
public class PageHit implements WritableComparable<PageHit>
{
  //Property variable declarations
  private String _Browser;
  private Date _HitDate;
  private String _IpAddress;
  private String _PageUrl;
  private String _PageName;
  private int _PageSequence;
  private String _Referer;
  private String _SessionId;
  private String _TrafficSource;
  private int _VisitNumber;
  
  //Constants
  private static final String DATE_FORMAT = "MM/dd/yyyy HH:mm:ss a";
  private static final SimpleDateFormat _customDate = new SimpleDateFormat(DATE_FORMAT);
  
  //###########################################################################
  // Public Properties
  //###########################################################################
  /****************************************************************************
  * Gets the browser of the page hit.
  ****************************************************************************/
  public String getBrowser()
  {
    return (_Browser != null) ? _Browser : "";
  }
  
  /****************************************************************************
  * Gets the hit date.
  ****************************************************************************/
  public Date getHitDate()
  {
    return _HitDate;
  }
  
  /****************************************************************************
  * Gets the IP address of the visitor.
  ****************************************************************************/
  public String getIpAddress()
  {
    return (_IpAddress != null) ? _IpAddress : "";
  }

  /****************************************************************************
  * Gets the page name.
  ****************************************************************************/
  public String getPageName()
  {
    return (_PageName != null) ? _PageName : "";
  }
  
  /****************************************************************************
  * Gets the sequence number of the page relative to a user's visit.
  ****************************************************************************/
  public int getPageSequence()
  {
    return _PageSequence;
  }
  
  /****************************************************************************
  * Gets the page URL.
  ****************************************************************************/
  public String getPageUrl()
  {
    return (_PageUrl != null) ? _PageUrl : "";
  }
  
  /****************************************************************************
  * Gets the referring page.
  ****************************************************************************/
  public String getReferer()
  {
    return (_Referer != null) ? _Referer : "";
  }
  
  /****************************************************************************
  * Gets the visitor's session Id.
  ****************************************************************************/
  public String getSessionId()
  {
    return (_SessionId != null) ? _SessionId : "";
  }
  
  /****************************************************************************
  * Gets the source of the visitor's traffic (direct, SEM, SEO, search engine,
  * referral link).
  ****************************************************************************/
  public String getTrafficSource()
  {
    return (_TrafficSource != null) ? _TrafficSource : "";
  }
  
  /****************************************************************************
  * Gets the number of this visit of a user to the site.
  ****************************************************************************/
  public int getVisitNumber()
  {
    return _VisitNumber;
  }
  
  /****************************************************************************
  * Sets the browser of the page hit.
  ****************************************************************************/
  public void setBrowser(String value)
  {
    _Browser = value;
  }

  /****************************************************************************
  * Sets the hit date.
  ****************************************************************************/
  public void setHitDate(Date value)
  {
    _HitDate = value;
  }
  
  /****************************************************************************
  * Sets the IP address of the visitor.
  ****************************************************************************/
  public void setIpAddress(String value)
  {
    _IpAddress = value;
  }
  
  /****************************************************************************
  * Sets the page name.
  ****************************************************************************/
  public void setPageName(String value)
  {
    _PageName = value;
  }
  
  /****************************************************************************
  * Sets the sequence number of the page relative to a user's visit.
  ****************************************************************************/
  public void setPageSequence(int value)
  {
    _PageSequence = value;
  }
  
  /****************************************************************************
  * Sets the page URL.
  ****************************************************************************/
  public void setPageUrl(String value)
  {
    _PageUrl = value;
  }
  
  /****************************************************************************
  * Sets the referring page.
  ****************************************************************************/
  public void setReferer(String value)
  {
    _Referer = value;
  }

  /****************************************************************************
  * Sets the visitor's session Id.
  ****************************************************************************/
  public void setSessionId(String value)
  {
    _SessionId = value;
  }
  
  /****************************************************************************
  * Gets the source of the visitor's traffic (direct, SEM, SEO, search engine,
  * referral link).
  ****************************************************************************/
  public void getTrafficSource(String value)
  {
    _TrafficSource = value;
  }
  
  /****************************************************************************
  * Gets the number of this visit of a user to the site.
  ****************************************************************************/
  public void getVisitNumber(int value)
  {
    _VisitNumber = value;
  }

  //###########################################################################
  // Constructors
  //###########################################################################
  /****************************************************************************
  * Initializes an empty instance of the object.
  ****************************************************************************/
  public PageHit()
  {
  }
  
  /****************************************************************************
  * Initializes a populated instance of the object.
  ****************************************************************************/
  public PageHit(Date hitDate, String ipAddress, String pageUrl, String referrer, String browser, String pageName, int pageSequence, String sessionId, String trafficSource, int visitNumber)
  {
    set(hitDate, ipAddress, pageUrl, referrer, browser, pageName, pageSequence, sessionId, trafficSource, visitNumber);
  }
  
  //###########################################################################
  // WritableComparable Implementation
  //###########################################################################
  @Override
  public void readFields(DataInput input) throws IOException
  {
    _HitDate = TextParsing.tryParseDate(DATE_FORMAT, input.readUTF());
    _PageUrl = input.readUTF();
    _Referer = input.readUTF();
    _IpAddress = input.readUTF();
    _Browser = input.readUTF();
    _PageName = input.readUTF();
    _PageSequence = input.readInt();
    _SessionId = input.readUTF();
    _TrafficSource = input.readUTF();
    _VisitNumber = input.readInt();
  }

  @Override
  public void write(DataOutput output) throws IOException
  {
    output.writeUTF(_customDate.format(_HitDate));
    output.writeUTF(this.getPageUrl());
    output.writeUTF(this.getReferer());
    output.writeUTF(this.getIpAddress());
    output.writeUTF(this.getBrowser());
    output.writeUTF(this.getPageName());
    output.writeInt(_PageSequence);
    output.writeUTF(this.getSessionId());
    output.writeUTF(this.getTrafficSource());
    output.writeInt(_VisitNumber);
  }
  
  @Override
  public int compareTo(PageHit targetPageHit)
  {
    return _SessionId.compareTo(targetPageHit.getSessionId())
        + new Integer(_PageSequence).compareTo(targetPageHit.getPageSequence());
  }

  //###########################################################################
  // Overridden Object Methods
  //###########################################################################
  @Override
  public boolean equals(Object equalityTarget)
  {
    if (equalityTarget instanceof PageHit)
    {
      PageHit forEquality = (PageHit)equalityTarget; 
      return _HitDate.equals(forEquality.getHitDate())
        && _IpAddress.equals(forEquality.getIpAddress())
        && _PageUrl.equals(forEquality.getPageUrl())
        && _Referer.equals(forEquality.getReferer())
        && _Browser.equals(forEquality.getBrowser())
        && _PageName.equals(forEquality.getPageName())
        && _PageSequence == forEquality.getPageSequence()
        && _SessionId.equals(forEquality.getSessionId())
        && _TrafficSource.equals(forEquality.getTrafficSource())
        && _VisitNumber == forEquality.getVisitNumber();
    }
    
    return false;
  }
  
  @Override
  public int hashCode()
  {
    return new Integer(_VisitNumber).hashCode()
      + _SessionId.hashCode()
      + new Integer(_PageSequence).hashCode();
  }

  @Override
  public String toString()
  {
    return _customDate.format(_HitDate) + "\t"
      + _PageUrl + "\t"
      + _Referer + "\t"
      + _IpAddress + "\t"
      + _Browser + "\t"
      + _PageName + "\t"
      + _PageSequence + "\t"
      + _SessionId + "\t"
      + _TrafficSource + "\t"
      + _VisitNumber;
  }
  
  //###########################################################################
  // Public Methods
  //###########################################################################
  /****************************************************************************
  * Populates the properties with the given values; used by the Mapper
  * so the object can be re-populated instead of instantiated each time.
  ****************************************************************************/
  public void set(Date hitDate, String ipAddress, String pageUrl, String referrer, String browser, String pageName, int pageSequence, String sessionId, String trafficSource, int visitNumber)
  {
    _HitDate = hitDate;
    _IpAddress = ipAddress;
    _PageUrl = pageUrl;
    _Referer = referrer;
    _Browser = browser;
    _PageName = pageName;
    _PageSequence = pageSequence;
    _SessionId = sessionId;
    _TrafficSource = trafficSource;
    _VisitNumber = visitNumber;
  }
}
