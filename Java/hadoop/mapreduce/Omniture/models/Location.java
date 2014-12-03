package hadoop.mapreduce.omniture.models;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.regex.Matcher;

import org.apache.hadoop.io.WritableComparable;

import platform.TextParsing;

/******************************************************************************
* This class encapsulates all of the data to be parsed from the Omniture
* click-stream data.
******************************************************************************/
public class Location implements WritableComparable<Location>
{
  //Property variable declarations
  private String _County;
  private String _GeoCity;
  private String _GeoState;
  private String _State;
  private String _ZipCode;
  
  //###########################################################################
  // Public Properties
  //###########################################################################
  /****************************************************************************
  * Gets the county.
  ****************************************************************************/
  public String getCounty()
  {
    return (_County != null) ? _County : "";
  }
  
  /****************************************************************************
  * Gets the geolocated city.
  ****************************************************************************/
  public String getGeoCity()
  {
    return (_GeoCity != null) ? _GeoCity : "";
  }
  
  /****************************************************************************
  * Gets the geolocated state.
  ****************************************************************************/
  public String getGeoState()
  {
    return (_GeoState != null) ? _GeoState : "";
  }
  
  /****************************************************************************
  * Gets the visitor's state.
  ****************************************************************************/
  public String getState()
  {
    return (_State != null) ? _State : "";
  }
  
  /****************************************************************************
  * Gets the visitor's ZIP code.
  ****************************************************************************/
  public String getZipCode()
  {
    return (_ZipCode != null) ? _ZipCode : "";
  }
  
  /****************************************************************************
  * Sets the county.
  ****************************************************************************/
  public void setCounty(String value)
  {
    _County = value;
  }
  
  /****************************************************************************
  * Sets the geolocated city.
  ****************************************************************************/
  public void setGeoCity(String value)
  {
    _GeoCity = value;
  }
  
  /****************************************************************************
  * Sets the geolocated state.
  ****************************************************************************/
  public void setGeoState(String value)
  {
    Matcher stateValidation = TextParsing.REGEX_STATE.matcher(value);
    if (stateValidation.matches())
      _GeoState = value;
  }
  
  /****************************************************************************
  * Sets the visitor's state.
  ****************************************************************************/
  public void setState(String value)
  {
    Matcher stateValidation = TextParsing.REGEX_STATE.matcher(value);
    if (stateValidation.matches())
      _State = value;
  }
  
  /****************************************************************************
  * Sets the visitor's ZIP code.
  ****************************************************************************/
  public void setZipCode(String value)
  {
    Matcher zipValidation = TextParsing.REGEX_ZIP_CODE.matcher(value);
    if (zipValidation.matches())
      _ZipCode = value;
  }
  
  //###########################################################################
  // Constructors
  //###########################################################################
  /****************************************************************************
  * Initializes an empty instance of the object.
  ****************************************************************************/
  public Location()
  {
  }
  
  /****************************************************************************
  * Initializes a populated instance of the object.
  ****************************************************************************/
  public Location(String zipCode, String county, String state, String geoCity, String geoState)
  {
    set(zipCode, county, state, geoCity, geoState);
  }

  //###########################################################################
  // WritableComparable Implementation
  //###########################################################################
  @Override
  public void readFields(DataInput input) throws IOException
  {
    _ZipCode = input.readUTF();
    _County = input.readUTF();
    _State = input.readUTF();
    _GeoCity = input.readUTF();
    _GeoState = input.readUTF();
  }

  @Override
  public void write(DataOutput output) throws IOException
  {
    output.writeUTF(this.getZipCode());
    output.writeUTF(this.getCounty());
    output.writeUTF(this.getState());
    output.writeUTF(this.getGeoCity());
    output.writeUTF(this.getGeoState());
  }

  @Override
  public int compareTo(Location targetLocation)
  {
    return _ZipCode.compareTo(targetLocation.getZipCode());
  }
  
  //###########################################################################
  // Overridden Object Methods
  //###########################################################################
  @Override
  public boolean equals(Object equalityTarget)
  {
    if (equalityTarget instanceof Location)
    {
      Location forEquality = (Location)equalityTarget; 
      return _County.equals(forEquality.getCounty())
        && _GeoCity.equals(forEquality.getGeoCity())
        && _GeoState.equals(forEquality.getGeoState())
        && _State.equals(forEquality.getState())
        && _ZipCode.equals(forEquality.getZipCode());
    }
    
    return false;
  }

  @Override
  public int hashCode()
  {
    return _ZipCode.hashCode();
  }
  
  @Override
  public String toString()
  {
    return _ZipCode + "\t"
      + _County + "\t"
      + _State + "\t"
      + _GeoCity + "\t"
      + _GeoState;
  }
  
  //###########################################################################
  // Public Methods
  //###########################################################################
  /****************************************************************************
  * Populates the properties with the given values; used by the Mapper
  * so the object can be re-populated instead of instantiated each time.
  ****************************************************************************/
  public void set(String zipCode, String county, String state, String geoCity, String geoState)
  {
    _ZipCode = zipCode;
    _County = county;
    _State = state;
    _GeoCity = geoCity;
    _GeoState = geoState;
  }
}
