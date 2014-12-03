using System;
using System.Linq;
using System.Net.Sockets;
using System.Text;

public class StatsDClient : IDisposable
{
  #region Variable Declarations
  private readonly UdpClient _udpClient;
  #endregion

  #region Constructors
  /// <summary>
  /// Prevent default instantiation.
  /// </summary>
  private StatsDClient()
  {
  }

  /// <summary>
  /// Creates an instance with the host and port set.
  /// </summary>
  /// <param name="hostName">The host name.</param>
  /// <param name="udpPort">The UDP port.</param>
  public StatsDClient(string hostName, int udpPort)
  {
    _udpClient = new UdpClient(hostName, udpPort);
  }
  #endregion

  #region IDisposable Members
  public void Dispose()
  {
    try
    {
      if (_udpClient != null)
        _udpClient.Close();
    }
    catch
    {
      //Don't do anything
    }
  }
  #endregion

  #region Public Methods
  /// <summary>
  /// Sends a timing (benchmark) to the StatsD server.
  /// </summary>
  /// <param name="sampleRate">The sample rate.</param>
  /// <param name="timingInMilliseconds">The time in milliseconds.</param>
  /// <param name="statBuckets">The statistic bucket name.</param>
  /// <returns>True if the data was sent, false otherwise.</returns>
  public bool Timing(string statBucket, int timingInMilliseconds, double sampleRate = 1.0)
  {
    return SendStatistics(sampleRate, String.Format("{0}:{1:d}|ms", statBucket, timingInMilliseconds));
  }

  /// <summary>
  /// Sends a counter decrement to the StatsD server.
  /// </summary>
  /// <param name="sampleRate">The sample rate.</param>
  /// <param name="statMagnitude">The magnitude.</param>
  /// <param name="statBuckets">The statistic bucket name.</param>
  /// <returns>True if the data was sent, false otherwise.</returns>
  public bool Decrement(double sampleRate = 1.0, int statMagnitude = -1, params string[] statBuckets)
  {
    statMagnitude = statMagnitude < 0 ? statMagnitude : -statMagnitude;
    return Increment(sampleRate, statMagnitude, statBuckets);
  }

  /// <summary>
  /// Sends a counter increment to the StatsD server.
  /// </summary>
  /// <param name="sampleRate">The sample rate.</param>
  /// <param name="statMagnitude">The magnitude.</param>
  /// <param name="statBuckets">The statistic bucket name.</param>
  /// <returns>True if the data was sent, false otherwise.</returns>
  public bool Increment(double sampleRate = 1.0, int statMagnitude = 1, params string[] statBuckets)
  {
    return SendStatistics(sampleRate, statBuckets.Select(statBucket => String.Format("{0}:{1}|c", statBucket, statMagnitude)).ToArray());
  }
  #endregion
  
  #region Private Methods
  /// <summary>
  /// Sends the statistics to the StatsD server.
  /// </summary>
  /// <param name="sampleRate">The sample rate.</param>
  /// <param name="statistics">The statistics.</param>
  /// <returns>True if the data was sent, false otherwise.</returns>
  private bool SendStatistics(double sampleRate, params string[] statistics)
  {
    var dataWasSent = false;

    if (sampleRate < 1.0)
    {
      var _randomSampling = new Random();

      foreach (var statistic in statistics)
      {
        if (sampleRate >= _randomSampling.NextDouble())
        {
          if (SendUdpPacket(String.Format("{0}|@{1:f}", statistic, sampleRate)))
            dataWasSent = true;
        }
      }
    }
    else
    {
      foreach (var stat in statistics)
      {
        if (SendUdpPacket(stat))
          dataWasSent = true;
      }
    }

    return dataWasSent;
  }

  /// <summary>
  /// Sends 
  /// </summary>
  /// <param name="statistic">The statistic.</param>
  /// <returns>True if the UDP packet was sent, false otherwise.</returns>
  private bool SendUdpPacket(string statistic)
  {
    var byteData = Encoding.Default.GetBytes(statistic);
    _udpClient.Send(byteData, byteData.Length);
    return true;
  }
  #endregion
}