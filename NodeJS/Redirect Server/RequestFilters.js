/******************************************************************************
* Filtering applied to requests. If a request is filtered it is not
* redirected to the destination URL.
******************************************************************************/
var RequestFilters = function(statsdClient, mongoDb)
{
  var MAX_CLICKS = 3;
  var DOUBLE_CLICK_DIFFERENCE = 2000;

  var dailyRequests = {};
  var requestFilters = {};
  loadBannedUserAgents();
  loadBannedIpAddresses();

  /****************************************************************************
  * Gets filter overrides for an affiliate.
  ****************************************************************************/
  function getAffiliate(affiliateId)
  {
    var affiliateOverrides = null;

    mongoDb.collection("AffiliateOverrides").findOne({ "_id": new ObjectID(affiliateId) },
    function(exception, affiliateSettings)
    {
      if (exception)
      {
        statsdClient.increment("RedirectServer.Requests.QueryFailed");
        RedirectServer.writeEntry("Error", httpRequest, "Failed to query Mongo for affiliate overrides. Reason: " + exception.err);
      }
      else if (!affiliateSettings)
      {
        statsdClient.increment("RedirectServer.Requests.NoAffiliateOverrides");
        RedirectServer.writeEntry("Warning", httpRequest, "No filters found for affiliate " + affiliateId + ".");
      }
      else
      {
        statsdClient.increment("RedirectServer.Requests.AffiliateRedirect");
        affiliateOverrides = affiliateSettings;
      }
    });

    return affiliateOverrides;
  }

  /****************************************************************************
  * Determines if an affiliate has exceeded their clicks per day.
  ****************************************************************************/
  function hasExceededClicksPerDay(affiliateOverrides)
  {
    if (!affiliateOverrides)
    {
      if (dailyRequests[httpRequest.connection.remoteAddress].length > MAX_CLICKS)
        return true;
    }
    else if (dailyRequests[httpRequest.connection.remoteAddress].length > affiliateOverrides.MaxClicksPerDay)
      return true;

    return false;
  }

  /****************************************************************************
  * Determines if a click is a result of a user double-clicking.
  ****************************************************************************/
  function isDoubleClick()
  {
    if (dailyRequests[httpRequest.connection.remoteAddress].length > 1)
    {
      var previousClick = dailyRequests[httpRequest.connection.remoteAddress][dailyRequests[httpRequest.connection.remoteAddress].length - 2];
      var currentClick = dailyRequests[httpRequest.connection.remoteAddress][dailyRequests[httpRequest.connection.remoteAddress].length - 1];

      var differenceInMs = Math.abs(currentClick - previousClick);
      if (differenceInMs < DOUBLE_CLICK_DIFFERENCE)
        return true;
    }

    return false;
  }

  /****************************************************************************
  * Determines if an IP address matches any blacklisted IP address or block of
  * IP addresses.
  ****************************************************************************/
  function isIpAddressBanned(ipAddress)
  {
    for (var ipIndex = 0; ipIndex < requestFilters.IpAddresses.length; ipIndex++)
    {
      if (requestFilters.IpAddresses[ipIndex].test(ipAddress))
        return true;
    }

    return false;
  }

  /****************************************************************************
  * Determines if a referring domain is blacklisted.
  ****************************************************************************/
  function isReferrerBanned()
  {
    return false;
  }

  /****************************************************************************
  * Determines if a user agent is blacklisted.
  ****************************************************************************/
  function isUserAgentBanned(userAgent)
  {
    for (var agentIndex = 0; agentIndex < requestFilters.UserAgents.length; agentIndex++)
    {
      if (requestFilters.UserAgents[agentIndex].test(userAgent))
        return true;
    }

    return false;
  }

  /****************************************************************************
  * Loads banned user agents.
  ****************************************************************************/
  function loadBannedUserAgents()
  {
    mongoDb.collection("BannedUserAgents").find({}, { "User Agent": true }).toArray(
    function(exception, blacklist)
    {
      if (exception)
      {
        statsdClient.increment("RedirectServer.Requests.QueryFailed");
        RedirectServer.writeEntry("Error", httpRequest, "Failed to query Mongo for banned user agents. Reason: " + exception.err);
      }
      else if (!blacklist)
        RedirectServer.writeEntry("Critical", httpRequest, "No blacklisted user agents were found in Mongo.");
      else
      {
        requestFilters.UserAgents = blacklist.map(
        function(userAgents)
        {
          return RegExp(userAgents["User Agent"]);
        });
      }
    });
  }

  /****************************************************************************
  * Loads banned IP addresses.
  ****************************************************************************/
  function loadBannedIpAddresses()
  {
    mongoDb.collection("BannedIps").find({}, { "IP Address": true }).toArray(
    function(exception, blacklist)
    {
      if (exception)
      {
        statsdClient.increment("RedirectServer.Requests.QueryFailed");
        RedirectServer.writeEntry("Error", httpRequest, "Failed to query Mongo for banned IP addresses. Reason: " + exception.err);
      }
      else if (!blacklist)
        RedirectServer.writeEntry("Critical", httpRequest, "No blacklisted IP addresses were found in Mongo.");
      else
      {
        requestFilters.IpAddresses = blacklist.map(
        function(userAgents)
        {
          return RegExp(userAgents["IP Address"]);
        });
      }
    });
  }

  /****************************************************************************
  * Determines if a request should be filtered.
  ****************************************************************************/
  this.isValid = function(httpRequest, queryString)
  {
    if (!dailyRequests[httpRequest.connection.remoteAddress])
      dailyRequests[httpRequest.connection.remoteAddress] = [];

    dailyRequests[httpRequest.connection.remoteAddress].push(new Date());

    var affiliateOverrides = null;
    if (queryString.s)
      affiliateOverrides = getAffiliateOverrides(queryString.s);

    if (isIpAddressBanned(httpRequest.connection.remoteAddress))
      return true;
    else if (isUserAgentBanned(httpRequest.headers["user-agent"]))
      return true;
    else if (isReferrerBanned(httpRequest.headers.referer))
      return true;
    else if (!affiliateOverrides || affiliateOverrides.ClickIPFilterFlag)
    {
      if (httpRequest.connection.remoteAddress != queryString.rip)
        return true;
    }
    else if (!affiliateOverrides || affiliateOverrides.ClickReferrerFilterFlag)
    {
      if (httpRequest.headers.referer.indexOf(queryString.rr) < 0)
        return true;
    }
    else if (hasExceededClicksPerDay(affiliateOverrides))
      return true;
    else if (isDoubleClick())
      return true;

    return false;
  }
}

module.exports.RequestFilters = RequestFilters;