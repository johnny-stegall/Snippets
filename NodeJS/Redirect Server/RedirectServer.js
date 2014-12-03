var http = require("http");
var url = require("url");
var emailjs = require("emailjs");
var lynx = require("lynx");
var mongoClient = require("mongodb").MongoClient;
var filtering = require("./RequestFilters.js");
var requestFilters;

/******************************************************************************
* Constructor, private properties and methods.
******************************************************************************/
var RedirectServer = function(portToListenTo, mongoServer, smtpServer, statsdServer)
{
  var smtpClient = emailjs.server.connect(smtpServer);
  var statsdClient = new lynx(statsdServer.Host, statsdServer.Port);
  var mongoDb;
  initializeMongo(mongoServer);

  /****************************************************************************
  * Opens a connection to Mongo.
  ****************************************************************************/
  function initializeMongo(mongoServer)
  {
    mongoClient.connect(mongoServer.ConnectionString, mongoServer.Options,
    function(exception, mongoDatabase)
    {
      if (exception || !mongoDatabase)
      {
        console.log("Failed to connect to the Mongo server. Reason: " + exception);
        statsdClient.increment("RedirectServer.MongoConnectionFailure");
        sendEmail("Redirect Server encountered a critical or fatal exception", "Failed to connect to the Mongo server. Reason: " + exception);
        return;
      }

      mongoDb = mongoDatabase;
      requestFilters = new filtering.RequestFilters(statsdClient, mongoDb);
    });
  }

  /****************************************************************************
  * Returns false if a request is not a GET request or is blacklisted;
  * returns true otherwise.
  ****************************************************************************/
  function isValidRequest(httpRequest, queryString, httpResponse)
  {
    if (httpRequest.method !== "GET" || !requestFilters.isValid(httpRequest, queryString))
    {
      statsdClient.increment("RedirectServer.Requests.Invalid");
      httpResponse.end();
      return false;
    }

    return true;
  }

  /****************************************************************************
  * Looks up the RequestId (RID) in Mongo and redirects the user to the URL.
  ****************************************************************************/
  function redirectRequest(redirectId, httpRequest, httpResponse)
  {
    var requestStart = new Date().getTime();
    var ObjectID = require("mongodb").ObjectID;

    mongoDb.collection("CampaignUrls").findOne({ "_id": new ObjectID(redirectId) },
    function (exception, campaignUrl)
    {
      if (exception)
      {
        httpResponse.statusCode = 500;
        httpResponse.end();

        statsdClient.increment("RedirectServer.Requests.QueryFailed");
        writeEntry("Error", httpRequest, "Failed to query Mongo for the URL. Reason: " + exception.err);
      }
      else if (!campaignUrl)
      {
        httpResponse.end();

        statsdClient.increment("RedirectServer.Requests.NoUrl");
        writeEntry("Warning", httpRequest, "No URL found for " + redirectId + ".");
      }
      else
      {
        httpResponse.statusCode = 301;
        httpResponse.setHeader("Location", campaignUrl.Url);
        httpResponse.end();

        statsdClient.timing("RedirectServer.Requests.Redirected", (new Date().getTime() - requestStart) / 1000);
      }
    });
  }

  /****************************************************************************
  * Responds to a heartbeat request.
  ****************************************************************************/
  function respondToPing(httpRequest, httpResponse)
  {
    httpResponse.statusCode = 200;
    httpResponse.setHeader("Content-Type", "text/html");
    httpResponse.write("Heartbeat");
    httpResponse.end();
  }

  /****************************************************************************
  * Sends an email.
  ****************************************************************************/
  function sendEmail(emailSubject, emailBody)
  {
    statsdClient.increment("RedirectServer.Email.Sent");

    smtpClient.send(
    {
      to: "{email-recipients}",
      from: "{from-email}",
      subject: emailSubject,
      text: emailBody
    },
    function(exception, message)
    {
      if (exception)
      {
        statsdClient.increment("RedirectServer.Email.Failed");
        console.log("Failed to send email. Reason: " + exception);
      }
    });
  }

  /****************************************************************************
  * Sends statistics to StatsD that a browser has rendered a page.
  ****************************************************************************/
  function trackBrowserComplete(httpRequest, httpResponse)
  {
    httpResponse.end();
    statsdClient.increment("RedirectServer.TrackBrowser");
  }

  /****************************************************************************
  * Writes an entry to the log.
  ****************************************************************************/
  function writeEntry(severity, httpRequest, message)
  {
    if (!mongoDb)
    {
      console.log(severity + ": " + message);
      sendEmail("Log Entry: " + severity, message);
    }

    mongoDb.collection("Log").save(
    {
      "Severity": severity,
      "Server": httpRequest.headers.host,
      "Message": message,
      "Referrer": httpRequest.headers.referer,
      "IpAddress": httpRequest.connection.remoteAddress,
      "UserAgent": httpRequest.headers["user-agent"],
      "Url": "http://" + httpRequest.headers.host + httpRequest.url
    },
    function(saveException, savedEntry)
    {
      if (saveException || !savedEntry)
      {
        statsdClient.increment("RedirectServer.Log.WriteFailure");
        sendEmail("Log Failure", "Failed to write to the log. Reason: " + (exception || "unknown."));
      }
    });

    if (severity === "Fatal" || severity == "Critical")
    {
      statsdClient.increment("RedirectServer.Log." + severity);
      sendEmail("Redirect Server encountered a critical or fatal exception", exception);
    }
  }

  /******************************************************************************
  * Parses the query string for unique keys and queries a Mongo database to get
  * the URL for those unique keys.
  ******************************************************************************/
  this.startListening = function()
  {
    http.createServer(function(httpRequest, httpResponse)
    {
      var parsedUrl = url.parse(httpRequest.url, true);
      var queryString = parsedUrl.query;

      if (isValidRequest(httpRequest, queryString, httpResponse))
      {
        if (parsedUrl.path.indexOf("/heartbeat") > -1)
          respondToPing(httpRequest, httpResponse);
        if (parsedUrl.path.indexOf("/browser") > -1)
          trackBrowserComplete(httpRequest, httpResponse);
        else if (queryString.rid)
          redirectRequest(queryString.rid, httpRequest, httpResponse);
      }
    }).listen(portToListenTo);

    console.log("Redirect server listening on port " + portToListenTo);
  }
}

module.exports.RedirectServer = RedirectServer;

var server = require("./RedirectServer");
var redirectServer = new server.RedirectServer(80, mongoServer, smtpServer, statsdServer);
redirectServer.startListening();