var fs = require("fs");
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
var ProxyServer = function(portToListenTo, mongoServer, smtpServer, statsdServer)
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
        statsdClient.increment("RedirectServer.MongoConnectionFailure");
        sendEmail("Redirect Server encountered a critical or fatal exception", "Failed to connect to the Mongo server. Reason: " + exception);
        console.log("Failed to connect to the Mongo server. Reason: " + exception);
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
  function isValidRequest(httpRequest, queryString)
  {
    if (httpRequest.method !== "GET" || !requestFilters.isValid(httpRequest, queryString))
    {
      statsdClient.increment("RedirectServer.Requests.Invalid");
      return false;
    }

    return true;
  }

  /****************************************************************************
  * Looks up the RequestId (RID) in Mongo and redirects the user to the URL.
  ****************************************************************************/
  function proxyRequest(httpRequest, queryString, httpResponse)
  {
    var requestStart = new Date().getTime();

    if (queryString.keywordId)
    {
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

    var proxyClient = http.createClient(portToListenTo, httpRequest.headers["host"]);
    var proxyRequest = proxyClient.request(httpRequest.method, httpRequest.url, httpRequest.headers);
    proxyRequest.addListener("response", function(proxyResponse)
    {
      proxyResponse.addListener("data", function(chunk)
      {
        response.write(chunk, "binary");
      });

      proxyResponse.addListener("end", function()
      {
        proxyRequest.end();
      });
    });

    request.addListener("data", function(chunk)
    {
      proxy_request.write(chunk, "binary");
    });

    request.addListener("end", function()
    {
      proxy_request.end();
    });
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
  * Writes an entry to the log.
  ****************************************************************************/
  function writeEntry(severity, httpRequest, message)
  {
    if (!mongoDb)
    {
      sendEmail("Log Entry: " + severity, message);
      console.log(severity + ": " + message);
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
        {
          httpResponse.statusCode = 200;
          httpResponse.setHeader("Content-Type", "text/html");
          httpResponse.write("Heartbeat");
        }
        else if (parsedUrl.path.indexOf("/browser") > -1)
        {
          statsdClient.increment("RedirectServer.TrackBrowser");
          writeEntry("Message", httpRequest, "Browser completed page rendering.");
        }
        else
          proxyRequest(httpRequest, queryString);
      }

      httpResponse.end();
    }).listen(portToListenTo);

    console.log("Redirect server listening on port " + portToListenTo);
  }
}

module.exports.ProxyServer = ProxyServer;

var server = require("./ProxyServer");
var proxyServer = new server.ProxyServer(80, mongoServer, smtpServer, statsdServer);
proxyServer.startListening();