# Siddhi-io-cdc

The **siddhi-io-cdc extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a>. It receives change data from MySQL, MS SQL Server, Postgresql, H2 and Oracle in the key-value format.

## Prerequisites
Default mode (listening):

* The MySQL server should be configured to use a row-level binary log.
* WSO2 SP State persistence should be enabled.
* A MySQL user should be created with SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT privileges on the tables he wants to capture changes.

Polling mode:

* Change data capturing table should be have a polling column. Auto Incremental column or Timestamp can be used.

Please see API docs for more details about change data capturing modes.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc/issues">Issue tracker</a>

## Latest API Docs

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-cdc/api/2.0.0">2.0.0</a>.

## How to use

 **Using the extension in <a target="_blank" href="https://github.com/wso2/product-sp">WSO2 Stream Processor</a>**

 * You can use this extension with the latest <a target="_blank" href="https://github.com/wso2/product-sp/releases">WSO2 Stream Processor</a> that is a part of the <a target="_blank" href="http://wso2.com/analytics?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">WSO2 Analytics</a> offering, with editor, debugger and simulation support.

* This extension is shipped by default with WSO2 Stream Processor. If you need to use an alternative version of this extension, you can replace the component <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc/releases">jar</a> that can be found in the `<STREAM_PROCESSOR_HOME>/lib` directory.

**Using the extension as a <a target="_blank" href="https://wso2.github.io/siddhi/documentation/running-as-a-java-library">java library</a>**

 * This extension can be added as a maven dependency along with other Siddhi dependencies to your project.

```
<dependency>
  <groupId>org.wso2.extension.siddhi.io.cdc</groupId>
  <artifactId>siddhi-io-cdc</artifactId>
  <version>x.x.x</version>
 </dependency>
```
## Running Integration tests in docker containers(Optional)

The CDC functionality are tested with the docker base integration test framework.
The test framework initialize a docker container with required configuration before execute the test suit.

**Start integration tests**

 1. Install and run docker

 2. To run the integration tests, navigate to the siddhi-io-cdc/ directory and issue the following commands.

    * H2 default:

            mvn clean install

    * MySQL 5.7:

             mvn verify -P local-mysql -Dskip.surefire.test=true

    * Postgres 9.6:

             mvn verify -P local-postgres -Dskip.surefire.test=true

    * MSSQL:

             mvn verify -P local-mssql -Dskip.surefire.test=true

    * Oracle 11.2.0.2-xe:

             mvn verify -P local-oracle -Dskip.surefire.test=true

## Jenkins Build Status


---
| Branch | Build Status |
| :------ |:------------|
| master  |  [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-cdc/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-cdc/)  |

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-cdc/api/2.0.0/#cdc-source">cdc</a> *<a target="_blank" href="http://siddhi.io/documentation/siddhi-5.x/query-guide-5.x/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>The CDC source receives events when change events (i.e., INSERT, UPDATE, DELETE) are triggered for a database table. Events are received in the 'key-value' format.<br>The key values of the map of a CDC change event are as follows.<br>&nbsp;&nbsp;&nbsp;&nbsp;For insert: Keys are specified as columns of the table.<br>&nbsp;&nbsp;&nbsp;&nbsp;For delete: Keys are followed followed by the specified table columns. This is achieved via 'before_'. e.g., specifying 'before_X' results in the key being added before the column named 'X'.<br>&nbsp;&nbsp;&nbsp;&nbsp;For update: Keys are followed followed by the specified table columns. This is achieved via 'before_'. e.g., specifying 'before_X' results in the key being added before the column named 'X'.<br>For 'polling' mode: Keys are specified as the coloumns of the table.<br>See parameter: mode for supported databases and change events.</p></div>

## How to Contribute

 * Report issues at <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc/issues">GitHub Issue Tracker</a>.

  * Send your contributions as pull requests to the <a target="_blank" href="https://github
  .com/wso2-extensions/siddhi-io-cdc/tree/master">master branch</a>.

## Contact us

 * Post your questions with the <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">"Siddhi"</a> tag in <a target="_blank" href="http://stackoverflow.com/search?q=siddhi">Stackoverflow</a>.

 * Siddhi developers can be contacted via the following mailing lists:

    Developers List   : [dev@wso2.org](mailto:dev@wso2.org)

    Architecture List : [architecture@wso2.org](mailto:architecture@wso2.org)

## Support
 * We are committed to ensuring support for this extension in production. Our unique approach ensures that all support leverages our open development methodology and is provided by the very same engineers who build the technology.

* For more details and to take advantage of this unique opportunity contact us via <a target="_blank" href="http://wso2.com/support?utm_source=gitanalytics&utm_campaign=gitanalytics_Jul17">http://wso2.com/support/</a>.
