
Siddhi-io-cdc
======================================

The **siddhi-io-cdc extension** is an extension to <a target="_blank" href="https://wso2.github.io/siddhi">Siddhi</a>. It publishes change data of MySQL database in the key-value map format.

## Prerequisites

* The MySQL server should be configured to use a row-level binary log.
* MySQL jdbc driver should be added to the `<STREAM_PROCESSOR_HOME>/lib` directory.
* WSO2 SP State persistence should be enabled.
* A MySQL user should be created with SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT privileges on the tables he wants to capture changes.

Find some useful links below:

* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc">Source code</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc/releases">Releases</a>
* <a target="_blank" href="https://github.com/wso2-extensions/siddhi-io-cdc/issues">Issue tracker</a>

## Latest API Docs

Latest API Docs is <a target="_blank" href="https://wso2-extensions.github.io/">1.0.0</a>.

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

## Jenkins Build Status

---

|  Branch | Build Status |
| :------ |:------------ |
| master  | [![Build Status](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-cdc/badge/icon)](https://wso2.org/jenkins/job/siddhi/job/siddhi-io-cdc/) |

---

## Features

* <a target="_blank" href="https://wso2-extensions.github.io/siddhi-io-cdc/api/1.0.9/#cdc-source">cdc</a> *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*<br><div style="padding-left: 1em;"><p>The cdc source receives events when MySQL database's change event occurs. The events are received in the form of key-value mappings. <br><br>The following are key values of the map of a change data and their descriptions: 
	*   X : The table's column X value after the event occurred. Applicable when 'insert' or 'update' operations are specified. 
	* before_X : The table's column X value before the event occurred. Applicable when 'delete' or 'update' operations are specified.

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