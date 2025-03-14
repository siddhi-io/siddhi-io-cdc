# API Docs - v2.0.17

!!! Info "Tested Siddhi Core version: *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/">5.1.21</a>*"
    It could also support other Siddhi Core minor versions.

## Source

### cdc *<a target="_blank" href="http://siddhi.io/en/v5.1/docs/query-guide/#source">(Source)</a>*
<p></p>
<p style="word-wrap: break-word;margin: 0;">The CDC source receives events when change events (i.e., INSERT, UPDATE, DELETE) are triggered for a database table. Events are received in the 'key-value' format.<br><br>There are two modes you could perform CDC: Listening mode and Polling mode.<br><br>In polling mode, the datasource is periodically polled for capturing the changes. The polling period can be configured.<br>In polling mode, you can only capture INSERT and UPDATE changes.<br><br>On listening mode, the Source will keep listening to the Change Log of the database and notify in case a change has taken place. Here, you are immediately notified about the change, compared to polling mode.<br><br>The key values of the map of a CDC change event are as follows.<br><br>For 'listening' mode: <br>&nbsp;&nbsp;&nbsp;&nbsp;For insert: Keys are specified as columns of the table.<br>&nbsp;&nbsp;&nbsp;&nbsp;For delete: Keys are followed by the specified table columns. This is achieved via 'before_'. e.g., specifying 'before_X' results in the key being added before the column named 'X'.<br>&nbsp;&nbsp;&nbsp;&nbsp;For update: Keys are followed followed by the specified table columns. This is achieved via 'before_'. e.g., specifying 'before_X' results in the key being added before the column named 'X'.<br><br>For 'polling' mode: Keys are specified as the columns of the table.In order to connect in to the database table for receive CDC events, url, username, password and driverClassName(in polling mode) can be provided in deployment.yaml file under the siddhi namespace as below,  </p><pre>
  siddhi:
    extensions:
      -
        extension:
          name: 'cdc'
          namespace: 'source'
          properties:
            url: jdbc:sqlserver://localhost:1433;databaseName=CDC_DATA_STORE
            password: &lt;password&gt;
            username: &lt;&gt;
            driverClassName: com.microsoft.sqlserver.jdbc.SQLServerDriver  </pre><p style="word-wrap: break-word;margin: 0;"><br><br>***Preparations required for working with Oracle Databases in listening mode***<br><br>Using the extension in Windows, Mac OSX and AIX are pretty straight forward inorder to achieve the required behaviour please follow the steps given below<br><br>&nbsp;&nbsp;- Download the compatible version of oracle instantclient for the database version from [here](https://www.oracle.com/database/technologies/instant-client/downloads.html) and extract<br>&nbsp;&nbsp;- Extract and set the environment variable <code>LD_LIBRARY_PATH</code> to the location of instantclient which was exstracted as shown below<br>&nbsp;&nbsp;</p><pre>
    export LD_LIBRARY_PATH=&lt;path to the instant client location&gt;
  </pre><p style="word-wrap: break-word;margin: 0;"><br>&nbsp;&nbsp;- Inside the instantclient folder which was download there are two jars <code>xstreams.jar</code> and <code>ojdbc&lt;version&gt;.jar</code> convert them to OSGi bundles using the tools which were provided in the <code>&lt;distribution&gt;/bin</code> for converting the <code>ojdbc.jar</code> use the tool <code>spi-provider.sh|bat</code> and for the conversion of <code>xstreams.jar</code> use the jni-provider.sh as shown below(Note: this way of converting Xstreams jar is applicable only for Linux environments for other OSs this step is not required and converting it through the <code>jartobundle.sh</code> tool is enough)<br>&nbsp;&nbsp;</p><pre>
    ./jni-provider.sh &lt;input-jar&gt; &lt;destination&gt; &lt;comma seperated native library names&gt;
  </pre><p style="word-wrap: break-word;margin: 0;"><br>&nbsp;&nbsp;once ojdbc and xstreams jars are converted to OSGi copy the generated jars to the <code>&lt;distribution&gt;/lib</code>. Currently siddhi-io-cdc only supports the oracle database distributions 12 and above<br><br>*** Configurations for PostgreSQL***<br>When using listening mode with PostgreSQL, following properties has to be configured accordingly to create the connection.<br><br>&nbsp;&nbsp;&nbsp;&nbsp;***slot.name***: (default value = debezium) in postgreSQL only one connection can be created from single slot, so to create multiple connection custom slot.name should be provided.<br>&nbsp;<br>&nbsp;&nbsp;&nbsp;&nbsp;***plugin.name***: (default value = decoderbufs ) Logical decoding output plugin name which the database is configured with. Other supported values are pgoutput, decoderbufs, wal2json.<br><br>&nbsp;&nbsp;&nbsp;&nbsp;***table.name***: table name should be provided as &lt;schema_name&gt;.&lt;table_name&gt;. As an example, public.customer <br><br><br>See parameter: mode for supported databases and change events.</p>
<p></p>
<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>

```
@source(type="cdc", url="<STRING>", mode="<STRING>", jdbc.driver.name="<STRING>", username="<STRING>", password="<STRING>", pool.properties="<STRING>", datasource.name="<STRING>", table.name="<STRING>", polling.column="<STRING>", polling.interval="<INT>", operation="<STRING>", connector.properties="<STRING>", database.server.id="<STRING>", database.server.name="<STRING>", wait.on.missed.record="<BOOL>", missed.record.waiting.timeout="<INT>", polling.history.size="<INT>", cron.expression="<STRING>", plugin.name="<STRING>", @map(...)))
```

<span id="query-parameters" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">QUERY PARAMETERS</span>
<table>
    <tr>
        <th>Name</th>
        <th style="min-width: 20em">Description</th>
        <th>Default Value</th>
        <th>Possible Data Types</th>
        <th>Optional</th>
        <th>Dynamic</th>
    </tr>
    <tr>
        <td style="vertical-align: top">url</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The connection URL to the database.<br>F=The format used is: 'jdbc:mysql://&lt;host&gt;:&lt;port&gt;/&lt;database_name&gt;' </p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">mode</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Mode to capture the change data. The type of events that can be received, and the required parameters differ based on the mode. The mode can be one of the following:<br>'polling': This mode uses a column named 'polling.column' to monitor the given table. It captures change events of the 'RDBMS', 'INSERT, and 'UPDATE' types.<br>'listening': This mode uses logs to monitor the given table. It currently supports change events only of the 'MySQL', 'INSERT', 'UPDATE', and 'DELETE' types.</p></td>
        <td style="vertical-align: top">listening</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">jdbc.driver.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The driver class name for connecting the database. **It is required to specify a value for this parameter when the mode is 'polling'.**</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The username to be used for accessing the database. This user needs to have the 'SELECT', 'RELOAD', 'SHOW DATABASES', 'REPLICATION SLAVE', and 'REPLICATION CLIENT'privileges for the change data capturing table (specified via the 'table.name' parameter).<br>To operate in the polling mode, the user needs 'SELECT' privileges.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The password of the username you specified for accessing the database.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">pool.properties</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The pool parameters for the database connection can be specified as key-value pairs.</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">datasource.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Name of the wso2 datasource to connect to the database. When datasource name is provided, the URL, username and password are not needed. A datasource based connection is given more priority over the URL based connection.<br>&nbsp;This parameter is applicable only when the mode is set to **polling**, and it can be applied only when you use this extension with WSO2 Stream Processor.</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The name of the table that needs to be monitored for data changes.</p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">polling.column</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The column name  that is polled to capture the change data. It is recommended to have a TIMESTAMP field as the 'polling.column' in order to capture the inserts and updates.<br>Numeric auto-incremental fields and char fields can also be used as 'polling.column'. However, note that fields of these types only support insert change capturing, and the possibility of using a char field also depends on how the data is input.<br>**It is required to enter a value for this parameter only when the mode is 'polling'.**</p></td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">polling.interval</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The time interval (specified in seconds) to poll the given table for changes.<br>This parameter is applicable only when the mode is set to 'polling'.</p></td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">operation</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The change event operation you want to carry out. Possible values are 'insert', 'update', 'delete' or you can provide multiple operation as coma separated values. This parameter is not case sensitive.  <br>&nbsp;When provided the multiple operations, the relevant operation for each event will be return as a transport property **trp:operation** this can be access when mapping the events. According to the operation, the required fields from the stream has to be extracted. <br>**It is required to specify a value only when the mode is 'listening'.**<br></p></td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">connector.properties</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Here, you can specify Debezium connector properties as a comma-separated string. <br>The properties specified here are given more priority over the parameters. This parameter is applicable only for the 'listening' mode.</p></td>
        <td style="vertical-align: top">Empty_String</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.id</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">An ID to be used when joining MySQL database cluster to read the bin log. This should be a unique integer between 1 to 2^32. This parameter is applicable only when the mode is 'listening'.</p></td>
        <td style="vertical-align: top">Random integer between 5400 and 6400</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">A logical name that identifies and provides a namespace for the database server. This parameter is applicable only when the mode is 'listening'.</p></td>
        <td style="vertical-align: top">{host}_{port}</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">wait.on.missed.record</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Indicates whether the process needs to wait on missing/out-of-order records. <br>When this flag is set to 'true' the process will be held once it identifies a missing record. The missing record is identified by the sequence of the polling.column value. This can be used only with number fields and not recommended to use with time values as it will not be sequential.<br>This should be enabled ONLY where the records can be written out-of-order, (eg. concurrent writers) as this degrades the performance.</p></td>
        <td style="vertical-align: top">false</td>
        <td style="vertical-align: top">BOOL</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">missed.record.waiting.timeout</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">The timeout (specified in seconds) to retry for missing/out-of-order record. This should be used along with the wait.on.missed.record parameter. If the parameter is not set, the process will indefinitely wait for the missing record.</p></td>
        <td style="vertical-align: top">-1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">polling.history.size</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">Should be use when metrics are enabled, Define the number of polling details that should expose to metrics, Ex: if polling.history.size is 20, then it will expose details of last 20 polling</p></td>
        <td style="vertical-align: top">10</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">cron.expression</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is used to specify a timestamp in cron expression.The records which has been inserted or updated is printed when the given expression satisfied by the system time. This parameter is applicable only when the mode is 'polling'.</p></td>
        <td style="vertical-align: top">None</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">plugin.name</td>
        <td style="vertical-align: top; word-wrap: break-word"><p style="word-wrap: break-word;margin: 0;">This is used when the logical decoding output plugin needed to specify to create the connection to the database. Mostly this will be required on PostgreSQL.</p></td>
        <td style="vertical-align: top">decoderbufs</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
</table>

<span id="examples" class="md-typeset" style="display: block; font-weight: bold;">Examples</span>
<span id="example-1" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 1</span>
```
@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', operation = 'insert', 
@map(type='keyvalue', @attributes(id = 'id', name = 'name')))
define stream inputStream (id string, name string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source listens to the row insertions that are made in the 'students' table with the column name, and the ID. This table belongs to the 'SimpleDB' MySQL database that can be accessed via the given URL.</p>
<p></p>
<span id="example-2" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 2</span>
```
@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', operation = 'update', 
@map(type='keyvalue', @attributes(id = 'id', name = 'name', 
before_id = 'before_id', before_name = 'before_name')))
define stream inputStream (before_id string, id string, 
before_name string , name string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source listens to the row updates that are made in the 'students' table. This table belongs to the 'SimpleDB' MySQL database that can be accessed via the given URL.</p>
<p></p>
<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', operation = 'delete', 
@map(type='keyvalue', @attributes(before_id = 'before_id', before_name = 'before_name')))
define stream inputStream (before_id string, before_name string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source listens to the row deletions made in the 'students' table. This table belongs to the 'SimpleDB' database that can be accessed via the given URL.</p>
<p></p>
<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', operation = 'insert,update,delete', 
@map(type='keyvalue', @attributes(before_id = 'before_id', before_name = 'before_name', name = 'name', id = 'id', operation= 'trp:operation')))
define stream inputStream (id string, name string, before_id string, before_name string, operation string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source listens to multiple operations of the 'students' table. This table belongs to the 'SimpleDB' database that can be accessed via the given URL.</p>
<p></p>
<span id="example-5" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 5</span>
```
@source(type = 'cdc', mode='polling', polling.column = 'id', 
jdbc.driver.name = 'com.mysql.jdbc.Driver', url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', 
@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))
define stream inputStream (id int, name string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source polls the 'students' table for inserts. 'id' that is specified as the polling colum' is an auto incremental field. The connection to the database is made via the URL, username, password, and the JDBC driver name.</p>
<p></p>
<span id="example-6" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 6</span>
```
@source(type = 'cdc', mode='polling', polling.column = 'id', datasource.name = 'SimpleDB',
table.name = 'students', 
@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))
define stream inputStream (id int, name string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source polls the 'students' table for inserts. The given polling column is a char column with the 'S001, S002, ... .' pattern. The connection to the database is made via a data source named 'SimpleDB'. Note that the 'datasource.name' parameter works only with the Stream Processor.</p>
<p></p>
<span id="example-7" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 7</span>
```
@source(type = 'cdc', mode='polling', polling.column = 'last_updated', datasource.name = 'SimpleDB',
table.name = 'students', 
@map(type='keyvalue'))
define stream inputStream (name string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source polls the 'students' table for inserts and updates. The polling column is a timestamp field.</p>
<p></p>
<span id="example-8" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 8</span>
```
@source(type='cdc', jdbc.driver.name='com.mysql.jdbc.Driver', url='jdbc:mysql://localhost:3306/SimpleDB', username='cdcuser', password='pswd4cdc', table.name='students', mode='polling', polling.column='id', operation='insert', wait.on.missed.record='true', missed.record.waiting.timeout='10',
@map(type='keyvalue'), 
@attributes(batch_no='batch_no', item='item', qty='qty'))
define stream inputStream (id int, name string);
```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source polls the 'students' table for inserts. The polling column is a numeric field. This source expects the records in the database to be written concurrently/out-of-order so it waits if it encounters a missing record. If the record doesn't appear within 10 seconds it resumes the process.</p>
<p></p>
<span id="example-9" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 9</span>
```
@source(type = 'cdc', url = 'jdbc:oracle:thin://localhost:1521/ORCLCDB', username='c##xstrm', password='xs', table.name='DEBEZIUM.sweetproductiontable', operation = 'insert', connector.properties='oracle.outserver.name=DBZXOUT,oracle.pdb=ORCLPDB1' @map(type = 'keyvalue'))
define stream insertSweetProductionStream (ID int, NAME string, WEIGHT int);

```
<p></p>
<p style="word-wrap: break-word;margin: 0;">In this example, the CDC source connect to an Oracle database and listens for insert queries of sweetproduction table</p>
<p></p>
