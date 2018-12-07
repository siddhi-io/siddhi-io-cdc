# API Docs - v1.0.5

## Source

### cdc *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">The CDC source receives events when a Database table's change event (INSERT, UPDATE, DELETE) is triggered. The events are received in key-value format.<br>The following are key values of the map of a CDC change event and their descriptions.<br>&nbsp;&nbsp;&nbsp;&nbsp;For insert: Keys will be specified table's columns.<br>&nbsp;&nbsp;&nbsp;&nbsp;For delete: Keys will be 'before_' followed by specified table's columns. Eg: before_X.<br>&nbsp;&nbsp;&nbsp;&nbsp;For update: Keys will be specified table's columns and 'before_' followed by specified table's columns.<br>For 'polling' mode: Keys will be specified table's columns.<br>See parameter: mode for supported databases and change events.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="cdc", url="<STRING>", mode="<STRING>", jdbc.driver.name="<STRING>", username="<STRING>", password="<STRING>", pool.properties="<STRING>", datasource.name="<STRING>", table.name="<STRING>", polling.column="<STRING>", polling.interval="<INT>", operation="<STRING>", connector.properties="<STRING>", database.server.id="<STRING>", database.server.name="<STRING>", @map(...)))
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
        <td style="vertical-align: top; word-wrap: break-word">Connection url to the database.<br>use format: jdbc:mysql://&lt;host&gt;:&lt;port&gt;/&lt;database_name&gt; </td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">mode</td>
        <td style="vertical-align: top; word-wrap: break-word">Mode to capture the change data. Mode 'polling' uses a polling.column to monitor the given table. Mode 'listening' uses logs to monitor the given table.<br>The required parameters are different for each modes.<br>mode 'listening' currently supports only MySQL. INSERT, UPDATE, DELETE events can be received.<br>mode 'polling' supports RDBMS. INSERT, UPDATE events can be received.</td>
        <td style="vertical-align: top">listening</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">jdbc.driver.name</td>
        <td style="vertical-align: top; word-wrap: break-word">The driver class name for connecting the database. **Required for 'polling' mode.**</td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">Username of a user with SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT privileges on Change Data Capturing table.<br>For polling mode, a user with SELECT privileges.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">password</td>
        <td style="vertical-align: top; word-wrap: break-word">Password for the above user.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">pool.properties</td>
        <td style="vertical-align: top; word-wrap: break-word">Any pool parameters for the database connection must be specified as key-value pairs.</td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">datasource.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the wso2 datasource to connect to the database. When datasource.name is provided, the url, username and password are not needed. Has a more priority over url based connection.<br>Accepted only when mode is set to 'polling'.</td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the table which needs to be monitored for data changes.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">polling.column</td>
        <td style="vertical-align: top; word-wrap: break-word">Column name on which the polling is done to capture the change data. It is recommend to have a TIMESTAMP field as the polling.column in order to capture inserts and updates.<br>Numeric auto incremental fields and char fields can be also used as polling.column. Note that it will only support insert change capturing and depends on how the char field's data is input.<br>**Mandatory when mode is 'polling'.**</td>
        <td style="vertical-align: top"><Empty_String></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">polling.interval</td>
        <td style="vertical-align: top; word-wrap: break-word">The interval in seconds to poll the given table for changes.<br>Accepted only when mode is set to 'polling'.</td>
        <td style="vertical-align: top">1</td>
        <td style="vertical-align: top">INT</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">operation</td>
        <td style="vertical-align: top; word-wrap: break-word">Interested change event operation. 'insert', 'update' or 'delete'. Required for 'listening' mode.<br>Not case sensitive.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">connector.properties</td>
        <td style="vertical-align: top; word-wrap: break-word">Debezium connector specified properties as a comma separated string. <br>This properties will have more priority over the parameters. Only for 'listening' mode</td>
        <td style="vertical-align: top">Empty_String</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.id</td>
        <td style="vertical-align: top; word-wrap: break-word">For MySQL, a unique integer between 1 to 2^32 as the ID, This is used when joining MySQL database cluster to read binlog. Only for 'listening'mode.</td>
        <td style="vertical-align: top">Random integer between 5400 and 6400</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Logical name that identifies and provides a namespace for the particular database server. Only for 'listening' mode.</td>
        <td style="vertical-align: top">{host}_{port}</td>
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
<p style="word-wrap: break-word">In this example, the cdc source starts listening to the row insertions  on students table with columns name and id which is under MySQL SimpleDB database that can be accessed with the given url</p>

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
<p style="word-wrap: break-word">In this example, the cdc source starts listening to the row updates on students table which is under MySQL SimpleDB database that can be accessed with the given url.</p>

<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', operation = 'delete', 
@map(type='keyvalue', @attributes(before_id = 'before_id', before_name = 'before_name')))
define stream inputStream (before_id string, before_name string);
```
<p style="word-wrap: break-word">In this example, the cdc source starts listening to the row deletions on students table which is under MySQL SimpleDB database that can be accessed with the given url.</p>

<span id="example-4" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 4</span>
```
@source(type = 'cdc', mode='polling', polling.column = 'id', 
jdbc.driver.name = 'com.mysql.jdbc.Driver', url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', 
@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))
define stream inputStream (id int, name string);
```
<p style="word-wrap: break-word">In this example, the cdc source starts polling students table for inserts. polling.column is an auto incremental field. url, username, password, and jdbc.driver.name are used to connect to the database.</p>

<span id="example-5" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 5</span>
```
@source(type = 'cdc', mode='polling', polling.column = 'id', datasource.name = 'SimpleDB',
table.name = 'students', 
@map(type='keyvalue'), @attributes(id = 'id', name = 'name'))
define stream inputStream (id int, name string);
```
<p style="word-wrap: break-word">In this example, the cdc source starts polling students table for inserts. polling.column is a char column with the pattern S001, S002, ... . datasource.name is used to connect to the database. Note that the datasource.name works only with the Stream Processor.</p>

<span id="example-6" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 6</span>
```
@source(type = 'cdc', mode='polling', polling.column = 'last_updated', datasource.name = 'SimpleDB',
table.name = 'students', 
@map(type='keyvalue'))
define stream inputStream (name string);
```
<p style="word-wrap: break-word">In this example, the cdc source starts polling students table for inserts and updates. polling.column is a timestamp field.</p>

