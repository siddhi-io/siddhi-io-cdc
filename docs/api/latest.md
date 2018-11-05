# API Docs - v1.0.0-SNAPSHOT

## Source

### cdc *<a target="_blank" href="https://wso2.github.io/siddhi/documentation/siddhi-4.0/#source">(Source)</a>*

<p style="word-wrap: break-word">The CDC source receives events when a specified MySQL table's change event (INSERT, UPDATE, DELETE) is triggered. The events are received in key-value map format.<br>The following are key values of the map of a CDC change event and their descriptions.<br>&nbsp;&nbsp;&nbsp;&nbsp;For insert: Keys will be specified table's columns<br>&nbsp;&nbsp;&nbsp;&nbsp;For delete: Keys will be 'before_' followed by specified table's columns. Eg: before_X<br>&nbsp;&nbsp;&nbsp;&nbsp;For update: Keys will be specified table's columns and 'before_' followed by specified table's columns.</p>

<span id="syntax" class="md-typeset" style="display: block; font-weight: bold;">Syntax</span>
```
@source(type="cdc", url="<STRING>", username="<STRING>", password="<STRING>", table.name="<STRING>", operation="<STRING>", connector.properties="<STRING>", database.server.id="<STRING>", database.server.name="<STRING>", @map(...)))
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
        <td style="vertical-align: top">username</td>
        <td style="vertical-align: top; word-wrap: break-word">Username of a user with SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT privileges on Change Data Capturing table.</td>
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
        <td style="vertical-align: top">table.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Name of the table which needs to be monitored for data changes.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">operation</td>
        <td style="vertical-align: top; word-wrap: break-word">Interested change event operation. 'insert', 'update' or 'delete'. <br>Not case sensitive.</td>
        <td style="vertical-align: top"></td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">No</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">connector.properties</td>
        <td style="vertical-align: top; word-wrap: break-word">Debezium connector specified properties as a comma separated string. <br>This properties will have more priority over the parameters.</td>
        <td style="vertical-align: top">Empty_String</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.id</td>
        <td style="vertical-align: top; word-wrap: break-word">For MySQL, a unique integer between 1 to 2^32 as the ID, This is used when joining MySQL database cluster to read binlog</td>
        <td style="vertical-align: top">Random integer between 5400 and 6400</td>
        <td style="vertical-align: top">STRING</td>
        <td style="vertical-align: top">Yes</td>
        <td style="vertical-align: top">No</td>
    </tr>
    <tr>
        <td style="vertical-align: top">database.server.name</td>
        <td style="vertical-align: top; word-wrap: break-word">Logical name that identifies and provides a namespace for the particular database server</td>
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
before_name string, , name string);
```
<p style="word-wrap: break-word">In this example, the cdc source starts listening to the row updates on students table which is under MySQL SimpleDB database that can be accessed with the given url</p>

<span id="example-3" class="md-typeset" style="display: block; color: rgba(0, 0, 0, 0.54); font-size: 12.8px; font-weight: bold;">EXAMPLE 3</span>
```
@source(type = 'cdc' , url = 'jdbc:mysql://localhost:3306/SimpleDB', 
username = 'cdcuser', password = 'pswd4cdc', 
table.name = 'students', operation = 'delete', 
@map(type='keyvalue', @attributes(before_id = 'before_id', before_name = 'before_name')))
define stream inputStream (before_id string, before_name string);
```
<p style="word-wrap: break-word">In this example, the cdc source starts listening to the row deletions on students table which is under MySQL SimpleDB database that can be accessed with the given url</p>

