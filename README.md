Hive GrokSerDe
==============

# Use
```sql
CREATE EXTERNAL TABLE access_log (
    clientip string,
    ident string,
    auth string,
    timestamp string,
    request string,
    response string,
    bytes string,
    bytes string,
    referrer string,
    agent string
)
ROW FORMAT SERDE 'com.github.minyk.hive.GrokSerDe'
WITH SERDEPROPERTIES (
    "input.pattern" = "%{COMBINEDAPACHELOG}",
    "output.format.string" = "%1$s %2$s %3$s [%4$s] \"GET %5$s HTTP/1.1\" %6$s %7$s \"%8$s\" \"%9$s\""
)
STORED AS TEXTFILE
LOCATION 's3a://access_log/access_log';
```

* Column names should be matched with capture name of grok pattern.
* `output.format.string` is not provided, serialization is not working.

# TODO

* Define custom pattern
* More test

# Acknowledgement
This project is heavily adopted from https://github.com/varmaprr/spark