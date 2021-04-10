Hive GrokSerDe
==============

# What is Grok?
https://en.wikipedia.org/wiki/Grok

In this project:
* grok is a pattern matcher like the regex, but it has some pre-define patterns.
* with those patterns, grok is very easy to use than regex. 

### Pre-define patterns
See: https://github.com/thekrakken/java-grok/tree/master/src/main/resources/patterns

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

* Column names should be matched with capture name of grok patterns.
* `output.format.string` is not provided, serialization is not working.

# TODO

* Use custom patterns
* More test

# Acknowledgement
* This project is heavily adopted from https://github.com/varmaprr/spark
* Grok java library is from https://github.com/thekrakken/java-grok