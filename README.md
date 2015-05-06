# Flume Interceptor: Enriched Event

This project provides an interceptor to enrich event body with custom data. The implementation consist of:

- EnrichedEventBody: represents enriched event body. The enriched event will have 2 attributes: extraData and message.
    extraData will be fed with the custom data set in the configuration file (see below) while message will preserve
    the original event body string. The enriched event is then serialized to a JSON object.
- EnrichmentInterceptor: implements the Flume interceptor interface. The intercept() method will add the custom data
    to the original event body and serialize it back as a JSON string.
- JSONStringSerializer: the JSON serializer

## How to use

Clone the project:

```sh
$ git clone https://github.com/keedio/flume-enrichment-interceptor-skeleton.git
```

Build with Maven:

```sh
$ mvn clean install
```

The jar file will be installed in your local maven repository and can be found in the target/ subdirectory also. Add it
to your Flume classpath.

Configure your agent to use this interceptor, setting the following options in your configuration file:

```ini
# interceptor
a1.sources.r1.interceptors = i1
a1.sources.r1.interceptors.i1.type = org.apache.flume.interceptor.EnrichmentInterceptor$EnrichmentBuilder
# Full path to the properties file that contains the extra data to enrich the event with
a1.sources.r1.interceptors.i1.properties.filename = /path/to/filename.properties
# The format of incoming events ( DEFAULT | enriched )
a1.sources.r1.interceptors.i1.event.type = DEFAULT


# Full path to folder that contains the files where to match the pattern
a1.sources.r1.interceptors.i1.properties.folder.logs = /anotherPath/to

# A map of regexps, where each regexp may be composed of Named captured groups according syntax (?<name>regex)
a1.sources.r1.interceptors.i1.properties.regexp.1 = (?<name>regex)
a1.sources.r1.interceptors.i1.properties.regexp.2 = (?<nameA>regex)\\METACHARACTER(?<nameB>regex)\\..
.......................
a1.sources.r1.interceptors.i1.properties.regexp.n = 
```
Althoug Java 7 allows named captured groups, flume-enrich-interceptor is using named-regexp 0.2.3 tony19's library because it adds
interestings features, example  (?\<foo_foo\>regex) or (?\<foo foo\>regex) are not allowed in Java 7.
Thanks to tony19: https://github.com/tony19/named-regexp

Example of custom properties:
```ini
hostname = localhost
domain = localdomain
```

The enriched event body will contain:
```json
{
 "extraData":{"hostname": "localhost", "domain": "localdomain"},
 "message": "the original body string"
}
```

Thanks to tony19: https://github.com/tony19/named-regexp
Althoug Java 7 allows named captured groups, flume-enrich-interceptor is using named-regexp 0.2.3 tony19's library because it adds
interestings features, example  (?\<foo_foo\>regex) or (?\<foo foo\>regex) are not allowed in Java 7.



## Changes from version 0.0.1

Added regexp in flume's context.
EnrichedEventBody.message is now a String (was byte[]).
