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
```

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

## Changes from version 0.0.1

EnrichedEventBody.message is now a String (was byte[]).
