Cassandra Connector
===================

Very rough work.

Usage:

```java
CassandraIO.Write.Unbound spec = CassandraIO.Write
  .hosts("localhost")
  .port(9042)
  .keyspace("paypoint");
   
  PTransform<PCollection<Row>, PDone>> sink = spec.<Row>bind("i'm a cql query");
  PCollection<Row> rows = ???;
  row.appy(sink);
```

Where `Row` is some type implementing `CassandraBindable`. The idea is that the type supplies an Array of objects suitable for binding to a prepared statement. You make a connection "spec", bind a query to that spec, producing a prepared statement. You supply a suitably typed collection, and away it will go.

## TODO

- Allow for many more cluster object options to be set.
- Use a static map to prevent excessive cluster / session creation per JVM
- Javadoc
- Allow for reading
- Better interplay between Row types and queries
- phantom (webdudos/phantom) support?
- tunable batch size

Lots of work to be done. PRs welcome!

