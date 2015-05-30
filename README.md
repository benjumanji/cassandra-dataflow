Cassandra Connector
===================

Very rough work.

Usage:

```java
CassandraIO.Write.Unbound spec = CassandraIO.Write
  .hosts("localhost")
  .port(9042)
  .keyspace("paypoint");

  PTransform<PCollection<Row>, PDone>> sink = spec.<Row>bind();
  PCollection<Row> rows = ???;
  row.appy(sink);
```

Where `Row` is some type type that uses the Datastax persistence API (JPA like annotations).

## TODO

- Allow for many more cluster object options to be set.
- Use a static map to prevent excessive cluster / session creation per JVM
- Javadoc
- Allow for reading
- tunable batch size

Lots of work to be done. PRs welcome!

