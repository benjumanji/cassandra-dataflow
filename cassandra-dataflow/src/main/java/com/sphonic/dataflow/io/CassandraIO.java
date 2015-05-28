package com.sphonic.dataflow.io;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.ResultSetFuture;
import com.google.cloud.dataflow.sdk.Pipeline;
import com.google.cloud.dataflow.sdk.coders.Coder;
import com.google.cloud.dataflow.sdk.coders.SerializableCoder;
import com.google.cloud.dataflow.sdk.options.PipelineOptions;
import com.google.cloud.dataflow.sdk.transforms.Create;
import com.google.cloud.dataflow.sdk.transforms.DoFn;
import com.google.cloud.dataflow.sdk.transforms.ParDo;
import com.google.cloud.dataflow.sdk.transforms.PTransform;
import com.google.cloud.dataflow.sdk.transforms.View;
import com.google.cloud.dataflow.sdk.values.PCollection;
import com.google.cloud.dataflow.sdk.values.PCollectionView;
import com.google.cloud.dataflow.sdk.values.PDone;

public class CassandraIO {

  /**
   * Write sink for Cassandra.
   */
  public static class Write {

    public static Unbound hosts(String... hosts) {
      return new Unbound().hosts(hosts);
    }

    public static Unbound keyspace(String keyspace) {
      return new Unbound().keyspace(keyspace);
    }

    public static Unbound port(int port) {
      return new Unbound().port(port);
    }

    public static class Unbound {

      private final String[] _hosts;
      private final String _keyspace;
      private final int _port;

      Unbound() {
        _hosts = null;
        _keyspace = null;
        _port = 9042;
      }

      Unbound(String[] hosts, String keyspace, int port) {
        _hosts = hosts;
        _keyspace = keyspace;
        _port = port;
      }

      public <T extends CassandraBindable> Bound<T> bind(String query) {
        return new Bound<T>(_hosts, _keyspace, _port, query);
      }

      public Unbound hosts(String... hosts) {
        return new Unbound(hosts, _keyspace, _port);
      }

      public Unbound keyspace(String keyspace) {
        return new Unbound(_hosts, keyspace, _port);
      }

      public Unbound port(int port) {
        return new Unbound(_hosts, _keyspace, port);
      }
    }

    public static class Bound<T extends CassandraBindable>
        extends PTransform<PCollection<T>, PDone> implements Supplier<PTransform<PCollection<T>, PDone>> {

      private static final long serialVersionUID = 0;

      private final String[] _hosts;
      private final String _keyspace;
      private final int _port;
      private final String _query;

      Bound(String[] hosts, String keyspace, int port, String query) {
        _hosts = hosts;
        _keyspace = keyspace;
        _port = port;
        _query = query;
      }

      public String[] getHosts() {
        return _hosts;
      }

      public String getKeyspace() {
        return _keyspace;
      }

      public int getPort() {
        return _port;
      }

      public String getInsert() {
        return _query;
      }

      public PTransform<PCollection<T>, PDone> get() {
        return new Bound<T>(_hosts, _keyspace, _port, _query);
      }

      @SuppressWarnings("unchecked")
      public PDone apply(PCollection<T> input) {
        Pipeline p = input.getPipeline();

        CassandraWriteOperation<T> op = new CassandraWriteOperation<T>(this);

        Coder<CassandraWriteOperation<T>> coder =
          (Coder<CassandraWriteOperation<T>>)SerializableCoder.of(op.getClass());

        PCollection<CassandraWriteOperation<T>> opSingleton =
          p.apply(Create.<CassandraWriteOperation<T>>of(op)).setCoder(coder);

        final PCollectionView<CassandraWriteOperation<T>> opSingletonView =
          opSingleton.apply(View.<CassandraWriteOperation<T>>asSingleton());

        PCollection<Void> results = input.apply(ParDo.of(new DoFn<T, Void>() {

          private static final long serialVersionUID = 0;

          private CassandraWriter<T> writer = null;

          @Override
          public void processElement(ProcessContext c) throws Exception {
            if (writer == null) {
              CassandraWriteOperation<T> op = c.sideInput(opSingletonView);
              writer = op.createWriter();
            }

            try {
              writer.write(c.element());
            } catch (Exception e) {
              try {
                writer.flush();
              } catch (Exception ec) {

              }
              throw e;
            }
          }

          @Override
          public void finishBundle(Context c) throws Exception {
            if (writer != null)
              writer.flush();
          }
        }).withSideInputs(opSingletonView));

        PCollectionView<Iterable<Void>> voidView = results.apply(View.<Void>asIterable());

        opSingleton.apply(ParDo.of(new DoFn<CassandraWriteOperation<T>, Void>() {
          private static final long serialVersionUID = 0;

          @Override
          public void processElement(ProcessContext c) {
            CassandraWriteOperation<T> op = c.element();
            op.finalize();
          }

        }).withSideInputs(voidView));

        return new PDone();
      }
    }

    private static class CassandraWriteOperation<T extends CassandraBindable> implements java.io.Serializable {

      private static final long serialVersionUID = 0;

      private final String[] _hosts;
      private final int _port;
      private final String _keyspace;
      private final String _query;

      private transient Cluster _cluster;
      private transient Session _session;

      private synchronized Cluster getCluster() {
        if (_cluster == null) {
          _cluster = Cluster.builder()
            .addContactPoints(_hosts)
            .withPort(_port)
            .withoutMetrics()
            .withoutJMXReporting()
            .build();
        }

        return _cluster;
      }

      private synchronized Session getSession() {
        if (_session == null) {
          Cluster cluster = getCluster();
          _session = cluster.connect(_keyspace);
        }

        return _session;
      }

      public CassandraWriteOperation(Bound<T> bound) {
        _hosts = bound.getHosts();
        _port = bound.getPort();
        _keyspace = bound.getKeyspace();
        _query = bound.getInsert();
      }

      public CassandraWriter<T> createWriter() {
        return new CassandraWriter<T>(this, getSession(), _query);
      }

      public void finalize() {
        getSession().close();
        getCluster().close();
      }
    }

    private static class CassandraWriter<T extends CassandraBindable> {
      private static int BATCH_SIZE = 20000;

      private final CassandraWriteOperation _op;
      private final Session _session;
      private final List<ResultSetFuture> _results = new ArrayList<ResultSetFuture>();
      private final PreparedStatement _stmt;

      public CassandraWriter(CassandraWriteOperation op, Session session, String query) {
        _op = op;
        _session = session;
        _stmt = session.prepare(query);
      }

      public void flush() {
        for (ResultSetFuture result: _results) {
          result.getUninterruptibly();
        }
        _results.clear();
      }

      public CassandraWriteOperation getWriteOperation() {
        return _op;
      }

      public void write(T statement) {
        Object[] bindable = statement.toBindable();
        BoundStatement stmt = _stmt.bind(bindable);
        if (_results.size() >= BATCH_SIZE)
          flush();

        _results.add(_session.executeAsync(stmt));
      }
    }
  }
}


