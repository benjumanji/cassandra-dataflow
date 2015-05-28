package com.sphonic.dataflow.io;

/**
 * Provide variables suitable for binding to a prepared statement.
 */
public interface CassandraBindable {

  /**
   * Return a collection of fields suitable for binding to a CQL stmt.
   */
  Object[] toBindable();
}

