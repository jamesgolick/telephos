package com.protose.telephos

import java.util.{Map => JMap}
import java.util.{HashMap => JHashMap}
import java.util.{List => JList}
import java.util.{ArrayList => JArrayList}

import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.cassandra.thrift.{Cassandra => TCassandra}
import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.{ColumnPath => TColumnPath}
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import org.apache.cassandra.thrift.KeyRange
import org.apache.cassandra.thrift.{Mutation => TMutation}
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange

class Cassandra(val keyspace: String,
                client:       TCassandra.Client,
                converter:    Converter) {
  def insert(batch: Batch) = {
    client.batch_mutate(keyspace, converter.toMutationMap(batch), 1)
  }

  //def get(keyspace: String, columnPath: ColumnPath) = {
  //  val parent            = new ColumnParent
  //  parent.column_family  = columnPath.columnFamily

  //  val sliceRange        = new SliceRange("".getBytes, "".getBytes, false, 100)
  //  val predicate         = new SlicePredicate
  //  predicate.slice_range = sliceRange

  //  val keyRange          = new KeyRange
  //  keyRange.start_key    = columnPath.key
  //  keyRange.end_key      = columnPath.key
  //  keyRange.count        = 1

  //  client.get_range_slices(keyspace, parent, predicate, keyRange, 1)
  //}
}
