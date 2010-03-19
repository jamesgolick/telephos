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
import org.apache.cassandra.thrift.KeySlice
import org.apache.cassandra.thrift.{Mutation => TMutation}
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange

object Cassandra {
  def apply(keyspace: String): Cassandra = {
    val socket    = new TSocket("localhost", 9160)
    val protocol  = new TBinaryProtocol(socket)
    val client    = new TCassandra.Client(protocol)

    socket.open
    
    new Cassandra(keyspace, client, new Converter)
  }
}

class Cassandra(val keyspace: String,
                client:       TCassandra.Client,
                converter:    Converter) {

  def insert(batch: Batch) = {
    client.batch_mutate(keyspace, converter.toMutationMap(batch), 1)
  }

  def get(columnFamily: String, key: String,
          reversed: Boolean, limit: Int): Map[Array[Byte], Array[Byte]] = {
    val parent    = converter.makeColumnParent(columnFamily)
    val predicate = converter.makeSlicePredicate(reversed, limit)
    val keyRange  = converter.makeKeyRange(key)
    val list      = client.get_range_slices(keyspace, parent, predicate, keyRange, 1)

    converter.toMap(list)
  }

  def get(columnFamily: String, key: String): Map[Array[Byte], Array[Byte]] = {
    get(columnFamily, key, false, 100)
  }

  def getSuper(columnFamily: String, key: String, superColumn: Array[Byte],
          reversed: Boolean, limit: Int): Map[Array[Byte], Array[Byte]] = {
    val parent    = converter.makeColumnParent(columnFamily, superColumn)
    val predicate = converter.makeSlicePredicate(reversed, limit)
    val keyRange  = converter.makeKeyRange(key)
    val list      = client.get_range_slices(keyspace, parent, predicate, keyRange, 1)

    converter.toMap(list)
  }
}
