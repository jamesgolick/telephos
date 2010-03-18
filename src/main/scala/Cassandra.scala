package com.protose.telephos

import java.util.Date
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

object Mutation {
  def apply(columnFamily: String, 
            key:          String,
            superColumn:  String,
            column:       String,
            value:        String,
            timestamp:    Long) = {
    new Mutation(columnFamily, key, superColumn.getBytes,
                 column.getBytes, value.getBytes, timestamp)
  }

  def apply(columnFamily: String, 
            key:          String,
            column:       String,
            value:        String,
            timestamp:    Long) = {
    new Mutation(columnFamily, key, null, column.getBytes, value.getBytes, timestamp)
  }
}
case class Mutation(columnFamily: String, 
                    key:          String,
                    superColumn:  Array[Byte],
                    column:       Array[Byte],
                    value:        Array[Byte],
                    timestamp:    Long)


class Batch(val mutations: List[Mutation]) {
  def this() = this(List[Mutation]())

  def insert(columnFamily: String,
             key:          String, 
             columns:      Map[String, String]): Batch = {
             this
  }

//  def toThrift: JMap[String, JMap[String, JList[TMutation]]] = {
//    val map = new JHashMap[String, JMap[String, JList[TMutation]]]()
//    mutations.foreach { m =>
//      val key          = m.columnPath.key
//      val columnFamily = m.columnPath.columnFamily

//      if (map.get(key) == null) {
//        map.put(key, new JHashMap[String, JList[TMutation]]())
//      }

//      val keyMap = map.get(key)
//      if (keyMap.get(columnFamily) == null) {
//        keyMap.put(columnFamily, new JArrayList[TMutation]())
//      }

//      val cfMap    = keyMap.get(columnFamily)
//      val mutation = new TMutation
//      val column   = new Column
//      column.name  = m.columnPath.column
//      column.value = m.value

//      val columnOrSuperColumn        = new ColumnOrSuperColumn
//      columnOrSuperColumn.column     = column
//      mutation.column_or_supercolumn = columnOrSuperColumn
//      cfMap.add(mutation)
//    }
//    map
//  }
}

object Cassandra {
}

class Cassandra {
  import Cassandra._

  val socket    = new TSocket("localhost", 9160)
  val protocol  = new TBinaryProtocol(socket)
  val client    = new TCassandra.Client(protocol)
  
  socket.open

  def insert(keyspace: String, batch: Batch) = {
    //client.batch_mutate(keyspace, batch.toThrift, 1)
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
