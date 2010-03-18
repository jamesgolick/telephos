package com.protose.telephos.spec

import java.util.{Map => JMap}
import java.util.{HashMap => JHashMap}
import java.util.{List => JList}
import java.util.{ArrayList => JArrayList}

import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import org.apache.cassandra.thrift.{Mutation => TMutation}
import org.apache.cassandra.thrift.SuperColumn

class Converter {
  def toMutationMap(batch: Batch): JMap[String, JMap[String, JList[TMutation]]] = {
    val map = new JHashMap[String, JMap[String, JList[TMutation]]]()
    batch.mutations.foreach { m =>
      if (map.get(m.key) == null) {
        map.put(m.key, new JHashMap[String, JList[TMutation]]())
      }

      val keyMap = map.get(m.key)
        if (keyMap.get(m.columnFamily) == null) {
        keyMap.put(m.columnFamily, new JArrayList[TMutation]())
      }

      val cfMap                      = keyMap.get(m.columnFamily)
      val mutation                   = new TMutation
      mutation.column_or_supercolumn = toColumnOrSuperColumn(m)

      cfMap.add(mutation)
    }
    map
  }

  protected def toColumnOrSuperColumn(mutation: Mutation) = {
    mutation.superColumn match {
      case null => toColumn(mutation)
      case _    => toSuperColumn(mutation)
    }
  }

  protected def toColumn(mutation: Mutation) = {
    val column   = new Column
    column.name  = mutation.column
    column.value = mutation.value

    val columnOrSuperColumn    = new ColumnOrSuperColumn
    columnOrSuperColumn.column = column
    columnOrSuperColumn
  }

  protected def toSuperColumn(mutation: Mutation) = {
    val column     = new SuperColumn
    column.name    = mutation.superColumn
    column.columns = new JArrayList[Column]
    val col        = new Column(mutation.column, mutation.value, mutation.timestamp)
    column.columns.add(col)

    val columnOrSuperColumn    = new ColumnOrSuperColumn
    columnOrSuperColumn.super_column = column
    columnOrSuperColumn
  }
}
