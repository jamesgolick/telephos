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

import Tap._

class Converter {
  def toMutationMap(batch: Batch): JMap[String, JMap[String, JList[TMutation]]] = {
    val map = new JHashMap[String, JMap[String, JList[TMutation]]]()
    batch.mutations.foreach { m =>
      if (!map.containsKey(m.key)) {
        map.put(m.key, new JHashMap[String, JList[TMutation]]())
      }

      val keyMap = map.get(m.key)
      if (!keyMap.containsKey(m.columnFamily)) {
        keyMap.put(m.columnFamily, new JArrayList[TMutation]())
      }

      val cfMap    = keyMap.get(m.columnFamily)
      val mutation = new TMutation().tap {
        tm => tm.column_or_supercolumn = toColumnOrSuperColumn(m)
      }

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
    new ColumnOrSuperColumn().tap { c => c.column = toTColumn(mutation) }
  }

  protected def toSuperColumn(mutation: Mutation) = {
    val superCol = new SuperColumn().tap { c => 
      c.name     = mutation.superColumn
      c.columns  = new JArrayList[Column]().tap { l => l.add(toTColumn(mutation)) }
    }

    new ColumnOrSuperColumn().tap { c => c.super_column = superCol }
  }

  protected def toTColumn(mutation: Mutation) = {
    new Column(mutation.column, mutation.value, mutation.timestamp)
  }
}