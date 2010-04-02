package com.protose.telephos

import java.util.{Map => JMap}
import java.util.{HashMap => JHashMap}
import java.util.{List => JList}
import java.util.{ArrayList => JArrayList}

import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import org.apache.cassandra.thrift.KeyRange
import org.apache.cassandra.thrift.KeySlice
import org.apache.cassandra.thrift.{Mutation => TMutation}
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange
import org.apache.cassandra.thrift.SuperColumn

import org.scala_tools.javautils.Imports._

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

  def toMap(keySlices: JList[KeySlice]): Map[Array[Byte], Array[Byte]] = {
    val columns = keySlices.get(0).getColumns.asScala
    columns.foldLeft(Map[Array[Byte], Array[Byte]]()) { (map, colOrSuper) =>
      val col = colOrSuper.column

      map + (col.name -> col.value)
    }
  }

  def toMap(results: JMap[String, JList[ColumnOrSuperColumn]]):
    Map[String, Map[Array[Byte], Array[Byte]]] = {
      val empty = Map[String, Map[Array[Byte], Array[Byte]]]()
      results.asScala.foldLeft(empty) { case (map, (k,v)) =>
        val emptyCols = Map[Array[Byte], Array[Byte]]()
        val columns   = v.asScala.map(_.column).foldLeft(emptyCols) { (map,column) =>
          map + (column.name -> column.value)
        }
        map + (k -> columns)
      }
  }

  def makeColumnParent(columnFamily: String): ColumnParent = {
    makeColumnParent(columnFamily, null)
  }

  def makeColumnParent(columnFamily:String, superColumn:Array[Byte]): ColumnParent={
    new ColumnParent().tap { c =>
      c.column_family = columnFamily
      c.super_column  = superColumn
    }
  }

  def makeSlicePredicate(start:    Array[Byte], finish: Array[Byte],
                         reversed: Boolean,     count:  Int): SlicePredicate = {
    new SlicePredicate().tap { sp =>
      sp.slice_range = makeSliceRange(start, finish, reversed, count)
    }
  }

  def makeSliceRange(start: Array[Byte], finish: Array[Byte],
                     reversed: Boolean, count: Int): SliceRange = {
    new SliceRange(start, finish, reversed, count)
  }

  def makeKeyRange(start: String): KeyRange = {
    new KeyRange().tap { range =>
      range.start_key = start
      range.end_key   = start
    }
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
