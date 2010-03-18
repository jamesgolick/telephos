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

import scala.collection.SortedMap
import scala.collection.immutable.TreeMap

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

  def toSortedMap[A <: Ordered[A], B](keySlices: JList[KeySlice]):
    SortedMap[A, B] = {
    val newMap = Map[A, B]()
    val map    = keySlices.get(0).columns.asScala.foldLeft(newMap) { 
      (map, colOrSuper) =>
      val col      = colOrSuper.column
      val name: A  = col.name.asInstanceOf[A]
      val value: B = col.value.asInstanceOf[B]

      map + (name -> value)
    }
    new TreeMap[A, B]() ++ map
  }

  def makeColumnParent(columnFamily: String) = {
    new ColumnParent().tap { c => c.column_family = columnFamily }
  }

  def makeSlicePredicate(reversed: Boolean, count: Int): SlicePredicate = {
    new SlicePredicate().tap { sp =>
      sp.slice_range = makeSliceRange("".getBytes, "".getBytes, reversed, count)
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
