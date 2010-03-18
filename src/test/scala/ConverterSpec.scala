package com.protose.telephos.spec

import java.util.{Map => JMap}
import java.util.{List => JList}

import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.KeyRange
import org.apache.cassandra.thrift.{Mutation => TMutation}
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

object ConverterSpec extends Specification with Mockito {
  val converter = new Converter

  "converting a batch of mutations to a mutation_map" in {
    val colMutation   = Mutation("Users", "key", "name", "value", 12345)
    val superMutation = Mutation("Relations", "1", "super", "uuid", "", 12345)
    val mutations     = List[Mutation](colMutation, superMutation)
    val batch         = new Batch(mutations)
    val map           = converter.toMutationMap(batch)

    "it creates a map of key -> cf -> list<mutation>" in {
      map.get("key") must haveSuperClass[JMap[String, JList[TMutation]]]
      map.get("key").get("Users") must haveSuperClass[JList[TMutation]]

      map.get("1") must haveSuperClass[JMap[String, JList[TMutation]]]
      map.get("1").get("Relations") must haveSuperClass[JList[TMutation]]
    }

    "it sets up column mutations correctly" in {
      val column = map.get("key").get("Users").get(0).column_or_supercolumn.column
      new String(column.name) must_== "name"
      new String(column.value) must_== "value"
    }

    "it sets up supercolumn mutations correctly" in {
      val superc = map.get("1").get("Relations").get(0).
                    column_or_supercolumn.super_column
      val column = superc.columns.get(0)

      new String(superc.name) must_== "super"
      new String(column.name) must_== "uuid"
      new String(column.value) must_== ""
    }
  }

  "converting a columnFamily in to a ColumnParent" in {
    val parent: ColumnParent = converter.makeColumnParent("Users")

    "creates a ColumnParent with column_family set to that cf" in {
      parent.column_family must_== "Users"
    }
  }

  "creating a slice predicate with no col range" in {
    val slicePredicate: SlicePredicate = converter.makeSlicePredicate(false, 5)
    val sliceRange: SliceRange         = slicePredicate.slice_range
    
    "has an empty start" in {
      new String(sliceRange.start) must_== ""
    }

    "has an empty finish" in {
      new String(sliceRange.finish) must_== ""
    }

    "sets the reversed parameter" in {
      sliceRange.reversed must beFalse
    }

    "sets the count parameter" in {
      sliceRange.count must_== 5
    }
  }

  "creating a key range with a single key" in {
    val keyRange: KeyRange = converter.makeKeyRange("1")

    "it has a start_key of key" in {
      keyRange.start_key must_== "1"
    }

    "it has a end_key of key" in {
      keyRange.end_key must_== "1"
    }
  }
}
