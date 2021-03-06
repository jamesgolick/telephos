package com.protose.telephos.spec

import java.util.{Map => JMap}
import java.util.{HashMap => JHashMap}
import java.util.{List => JList}
import java.util.{ArrayList => JArrayList}

import org.apache.cassandra.thrift.Column
import org.apache.cassandra.thrift.ColumnOrSuperColumn
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.KeyRange
import org.apache.cassandra.thrift.KeySlice
import org.apache.cassandra.thrift.{Mutation => TMutation}
import org.apache.cassandra.thrift.SlicePredicate
import org.apache.cassandra.thrift.SliceRange

import Tap._

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

object ConverterSpec extends Specification with Mockito {
  import TypeConversions._

  val converter = new Converter

  "converting a batch of mutations to a mutation_map" in {
    val colMutation   = new Mutation("Users", "key", null, "name", "value", 12345)
    val superMutation = new Mutation("Relations", "1", "super", "uuid", "", 12345)
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
  
  "converting a columnFamily and superColumn in to a ColumnParent" in {
    val superCol = "whatevers".getBytes
    val parent: ColumnParent = converter.makeColumnParent("Relations", superCol)

    "sets column_family to that cf" in {
      parent.column_family must_== "Relations"
    }

    "sets supercolumn to the column" in {
      parent.super_column must_== superCol
    }
  }

  "creating a slice predicate" in {
    val slicePredicate: SlicePredicate =
      converter.makeSlicePredicate("", "", false, 5)
    val sliceRange: SliceRange         =
      slicePredicate.slice_range
    
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

  "converting a java list of keyslices in to a sorted map of columns" in {
    "when some keyslices are returned" in {
      val name1   = "name".getBytes
      val value1  = "value".getBytes
      val name2   = "name2".getBytes
      val value2  = "value2".getBytes

      val columns  = new JArrayList[ColumnOrSuperColumn]().tap { cols =>
        cols.add(new ColumnOrSuperColumn().tap { cos =>
          cos setColumn new Column(name1, value1, 12345)
        })

        cols.add(new ColumnOrSuperColumn().tap { cos =>
          cos setColumn new Column(name2, value2, 12345)
        })
      }
      val keySlice = mock[KeySlice]
      val slices   = new JArrayList[KeySlice]().tap { l => l.add(keySlice) }

      keySlice.getColumns returns columns

      val map = converter.toMap(slices)

      "converts the structure into a sorted map" in {
        map(name1) must_== value1
        map(name2) must_== value2
      }
    }

    "when no keyslices are returned" in {
      val emptyMap = new JArrayList[KeySlice]()

      "it returns an empty map" in {
        converter.toMap(emptyMap) must_== Map[Array[Byte], Array[Byte]]()
      }
    }
  }

  "converting multiget results to a scala map" in {
    val name1   = "name".getBytes
    val value1  = "value".getBytes
    val name2   = "name2".getBytes
    val value2  = "value2".getBytes

    val results = new JHashMap[String, JList[ColumnOrSuperColumn]]
    val columns = new JArrayList[ColumnOrSuperColumn]().tap { cols =>
      cols.add(new ColumnOrSuperColumn().tap { cos =>
        cos setColumn new Column(name1, value1, 12345)
      })

      cols.add(new ColumnOrSuperColumn().tap { cos =>
        cos setColumn new Column(name2, value2, 12345)
      })
    }

    results.put("1", columns)
    results.put("2", columns)

    val map = converter.toMap(results)

    "maps key -> name/value map" in {
      map("1") must_== Map(name1 -> value1, name2 -> value2)
      map("2") must_== Map(name1 -> value1, name2 -> value2)
    }
  }
}
