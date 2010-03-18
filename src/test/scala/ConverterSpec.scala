package com.protose.telephos.spec

import java.util.{Map => JMap}
import java.util.{List => JList}

import org.apache.cassandra.thrift.{Mutation => TMutation}

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

object ConverterSpec extends Specification with Mockito {
  "converting a batch of mutations to a mutation_map" in {
    val colMutation   = Mutation("Users", "key", "name", "value", 12345)
    val superMutation = Mutation("Relations", "1", "super", "uuid", "", 12345)
    val mutations     = List[Mutation](colMutation, superMutation)
    val batch         = new Batch(mutations)
    val converter     = new Converter
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
}
