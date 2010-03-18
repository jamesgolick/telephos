package com.protose.telephos.spec

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

object BatchSpec extends Specification with Mockito {
  val batch = new Batch

  "inserting in to a batch" in {
    val columns   = Map("name" -> "Fred", "age" -> "2")
    val mutations = batch.insert("Users", "1", columns).mutations

    "returns a new batch with a mutation added for each column in the map" in {
      mutations.size must_== 2

      val ageMutation = mutations(0)
      ageMutation.columnFamily must_== "Users"
      ageMutation.key must_== "1"
      new String(ageMutation.column) must_== "age"
      new String(ageMutation.value) must_== "2"

      val nameMutation = mutations(1)
      nameMutation.columnFamily must_== "Users"
      nameMutation.key must_== "1"
      new String(nameMutation.column) must_== "name"
      new String(nameMutation.value)  must_== "Fred"
    }
  }
}
