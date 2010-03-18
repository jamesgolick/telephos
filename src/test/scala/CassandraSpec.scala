package com.protose.telephos.spec

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

import java.util.{Map => JMap}
import java.util.{List => JList}

import org.apache.cassandra.thrift.{Cassandra => TCassandra}
import org.apache.cassandra.thrift.{Mutation => TMutation}

object CassandraSpec extends Specification with Mockito {
  val client    = mock[TCassandra.Client]
  val converter = mock[Converter]
  val keyspace  = "ActivityFeed"
  val cassandra = new Cassandra(keyspace, client, converter)

  "inserting a batch" in {
    val batch       = mock[Batch]
    val mutationMap = mock[JMap[String, JMap[String, JList[TMutation]]]]

    converter.toMutationMap(batch) returns mutationMap
    cassandra.insert(batch)

    "converts the batch and passes it to the client with default consistency" in {
      client.batch_mutate(keyspace, mutationMap, 1) was called
    }
  }
}
