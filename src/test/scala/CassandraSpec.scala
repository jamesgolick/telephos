package com.protose.telephos.spec

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

import java.util.{Map => JMap}
import java.util.{List => JList}

import org.apache.cassandra.thrift.{Cassandra => TCassandra}
import org.apache.cassandra.thrift.ColumnParent
import org.apache.cassandra.thrift.KeyRange
import org.apache.cassandra.thrift.{Mutation => TMutation}
import org.apache.cassandra.thrift.SlicePredicate

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

  "getting a row" in {
    val columnParent   = mock[ColumnParent]
    val slicePredicate = mock[SlicePredicate]
    val keyRange       = mock[KeyRange]

    "with all the options specified" in {
      converter.makeColumnParent("Users") returns columnParent
      converter.makeSlicePredicate(false, 10) returns slicePredicate
      converter.makeKeyRange("1") returns keyRange

      cassandra.get("Users", "1", false, 10)

      "converts to thrift data types and queries thrift" in {
        client.get_range_slices(keyspace, columnParent,
          slicePredicate, keyRange, 1) was called
      }
    }

    "with only the cf and key specified" in {
      converter.makeColumnParent("Users") returns columnParent
      converter.makeSlicePredicate(false, 100) returns slicePredicate
      converter.makeKeyRange("1") returns keyRange

      cassandra.get("Users", "1")

      "converts to thrift data types and queries thrift" in {
        client.get_range_slices(keyspace, columnParent,
          slicePredicate, keyRange, 1) was called
      }
    }
  }
}
