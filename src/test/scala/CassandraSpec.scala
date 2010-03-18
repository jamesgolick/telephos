package com.protose.telephos.spec

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

import org.apache.cassandra.thrift.{Cassandra => TCassandra}
import org.apache.cassandra.thrift.{ColumnPath => TColumnPath}

object CassandraSpec extends Specification with Mockito {
  val client    = mock[TCassandra.Client]
  val keyspace  = "ActivityFeed"
  val cassandra = new Cassandra

  "inserting data" in {
    "maps the data to " in {
    }
  }
}
