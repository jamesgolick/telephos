package com.protose.telephos.spec

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

object ConnectionPoolSpec extends Specification with Mockito {
  val connection        = mock[Cassandra]
  val connectionTwo     = mock[Cassandra]
  val connectionFactory = mock[ConnectionFactory]
  val connectionPool    = new ConnectionPool(connectionFactory)

  connectionFactory.create returns connection thenReturns connectionTwo
  
  "checking out a connection" in {
    var yielded: Cassandra = null

    connectionPool.withConnection { c =>
      yielded = c
    }

    "creates a connection" in {
      connectionFactory.create was called
    }

    "yields the connection from the factory" in {
      yielded must_== connection
    }
  }

  "when a connection has already been created and returned to the pool" in {
    var firstYield: Cassandra  = null
    var secondYield: Cassandra = null

    connectionPool.withConnection { c => firstYield  = c }
    connectionPool.withConnection { c => secondYield = c }

    "it reuses the first connection" in {
      firstYield must_== connection
      secondYield must_== connection
    }

    "it only creates one total connection" in {
      connectionFactory.create was called.once
    }
  }

  "when a connection is out of the pool" in {
    var firstYield: Cassandra  = null
    var secondYield: Cassandra = null

    connectionPool.withConnection { c =>
      firstYield  = c 
      connectionPool.withConnection { c => secondYield = c }
    }

    "it gets another connection from the factory" in {
      firstYield must_== connection
      secondYield must_== connectionTwo
    }

    "it creates two connections" in {
      connectionFactory.create was called.twice
    }
  }
}
