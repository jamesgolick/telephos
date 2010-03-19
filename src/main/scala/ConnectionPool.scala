package com.protose.telephos

import scala.collection.mutable.Queue

class ConnectionPool(connectionFactory: ConnectionFactory, 
                     connections:       Queue[Cassandra]) {
  def this(connectionFactory: ConnectionFactory) = {
    this(connectionFactory, new Queue[Cassandra])
  }

  def this(keyspace: String, host: String, port: Int) = {
    this(new ConnectionFactory(keyspace, host, port), new Queue[Cassandra])
  }

  def this(keyspace: String, host: String) = {
    this(keyspace, host, 9160)
  }

  def withConnection[A](f: Cassandra => A): A = {
    val connection = get
    try {
      return f(connection)
    } finally {
      put(connection)
    }
  }

  protected def get: Cassandra = {
    synchronized(connections) {
      return connections.size match {
        case 0 => connectionFactory.create
        case _ => connections.dequeue
      }
    }
  }

  protected def put(connection: Cassandra) = {
    synchronized(connections) {
      connections += connection
      0
    }
  }
}
