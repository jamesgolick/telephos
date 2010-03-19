package com.protose.telephos

class ConnectionFactory(keyspace: String, host:          String,
                        port:     Int,    clientFactory: TClientFactory) {
  def this(keyspace: String, host: String, port: Int) = {
    this(keyspace, host, port, new TClientFactory)
  }

  def this(keyspace: String, host: String) = {
    this(keyspace, host, 9160)
  }

  def create: Cassandra = {
    new Cassandra(keyspace, clientFactory.create(host, port))
  }
}
