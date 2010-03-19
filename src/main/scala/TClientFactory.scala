package com.protose.telephos

import org.apache.cassandra.thrift.{Cassandra => TCassandra}
import org.apache.thrift.transport.TSocket
import org.apache.thrift.protocol.TBinaryProtocol

import Tap._

class TClientFactory {
  def create(host: String, port: Int): TCassandra.Client = {
    val socket    = new TSocket(host, port).tap { s => s.open }
    val protocol  = new TBinaryProtocol(socket)

    new TCassandra.Client(protocol)
  }
}
