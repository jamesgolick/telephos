package com.protose.telephos

import java.util.Date

object Mutation {
  def apply(columnFamily: String, 
            key:          String,
            superColumn:  Array[Byte],
            column:       Array[Byte],
            value:        Array[Byte]): Mutation = {
    Mutation(columnFamily, key, superColumn, column, value, mkTimestamp)
  }

  def apply(columnFamily: String, 
            key:          String,
            column:       Array[Byte],
            value:        Array[Byte]): Mutation = {
    Mutation(columnFamily, key, null, column, value, mkTimestamp)
  }


  protected def mkTimestamp = new Date().getTime
}

case class Mutation(columnFamily: String, 
                    key:          String,
                    superColumn:  Array[Byte],
                    column:       Array[Byte],
                    value:        Array[Byte],
                    timestamp:    Long)
