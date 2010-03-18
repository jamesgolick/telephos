package com.protose.telephos

import java.util.Date

object Mutation {
  def apply(columnFamily: String, 
            key:          String,
            superColumn:  String,
            column:       String,
            value:        String,
            timestamp:    Long): Mutation = {
    Mutation(columnFamily, key, superColumn.getBytes,
                 column.getBytes, value.getBytes, timestamp)
  }

  def apply(columnFamily: String, 
            key:          String,
            column:       String,
            value:        String,
            timestamp:    Long): Mutation = {
    Mutation(columnFamily, key, null, column.getBytes, value.getBytes, timestamp)
  }

  def apply(columnFamily: String, 
            key:          String,
            column:       String,
            value:        String): Mutation = {
    Mutation(columnFamily, key, null, column.getBytes, value.getBytes, mkTimestamp)
  }

  def apply(columnFamily: String, 
            key:          String,
            superColumn:  String,
            column:       String,
            value:        String): Mutation = {
    Mutation(columnFamily, key, superColumn.getBytes,
                 column.getBytes, value.getBytes, mkTimestamp)
  }

  protected def mkTimestamp = new Date().getTime
}

case class Mutation(columnFamily: String, 
                    key:          String,
                    superColumn:  Array[Byte],
                    column:       Array[Byte],
                    value:        Array[Byte],
                    timestamp:    Long)
