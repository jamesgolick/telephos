package com.protose.telephos

class Batch(val mutations: List[Mutation]) {
  def this() = this(List[Mutation]())

  def insert(columnFamily: String,
             key:          String, 
             columns:      Map[String, String]): Batch = {
    new Batch(mutations ++ columns.foldLeft(List[Mutation]()) { (mutations, kv) =>
      Mutation(columnFamily, key, kv._1, kv._2) :: mutations
    })
  }
}
