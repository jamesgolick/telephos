package com.protose.telephos

object TypeConversions {
  type ColumnMap = Map[Array[Byte], Array[Byte]]

  implicit def string2ByteArray(string: String): Array[Byte] = string.getBytes
  implicit def map2ColumnMap[A <% Array[Byte], B <% Array[Byte]](map: Map[A, B]): ColumnMap = {
    map.foldLeft(Map[Array[Byte], Array[Byte]]()) { (map, kv) =>
      map + Tuple2[Array[Byte], Array[Byte]](kv._1, kv._2)
    }
  }
}
