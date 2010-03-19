package com.protose.telephos

object TypeConversions {
  implicit def string2ByteArray(string: String): Array[Byte] = string.getBytes
  implicit def map2ColumnMap[A <% Array[Byte], B <% Array[Byte]](map: Map[A, B]):
    Map[Array[Byte], Array[Byte]] = {
    map.foldLeft(Map[Array[Byte], Array[Byte]]()) { case (map, (k, v)) =>
      val tuple = (k: Array[Byte], v: Array[Byte])
      map + tuple
    }
  }

  implicit def columnMap2Map[A, B](map: Map[Array[Byte], Array[Byte]])
    (implicit f: Array[Byte] => A, g: Array[Byte] => B): Map[A, B] = {
    map.foldLeft(Map[A, B]()) { case(map, (k, v)) => 
      val tuple = (k: A, v: B)
      map + tuple
    }
  }

  implicit def uuid2ByteArray(uuid: UUID): Array[Byte]  = uuid.asByteArray
  implicit def byteArray2UUID(bytes: Array[Byte]): UUID = new UUID(bytes)
}
