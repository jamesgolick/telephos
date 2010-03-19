package com.protose.telephos

import org.safehaus.uuid.UUID

object TypeConversions {
  implicit def string2ByteArray(string: String): Array[Byte] = string.getBytes
  implicit def map2ColumnMap[A <% Array[Byte], B <% Array[Byte]](map: Map[A, B]):
    Map[Array[Byte], Array[Byte]] = {
    map.foldLeft(Map[Array[Byte], Array[Byte]]()) { case (map, (k, v)) =>
      val tuple = (k: Array[Byte], v: Array[Byte])
      map + tuple
    }
  }

  implicit def uuid2ByteArray(uuid: UUID): Array[Byte] = uuid.asByteArray
}
