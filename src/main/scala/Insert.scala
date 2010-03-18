package com.protose.telephos.spec

case class Insert(columnFamily: String, key: String, 
                  column:       String, value: Array[Byte]) {

}
