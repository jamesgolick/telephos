package com.protose.telephos

import org.safehaus.uuid.{UUID => UnderlyingUUID}
import org.safehaus.uuid.UUIDGenerator

@serializable
class UUID(protected val underlying: UnderlyingUUID) extends Ordered[UUID] {
  def this() = this(UUIDGenerator.getInstance.generateTimeBasedUUID)

  def this(string: String)     = this(new UnderlyingUUID(string))
  def this(bytes: Array[Byte]) = this(new UnderlyingUUID(bytes))

  override def compare(other: UUID): Int = underlying compareTo other.underlying
  override def toString: String          = underlying.toString
  override def hashCode: Int             = underlying.hashCode

  def equals(other: UUID): Boolean       = underlying.equals(other.underlying)
  def asByteArray: Array[Byte]           = underlying.asByteArray
}
