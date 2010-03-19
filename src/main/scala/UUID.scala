package com.protose.telephos

import org.safehaus.uuid.{UUID => UnderlyingUUID}
import org.safehaus.uuid.UUIDGenerator

class UUID(protected val underlying: UnderlyingUUID) extends Ordered[UUID] {
  def this()   = this(UUIDGenerator.getInstance.generateTimeBasedUUID)
  override def compare(other: UUID): Int = underlying compareTo other.underlying
  override def toString = underlying.toString
  override def hashCode = underlying.hashCode
}
