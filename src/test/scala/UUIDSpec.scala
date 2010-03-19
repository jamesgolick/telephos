package com.protose.telephos.spec

import org.specs.Specification
import org.specs.mock.Mockito
import org.mockito.Matchers._

import org.safehaus.uuid.{UUID => UnderlyingUUID}

object UUIDSpec extends Specification with Mockito {
  val bigUnderlying   = mock[UnderlyingUUID]
  val smallUnderlying = mock[UnderlyingUUID]
  val small           = new UUID(smallUnderlying)
  val big             = new UUID(bigUnderlying)

  "UUID ordering" in {
    bigUnderlying.compareTo(smallUnderlying) returns 1

    "is defined by the ordering of the underlying uuid" in {
      big.compare(small) must_== 1
    }
  }

  "it delegates to the underlying uuid for hashCode" in {
    big.hashCode must_== bigUnderlying.hashCode
  }

  "it delegates to the underlying uuid for its String representation" in {
    bigUnderlying.toString returns "abcdefg-abc-abc-abcdefg"
    big.toString must_== "abcdefg-abc-abc-abcdefg"
  }
}
