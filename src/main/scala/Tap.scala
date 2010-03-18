package com.protose.telephos

object Tap {
  implicit def any2Tappable[A](toTap: A): Tappable[A] = new Tappable(toTap)
}

class Tappable[A](toTap: A) {
  def tap(block: (A) => Unit): A = { block(toTap); toTap }
}
