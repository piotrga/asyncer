package scalaexperts.error

import scala.concurrent.duration.FiniteDuration
import scala.annotation.tailrec
import scala.util.Try

object Retry{
  def expotentialBackoff[T](initial: FiniteDuration, max: FiniteDuration) :  ( => T) => T =
    b => {
      require(initial.toMillis > 0, "Initial delay must be > 1 millisecond")

      @tailrec
      def retry(current: FiniteDuration) : T ={
        val res = Try{b}
        if (res.isFailure){ // this is crap but it makes it tail-recursive...
          println(s"[${Thread.currentThread().getName()}] retrying in ($current)")
          Thread.sleep(current.toMillis)
          val nextDelay = if( 2 * current > max) max else 2 * current
          retry(nextDelay)
        } else res.get
      }

      retry(initial)
    }
}

trait Retry{
  def apply[T](b : => T) : T
}