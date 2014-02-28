package scalaexperts.async

import scala.Some
import akka.actor.{Actor, Props, ActorSystem}
import java.util.concurrent.atomic.AtomicReference
import scala.util.{Try, Random}
import scala.concurrent.duration._
import akka.util.Timeout
import akka.pattern.ask

object AsyncSimulation extends App{
  val sys = ActorSystem("gucio")
  import AsyncThatThing._

  val downUntil = new AtomicReference[Option[Deadline]](None)

  class TestResource{
    def doSomething() = downUntil.get match {
      case Some(deadline) if deadline.hasTimeLeft() => throw new RuntimeException("Connection is down")
      case _ =>
        Random.nextInt(1000) match {
          case r  if r>998  =>
            val howLongDown = ((Random.nextGaussian().abs * 5000).toInt + 5000).millis
            downUntil.set(Some(Deadline.now + howLongDown))
            println(s"Connection is going down for ($howLongDown)")
            throw new RuntimeException("connection is down")
          case r if r>990 =>
            throw new RuntimeException("Intermittent failure")
          case _ =>
            val duration = (Random.nextGaussian().abs * 2).toInt + 3
            Thread.sleep(duration)
          //            println(s"Doing the operation took = $duration ms")
        }
    }
  }

  class TestResourceMgr extends ResourceManager[TestResource]{
    override protected def create: TestResource = downUntil.get match {
      case Some(deadline) if deadline.hasTimeLeft() =>
        Thread.sleep(3000)
        throw new RuntimeException("can not instantiate resource")
      case _ =>
        Thread.sleep((Random.nextGaussian().abs * 3000).toInt + 100)
        new TestResource
    }

    override def close(r: TestResource): Try[Unit] = Try{}
  }


  val res = sys.actorOf(Props(new AsyncResource[TestResource](new TestResourceMgr, 5, "pinned", 4 seconds)), "TestResource")

  sys.actorOf(Props(new Actor{

    context.system.eventStream.subscribe(self, classOf[Event])

    override def receive: Actor.Receive = { case msg:Event => println(msg.toString) }
  }))
  //  type CW = CommandWrapper[TestResource, Unit]
  implicit val timeout = Timeout(3 seconds)
  for(i  <- 1 to 1000000000){
    res ? ((_:TestResource).doSomething())

    Thread.sleep(10)
  }
}
