package scalaexperts.async

import akka.actor._
import scala.util.Try
import akka.routing.SmallestMailboxRouter
import akka.actor.SupervisorStrategy.Restart
import scala.concurrent.duration._
import scalaexperts.error.Retry

object AsyncThatThing{

  type Command[RESOURCE, RESULT] = RESOURCE => RESULT

  trait ResourceManager[RESOURCE] {
    def close(r: RESOURCE) : Try[Unit]
    final def open() : RESOURCE = retry(create)

    protected def create: RESOURCE
    protected def retry[T] : (=>T) => T = Retry.expotentialBackoff(10 millis, 5 seconds)
  }

  sealed trait Event
  case object ConnectionWentDown extends Event
  case object ResourceTemporarilyUnavailable extends Event
  case object ResourceAvailable extends Event
  case class ConnectionOpened(connections: Int) extends Event

  private case object ConnectionUp
  private case object ConnectionDown
  private case class InactiveForLong(since: Long)

  class Connection[RESOURCE](mgr: ResourceManager[RESOURCE], monitor: ActorRef) extends Actor{
    val resource = mgr.open()
    monitor ! ConnectionUp

    override def receive: Actor.Receive = {
      case (cmd: Command[RESOURCE, _], deadline: Deadline) if deadline.hasTimeLeft() =>
        sender ! cmd(resource)
    }

    override def preRestart(reason: Throwable, message: Option[Any]): Unit = {
      super.preRestart(reason, message)
      sender ! reason
      monitor ! ConnectionDown
      mgr close resource
    }

    override def postStop(): Unit = {
      super.postStop()
      mgr close resource
    }
  }

  case object ResourceDown
  case object ResourceUp

  class AsyncResource[R](mgr : ResourceManager[R], connections: Int,
                         dispatcher: String, timeout: FiniteDuration) extends Actor{

    private val monitor = context.actorOf(Props(new ResourceMonitor(self)))
    private val router = {
      val props = Props(new Connection[R](mgr, monitor))
        .withRouter(
          SmallestMailboxRouter(connections)
            .withSupervisorStrategy(OneForOneStrategy(){case _ => Restart})

        ) .withDispatcher(dispatcher)

      context.actorOf(props, "router")
    }

    override def receive: Actor.Receive = resourceIsUp


    private def resourceIsUp : Actor.Receive = {
      case ResourceDown =>
        log(ResourceDown)
        context.become(resourceIsDown)
      case cmd =>
        router.tell((cmd, Deadline.now + timeout), sender)
    }

    private def resourceIsDown : Actor.Receive = {
      case ResourceUp =>
        log(ResourceUp)
        context.become(resourceIsUp)
      case _ =>
        sender ! new RuntimeException(s"Resource temporarily unavailable")
    }

    private def log = context.system.eventStream.publish _

  }



  private class ResourceMonitor(listener: ActorRef) extends Actor{
    implicit val ex = context.dispatcher

    override def receive: Actor.Receive = inactive()

    private def inactive(since: Long = System.currentTimeMillis()) : Receive = {
      log(ConnectionWentDown)
      context.system.scheduler.scheduleOnce(3000 millis, self, InactiveForLong(since))

      {
        case ConnectionUp =>
          listener ! ResourceUp
          context.become(active(1))
        case InactiveForLong(s) if s == since =>
          context.become(inactiveForLongTime(since))
      }
    }

    private def active(activeCount: Int) : Receive = {
      case ConnectionUp =>
        log(ConnectionOpened(activeCount))
        context.become(active(activeCount+1))
      case ConnectionDown =>
        log(ConnectionWentDown)
        if( activeCount == 1) context.become(inactive())
        else context.become(active(activeCount-1))
    }


    private def inactiveForLongTime(since: Long) : Receive = {
      log(ResourceTemporarilyUnavailable)
      listener ! ResourceDown

      {
        case ConnectionUp =>
          log(ResourceAvailable)
          listener ! ResourceUp
          context.become(active(1))
      }
    }


    private def log(e :Event) = context.system.eventStream.publish(e)
  }


}








