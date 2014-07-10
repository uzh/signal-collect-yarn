package com.signalcollect.logging

import akka.actor.Actor
import akka.event.Logging.InitializeLogger
import akka.event.Logging.LoggerInitialized
import akka.event.Logging.Error
import akka.event.Logging.Warning
import akka.event.Logging.Info
import akka.event.Logging.Debug

class AkkaLog4JAdapter extends Actor {
val log = new Log4JWrapper(this.getClass.getName)
def receive = {
    case InitializeLogger(_)                        => sender ! LoggerInitialized
    case Error(cause, logSource, logClass, message) => log.error(message.toString)
    case Warning(logSource, logClass, message)      => log.warn(message.toString)
    case Info(logSource, logClass, message)         => log.info(message.toString)
    case Debug(logSource, logClass, message)        => log.debug(message.toString)
  }
}