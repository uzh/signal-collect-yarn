package com.signalcollect.util

import org.apache.log4j.Logger;

trait LogHelper {
    val loggerName = this.getClass.getName
    lazy val log = Logger.getLogger(loggerName)
}