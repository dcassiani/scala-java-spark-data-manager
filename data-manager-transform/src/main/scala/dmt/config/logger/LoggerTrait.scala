package eqp.config.logger

import org.apache.log4j.Logger

trait LoggerTrait {

  val log = LoggerConfig.load(this.getClass)

}
