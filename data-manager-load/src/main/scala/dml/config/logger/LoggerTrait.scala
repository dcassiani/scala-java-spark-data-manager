package dml.config.logger

import org.apache.log4j.Logger

trait LoggerTrait {

  val log: Logger = LoggerConfig.load(this.getClass)

}
