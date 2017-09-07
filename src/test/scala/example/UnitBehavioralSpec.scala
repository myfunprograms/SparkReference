package example

import com.typesafe.scalalogging.LazyLogging
import org.scalatest.{BeforeAndAfterAll, FlatSpec, GivenWhenThen, Matchers}

trait UnitBehavioralSpec extends FlatSpec
  with GivenWhenThen with BeforeAndAfterAll with Matchers with LazyLogging {
}