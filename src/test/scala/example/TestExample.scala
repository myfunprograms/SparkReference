package example

// A class as a template for tests
class TestExample extends UnitBehavioralSpec {
  "One plus pne" should "Return 2" in {
    Given("Values")
    val num1 = 1
    val num2 = 1

    When("Add")
    val result = num1 + num2

    Then("Result is two")
    val expected = 2
    result shouldEqual expected
  }
}
