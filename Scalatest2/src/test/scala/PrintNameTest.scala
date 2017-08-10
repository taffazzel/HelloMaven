import org.scalatest._


/**
  * Created by tafaz on 7/8/2017.
  */

@DoNotDiscover
class PrintNameTest extends FlatSpec with Matchers{
  "ScalaTestHello" should "divide 2 numbers" in {
    ScalaTestHello.divide(10,5) should be (2)
  }

  it should "throw ArithmaticException if attempted to divide by 0" in {
    a[java.lang.ArithmeticException] should be thrownBy {
      ScalaTestHello.divide(20,0)
    }
  }
/*
  it should "let adults vote" in {
    PrintName.canVote(18) should be (true)
  }
  it should "not let minor vote " in {
    PrintName.canVote(17) should be (false)
  }
*/

}
