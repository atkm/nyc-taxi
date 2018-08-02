import org.scalatest.Matchers._
import org.scalatest.FlatSpec

class DoubleToleranceTest extends FlatSpec {
  "A list of doubles" should "equal another list of doubles with the same entries" in {
    val tolerance = 0.1
    val xs = List(1.1, 2.2, 3.3, 4.4, 5.5)
    val ys = List(1.05, 2.3, 3.29, 4.41, 5.4)
    for ((x,y) <- xs zip ys) {
      assert(x === y +- tolerance)
    }
  }
  "A double" should "equal another double within tolerance" in {
    val a = 1.001
    val b = 1.0
    assert(a !== b +- 0.0005)
    assert(a === b +- 0.001)
    assert(a === b +- 0.005)
  }
}
