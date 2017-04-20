import scalaz._
val m1 = Map("white" -> (1, 1),
"black" -> (2, 2))
val m2 = Map("white" -> (3, 3),
  "black" -> (4, 4))
m1 |+| m2