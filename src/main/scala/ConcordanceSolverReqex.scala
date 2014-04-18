
object ConcordanceSolverRegex {

  def sentences(text: String): List[String] = {
    val r = """$|\n|\.+($|\n|\s+(?=[A-Z]))""".r
    r.split(text).toList.map(_.trim).filterNot(_.isEmpty)
  }

  def words(text: String): List[String] = {
    val r = """[\s,:]+""".r
    r.split(text).toList.map(_.toLowerCase)
  }
}

class ConcordanceSolverRegex {

  import Concordance._
  import ConcordanceSolverRegex._

  def solve(text: String): List[Concordance] = {
    sentences(text).zipWithIndex.flatMap({
      case (sentence, index) => words(sentence).map(Word(_, index + 1))
    }).groupBy(_.word).map({
      case (word, wordInSentence) => Concordance(word, wordInSentence.map(_.sentence))
    }).toList.sortBy(_.word)
  }
}
