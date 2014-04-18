
object Concordance {
    case class Word(word: String, sentence: Int)
}

case class Concordance(word: String, sentences: List[Int])
