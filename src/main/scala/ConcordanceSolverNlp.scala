import java.io.ByteArrayInputStream
import opennlp.tools.dictionary.Dictionary
import opennlp.tools.sentdetect.{SentenceSampleStream, SentenceDetectorFactory, SentenceDetectorME, SentenceModel}
import opennlp.tools.tokenize.{TokenizerFactory, TokenSampleStream, TokenizerModel, TokenizerME}
import opennlp.tools.util.{PlainTextByLineStream, TrainingParameters}

object ConcordanceSolverNlp {
  private val eosCharacters = Array('.', '!', '?')
  private val punctuation = "," :: ":" :: eosCharacters.map(_.toString).toList
  private val sentenceModel = trainSentenceBoundary()
  private val tokenizerModel = trainTokenizer()

  private def trainSentenceBoundary(): SentenceModel = {
    val samples =
      """
        |Simple English sentence.
        |This one is not simple, i.e. has dots that are not end of sentence.
        |
        |Simple English sentence.
        |This one is not simple, i.e. has dots that are not end of sentence.
        |
        |Simple English sentence.
        |This one is not simple, i.e. has dots that are not end of sentence.
        |
        |Simple English sentence.
        |This one is not simple, i.e. has dots that are not end of sentence.
        |
        |Simple English sentence.
        |This one is not simple, i.e. has dots that are not end of sentence.
      """.stripMargin

    val lineStream = new PlainTextByLineStream(new ByteArrayInputStream(samples.getBytes), "UTF-8")
    val sampleStream = new SentenceSampleStream(lineStream)

    SentenceDetectorME.train("en", sampleStream,
      new SentenceDetectorFactory("en", true, new Dictionary(), eosCharacters),
      TrainingParameters.defaultParams())
  }

  private def trainTokenizer(): TokenizerModel = {
    val samples =
      """
        |Pierre Vinken<SPLIT>, 61 years old<SPLIT>, will join the board as a nonexecutive director Nov. 29<SPLIT>.
        |Mr. Vinken is chairman of Elsevier N.V.<SPLIT>, the Dutch publishing group<SPLIT>.
        |Rudolph Agnew<SPLIT>, 55 years old and former chairman of Consolidated Gold Fields PLC<SPLIT>,
        |    was named a nonexecutive director of this British industrial conglomerate<SPLIT>.
        |They only gave me one reason<SPLIT>: nothing is guaranteed<SPLIT>.
        |As for me I gave them three choices<SPLIT>: take my offer<SPLIT>, ignore it<SPLIT>, or do nothing<SPLIT>.
        |Sample colors<SPLIT>: red<SPLIT>, green<SPLIT>. Sample metals<SPLIT>: gold and silver<SPLIT>.
        |They only gave me one reason<SPLIT>: nothing is guaranteed<SPLIT>.
        |As for me I gave them three choices<SPLIT>: take my offer<SPLIT>, ignore it<SPLIT>, or do nothing<SPLIT>.
        |Sample colors<SPLIT>: red<SPLIT>, green<SPLIT>. Sample metals<SPLIT>: gold and silver<SPLIT>.
        |They only gave me one reason<SPLIT>: nothing is guaranteed<SPLIT>.
        |As for me I gave them three choices<SPLIT>: take my offer<SPLIT>, ignore it<SPLIT>, or do nothing<SPLIT>.
        |Sample colors<SPLIT>: red<SPLIT>, green<SPLIT>. Sample metals<SPLIT>: gold and silver<SPLIT>.
        |Some words<SPLIT>, i.e. one and two<SPLIT>, do not end with dot<SPLIT>.
        |Some numbers<SPLIT>, i.e. one and two<SPLIT>, do not end with dot<SPLIT>.
        |Some words<SPLIT>, i.e. one and two<SPLIT>, do not end with dot<SPLIT>.
        |Some numbers<SPLIT>, i.e. one and two<SPLIT>, do not end with dot<SPLIT>.
        |Some words<SPLIT>, i.e. one and two<SPLIT>, do not end with dot<SPLIT>.
        |Something<SPLIT>, i.e. one and two<SPLIT>, do not end with dot<SPLIT>.
        |Write a program that will generate a concordance<SPLIT>, i.e. an alphabetical list of all word occurrences<SPLIT>.
      """.stripMargin

    val lineStream = new PlainTextByLineStream(new ByteArrayInputStream(samples.getBytes), "UTF-8")
    val sampleStream = new TokenSampleStream(lineStream)

    TokenizerME.train(sampleStream, new TokenizerFactory("en", new Dictionary(), false, null), TrainingParameters.defaultParams())
  }

  private case class Word(word: String, sentence: Int)

}

class ConcordanceSolverNlp {

  import ConcordanceSolverNlp._

  private lazy val detector = new SentenceDetectorME(sentenceModel)
  private lazy val tokenizer = new TokenizerME(tokenizerModel)

  def sentences(text: String): List[String] = {
    detector.sentDetect(text).toList
  }

  def words(text: String): List[String] = {
    tokenizer.tokenize(text).toList.filterNot(punctuation.contains(_)).map(_.toLowerCase)
  }

  def solve(text: String): List[Concordance] = {
    sentences(text).zipWithIndex.flatMap({
      case (sentence, index) => words(sentence).map(Word(_, index + 1))
    }).groupBy(_.word).map({
      case (word, wordInSentence) => Concordance(word, wordInSentence.map(_.sentence))
    }).toList.sortBy(_.word)
  }
}
