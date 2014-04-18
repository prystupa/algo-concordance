
import org.scalatest.{Matchers, path}

class ConcordanceSolverRegexSpec extends path.FunSpec with Matchers {

  import ConcordanceSolverRegex._

  describe("Sentences splitter helper") {
    it("treats a single word as a sentence") {
      sentences("hello") should equal(List("hello"))
    }

    it("treats multiple white space separated words as a sentence") {
      sentences("hello world") should equal(List("hello world"))
    }

    it("treats . as and end of sentence") {
      sentences("hello world.") should equal(List("hello world"))
    }

    it("treats multiple . as and end of sentence") {
      sentences("hello world...") should equal(List("hello world"))
    }

    it("treats words separated by . followed by white space *and* upper case letter as different sentences") {
      sentences("Hello world. Hope all is well") should equal(List("Hello world", "Hope all is well"))
    }

    it("treats words separated by multiple . followed by space as different sentences") {
      sentences("Hello world... Hope all is well") should equal(List("Hello world", "Hope all is well"))
    }

    it("does not treat . followed by non-whitespace as an end of sentence") {
      sentences("hello from google.com") should equal(List("hello from google.com"))
    }

    it("treats words separated by new line as separate sentences") {
      val text =
        """hello world
           hope all is well""".replace("\r", "")
      sentences(text) should equal(List("hello world", "hope all is well"))
    }

    it("treats words separated by multiple new line as separate sentences") {
      val text =
        """hello world

           hope all is well""".replace("\r", "")
      sentences(text) should equal(List("hello world", "hope all is well"))
    }

    it("handles complex example") {
      val text =
        """Given an arbitrary text document written in English, write a program that will generate a concordance, i.e. an alphabetical list of all word occurrences, labeled with word frequencies.
Bonus: label each word with the sentence numbers in which each occurrence appeared.""".replace("\r", "")
      sentences(text) should equal(List(
        "Given an arbitrary text document written in English, write a program that will generate a concordance, i.e. an alphabetical list of all word occurrences, labeled with word frequencies",
        "Bonus: label each word with the sentence numbers in which each occurrence appeared"
      ))
    }
  }

  describe("Words splitter helper") {

    it("splits the words separated by whitespace") {
      words("hello world") should equal(List("hello", "world"))
    }

    it("strips punctuation form words") {
      words("hello, world") should equal(List("hello", "world"))
      words("reason: none") should equal(List("reason", "none"))
    }

    it("does not treat . as delimiter") {
      words("i.e. not delimiter") should equal(List("i.e.", "not", "delimiter"))
    }
  }

  describe("Concordance solver") {
    val solver = new ConcordanceSolverRegex

    it("handles a single word appearing 1 time") {
      solver.solve("a") should equal(List(Concordance("a", List(1))))
    }

    it("handles a single word appearing multiple times in different sentences") {
      solver.solve("A in first. A in second.") should equal(List(
        Concordance("a", List(1, 2)),
        Concordance("first", List(1)),
        Concordance("in", List(1, 2)),
        Concordance("second", List(2))
      ))
    }

    it("handles complex example") {
      val text =
        """Given an arbitrary text document written in English, write a program that will generate a concordance, i.e. an alphabetical list of all word occurrences, labeled with word frequencies.
Bonus: label each word with the sentence numbers in which each occurrence appeared.""".replace("\r", "")

      solver.solve(text) should equal(List(
        Concordance("a", List(1, 1)),
        Concordance("all", List(1)),
        Concordance("alphabetical", List(1)),
        Concordance("an", List(1, 1)),
        Concordance("appeared", List(2)),
        Concordance("arbitrary", List(1)),
        Concordance("bonus", List(2)),
        Concordance("concordance", List(1)),
        Concordance("document", List(1)),
        Concordance("each", List(2, 2)),
        Concordance("english", List(1)),
        Concordance("frequencies", List(1)),
        Concordance("generate", List(1)),
        Concordance("given", List(1)),
        Concordance("i.e.", List(1)),
        Concordance("in", List(1, 2)),
        Concordance("label", List(2)),
        Concordance("labeled", List(1)),
        Concordance("list", List(1)),
        Concordance("numbers", List(2)),
        Concordance("occurrence", List(2)),
        Concordance("occurrences", List(1)),
        Concordance("of", List(1)),
        Concordance("program", List(1)),
        Concordance("sentence", List(2)),
        Concordance("text", List(1)),
        Concordance("that", List(1)),
        Concordance("the", List(2)),
        Concordance("which", List(2)),
        Concordance("will", List(1)),
        Concordance("with", List(1, 2)),
        Concordance("word", List(1, 1, 2)),
        Concordance("write", List(1)),
        Concordance("written", List(1))
      ))
    }
  }
}

