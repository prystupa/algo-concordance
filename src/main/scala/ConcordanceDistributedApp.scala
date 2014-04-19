import akka.actor._
import akka.actor.Terminated
import scala.Some

object ConcordanceDistributedApp extends App {

  val system = ActorSystem("ConcordanceSolverSystem")

  // kick-off simulation
  system.actorOf(Props(classOf[ConcordanceDistributedApp]))
}

class ConcordanceDistributedApp extends Actor with ActorLogging {

  import ConcordanceIndexerJob._

  val numberOfBatches = 1000
  val batchSize = 500
  val storage = context.actorOf(Props(classOf[ConcordanceStorage]))
  context watch storage
  val indexer = context.actorOf(Props(classOf[ConcordanceIndexerJob], storage))

  batches(numberOfBatches, batchSize)
    .zipWithIndex.map(zipped => BatchText(zipped._1, zipped._2))
    .foreach(indexer ! _)
  indexer ! EndOfInput

  override def receive: Receive = {
    case Terminated(`storage`) =>
      log.info("done storing all concordance data, terminating...")
      context.system.shutdown()
  }

  private def batches(numberOfBatches: Int, batchSize: Int): Stream[String] = {
    val batch = """Given an arbitrary text document written in English,
                  |write a program that will generate a concordance, i.e. an alphabetical list of all word occurrences,
                  |labeled with word frequencies.
                  |Bonus: label each word with the sentence numbers in which each occurrence appeared.
                  | """.stripMargin * batchSize

    Stream.continually(batch).take(numberOfBatches)
  }
}

class SentenceBoundaryJob extends Actor {

  import ConcordanceIndexerJob._

  private val solver = new ConcordanceSolverNlp

  override def receive: Receive = {
    case BatchText(text: String, batchNumber: Int) =>
      sender ! BatchSentences(solver.sentences(text), batchNumber)
      context stop self
  }
}

class TokenizerJob(next: Option[ActorRef]) extends Actor with ActorLogging {

  import ConcordanceIndexerJob._

  private val solver = new ConcordanceSolverNlp

  override def receive: Receive = {

    case BatchSentences(sentences: List[String], batchNumber: Int) =>
      log.debug("tokenizing batch #{} into words within sentences", batchNumber)

      val concordance: Map[String, Set[Int]] = sentences.zipWithIndex flatMap {
        case (sentence, index) => solver.words(sentence).map((_, index))
      } groupBy {
        case (word, _) => word
      } mapValues {
        case group => group.map {
          case (_, index) => index
        }.toSet
      }

      next.getOrElse(sender()) ! BatchConcordance(concordance, totalSentences = sentences.length, batchNumber = batchNumber)
      context stop self
  }
}

object ConcordanceIndexerJob {

  case class BatchText(text: String, batchNumber: Int)

  case class BatchSentences(sentences: List[String], batchNumber: Int)

  case class BatchConcordance(concordance: Map[String, Set[Int]], totalSentences: Int, batchNumber: Int)

  case object EndOfInput

}

class ConcordanceIndexerJob(storage: ActorRef) extends Actor with ActorLogging {

  import ConcordanceIndexerJob._

  private var activeJobs = 0
  private var endOfInput = false

  override def receive: Receive = {

    case batchText: BatchText =>
      log.debug("parsing batch #{} into sentences", batchText.batchNumber)

      val sentenceSplitter = addJob(Props(classOf[SentenceBoundaryJob]))
      val tokenizer = addJob(Props(classOf[TokenizerJob], Some(storage)))
      sentenceSplitter.tell(batchText, tokenizer)

    case EndOfInput =>
      endOfInput = true
      checkEndOfWork(decreaseJobs = false)

    case Terminated(_) => checkEndOfWork(decreaseJobs = true)
  }

  private def addJob(job: Props): ActorRef = {
    val actor = context.actorOf(job)
    activeJobs = activeJobs + 1
    context watch actor
    actor
  }

  private def checkEndOfWork(decreaseJobs: Boolean): Unit = {

    if (decreaseJobs) {
      activeJobs = activeJobs - 1
    }

    if (activeJobs == 0 && endOfInput) {
      log.debug("shutting down indexer and storage")
      context stop self
      storage ! PoisonPill
    }
  }
}

class ConcordanceStorage extends Actor with ActorLogging {

  import ConcordanceIndexerJob._
  import scala.collection.mutable

  private val storage = new mutable.HashMap[String, mutable.Set[Int]] with mutable.MultiMap[String, Int]
  private var nextBatchNumber = 0
  private var nextSentenceNumber = 0
  private val smallestBatchNumberOrdering = Ordering.Int.reverse
  private val queue = mutable.PriorityQueue[BatchConcordance]()(new Ordering[BatchConcordance] {
    override def compare(x: BatchConcordance, y: BatchConcordance): Int = smallestBatchNumberOrdering.compare(x.batchNumber, y.batchNumber)
  })

  override def receive: Receive = {
    case result: BatchConcordance =>
      log.debug("storing concordance for batch #{}", result.batchNumber)
      queue.enqueue(result)

      while (queue.headOption.exists(_.batchNumber == nextBatchNumber)) {
        val orderedResult = queue.dequeue()
        orderedResult.concordance.foreach {
          case (word, sentences) => sentences.foreach(number => storage.addBinding(word, number + nextSentenceNumber))
        }
        nextBatchNumber = nextBatchNumber + 1
        nextSentenceNumber = nextSentenceNumber + orderedResult.totalSentences
      }
  }
}
