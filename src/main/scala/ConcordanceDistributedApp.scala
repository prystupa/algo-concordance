import akka.actor._
import akka.actor.Terminated

/** Application entry point for simulated distributed concordance computation for large volume of text
  *
  * Application uses Akka to manage and distribute work.
  * All worker actors in the simulation are local. In real deployment those are usually configured to be drawn from
  * distributed pool utilizing different nodes.
  * Application starts up Akka system and kicks off the main "application" actor
  */

object ConcordanceDistributedApp extends App {

  val system = ActorSystem("ConcordanceSolverSystem")

  // kick-off simulation
  system.actorOf(Props(classOf[ConcordanceDistributedApp]))
}

/** Main actor of the simulation application
  *
  * Creates and monitors two top level workers - indexer and storage. Application reads large text batches from a
  * simulated location and submits them to indexer. Batches are numbered sequentially so it is possible to reconcile
  * their processing results when they are out of order. Indexer is configured with a `storage` worker to persist the
  * results of processing the text (accordance data). Application watches the storage worker - when storage worker
  * is done - application shuts down.
  * The number of batches can be controlled by `numberOfBatches` switch. The size of each batch can be set via
  * `batchSize`, the default 500 sets each batch to have about 1,000 sentences in it.
  */
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

/** This job splits a given batch of text into sentences.
  *
  * Given English text splits it into separate sentences using NLP sentence boundary solver.
  * Replies to the `sender` with results and terminates itself to signal the job is done. The sender is usually
  * configured to be the next job in the pipeline, i.e. word tokenizer
  */
class SentenceBoundaryJob extends Actor {

  import ConcordanceIndexerJob._

  private val solver = new ConcordanceSolverNlp

  override def receive: Receive = {
    case BatchText(text: String, batchNumber: Int) =>
      sender ! BatchSentences(solver.sentences(text), batchNumber)
      context stop self
  }
}

/** This job splits given sentences into words
  *
  * Given a set of sentences, splits them into words using NLP tokenizer solver.
  * After splitting into words computes "local" concordance. This local concordance
  * is relative to the batch, so sentence numbers need to be adjusted later when batch results are sorted to
  * recreate sentences order in the original text.
  *
  * @param next - next worker in the pipeline, usually storage. If not specified, replies with results to `sender`
  */
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

/** Major messages exchanged by distributed jobs to compute concordance
  *
  * BatchText - raw text for a given batch
  * BatchSentences - given batch split into sentences
  * BatchConcordance - computed concordance, sentence numbers are local to the batch. Total number of sentences in the
  * batch is included so that final reducer is able to recompute sentence numbers relative to original full text.
  * EndOfInput - signals that the last batch for a given large text has been submitted and there will be no more
  * batches after it
  */
object ConcordanceIndexerJob {

  case class BatchText(text: String, batchNumber: Int)

  case class BatchSentences(sentences: List[String], batchNumber: Int)

  case class BatchConcordance(concordance: Map[String, Set[Int]], totalSentences: Int, batchNumber: Int)

  case object EndOfInput

}

/** Main job coordinating other jobs in the distributed computation
  *
  * On receiving a batch of raw text this indexer spins up two other distributed jobs to process it: sentence splitter
  * and word tokenizer. It then wires the pipeline such that output of sentence splitter is going to tokenizer, and
  * tokenizer's output is sent directly to storage for final reduction and persistence.
  * Indexer also monitors all spun up jobs and terminates itself after processing is completed. It also gracefully
  * terminate the storage job.
  *
  * @param storage this is the job to persist final batch processing results
  */
class ConcordanceIndexerJob(storage: ActorRef) extends Actor with ActorLogging {

  import ConcordanceIndexerJob._

  private var activeJobs = 0
  private var endOfInput = false

  override def receive: Receive = {

    case batchText: BatchText =>
      log.debug("parsing batch #{} into sentences", batchText.batchNumber)

      val sentenceSplitter = addJob(Props(classOf[SentenceBoundaryJob]))
      val tokenizer = addJob(Props(classOf[TokenizerJob], Some(storage))) // results of tokenizer go to storage
      sentenceSplitter.tell(batchText, tokenizer) // results of sentence splitter go to tokenizer

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

/** This is the job to persist individual batch processing results
  *
  * This job simulates storage by writing computed results to in-memory `storage` hash table. In real deployment
  * scenarios though this job would write to some persistent storage or distributed cache (e.g. MongoDB, Cassandra,
  * Redis, relational database) designed to meet specific application needs.
  * This job also handles batch results that, in a distributed environment, can arrive out of order. If batch results
  * arrive in order they are stored immediately, if not - they are queued until the order can be reconstructed.
  * The reason we need to process batch results in order is to reconstruct sentence numbers in the original full text.
  * This implementation uses heap (priority queue) to reconstruct original order efficiently.
  */
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
