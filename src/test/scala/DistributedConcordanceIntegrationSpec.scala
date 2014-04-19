import ConcordanceStorageInMemory.Search
import akka.testkit.TestProbe
import ConcordanceIndexerJob.{EndOfInput, BatchText}
import akka.actor.ActorSystem
import org.scalatest.{Matchers, path}

class DistributedConcordanceIntegrationSpec extends path.FunSpec with Matchers {

  implicit val system = ActorSystem("DistributedConcordanceIntegrationSpec")
  val storage = system.actorOf(ConcordanceStorageInMemory.props())
  val indexer = system.actorOf(ConcordanceIndexerJob.props(storage))
  val probe = TestProbe()

  probe watch indexer

  describe("when I sent multiple out-of-order batches to indexer") {
    // give indexer several different batches, give them out of order
    indexer ! BatchText("This is second batch. It has two sentences.", 1)
    indexer ! BatchText("This is first batch with exactly one sentence.", 0)
    indexer ! BatchText("Last batch. Three sentences. End.", 2)
    indexer ! EndOfInput

    val terminated = probe.expectTerminated(indexer)

    it("then indexer terminates after all work is done") {
      terminated should not be null
    }

    describe("and I ask the storage for 'sentences' word concordance") {

      probe.send(storage, Search("sentences"))
      val reply = probe.expectMsgType[Option[Concordance]]

      it("replies with correctly computed concordance with properly ordered sentence numbers") {
        reply should equal(Some(Concordance("sentences", List(2, 4))))
      }
    }
  }

  system shutdown()
}
