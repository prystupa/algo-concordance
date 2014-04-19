object MainCmdLine extends App {

  def getMultiLine(result: String): String = {
    val s = readLine()
    if (s.isEmpty) result else getMultiLine(result + "\n" + s)
  }

  println("Enter text to analyze (or copy/paste). You can use [Enter] to input multiple lines. Enter empty line to complete input.")

  val text = getMultiLine("")
  val analysis = new ConcordanceSolverRegex().solve(text)

  analysis.foreach(c => {
    println(s"${c.word} {${c.sentences.length}: ${c.sentences.mkString(",")}}")
  })
}
