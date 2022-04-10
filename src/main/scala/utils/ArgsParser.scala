package org.example
package utils

import org.kohsuke.args4j.{CmdLineException, CmdLineParser}

object ArgsParser {
  def parseArguments(args: Seq[String]) {
    val parser = new CmdLineParser(this);

    try {
      parser.parseArgument(scala.collection.JavaConverters.seqAsJavaList(args))
    } catch {
      case ex: CmdLineException =>
        System.err.println("Usage:")
        parser.printUsage(System.err)
        System.err.println()

        throw new RuntimeException("Unable to parse options", ex)
    }
  }
}
