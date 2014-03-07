package slamdata.engine

import java.io.IOException
import org.jboss.aesh.console.Console
import org.jboss.aesh.console.AeshConsoleCallback
import org.jboss.aesh.console.ConsoleOperation
import org.jboss.aesh.console.Prompt
import org.jboss.aesh.console.settings.SettingsBuilder

import slamdata.engine.std._
import slamdata.engine.sql._
import slamdata.engine.analysis._

import scalaz.{NonEmptyList, Show}
import scalaz.std.string._
import scalaz.std.tuple._
import scalaz.std.map._
import scalaz.syntax._

object Repl {
  def main(args: Array[String]) {
    val console = new Console(new SettingsBuilder().parseOperators(false).create())

    console.setPrompt(new Prompt("slamdata$ "))
    console.setConsoleCallback(new AeshConsoleCallback() {
      override def execute(output: ConsoleOperation): Int = {
        if (output.getBuffer == "exit") {
          try {
            console.stop()
          } catch {
            case e: IOException => e.printStackTrace()
          }
        } else {
          val out = console.getShell.out()

          new SQLParser().parse(output.getBuffer).fold(
            error => out.println("SQL could not be parsed:\n" + error),
            select => {
              import SemanticAnalysis._

              out.println("Successfully parsed SQL: \n" + select.sql)

              val phases = (ScopeTables[Unit] >>> 
                           ProvenanceInfer).split >>> 
                           FunctionBind[Provenance](StdLib).first >>>
                           TypeInfer.first

              try {
                phases(tree(select)).fold(
                  error => out.println(Show[NonEmptyList[SemanticError]].show(error).toString),
                  success => {
                    println(Show[AnnotatedTree[Node, (Map[Node, Type], Provenance)]].show(success).toString)
                  }
                )
              } catch {
                case e: Throwable => e.printStackTrace
              }
            }
          )
        }

        0
      }
    })
    console.start()
  }
}