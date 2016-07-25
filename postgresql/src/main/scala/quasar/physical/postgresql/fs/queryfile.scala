/*
 * Copyright 2014–2016 SlamData Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package quasar.physical.postgresql.fs

import quasar.Predef._
import quasar.fp.{κ, free}, free.injectFT
import quasar.fs._
import quasar.physical.postgresql.{Planner, SQLAST}, Planner._, Planner.Planner._
import quasar.physical.postgresql.util._
import quasar.qscript._

import matryoshka._, Recursive.ops._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile._

import quasar.Planner.PlannerError

  def evaluate(sqlAST: SQLAST, destinationPath: APath): Task[AFile] = ???

  def interpret[S[_]](implicit S0: Task :<: S): QueryFile ~> Free[S, ?] = new (QueryFile ~> Free[S, ?]) {
    def apply[A](qf: QueryFile[A]) = qf match {
      case ExecutePlan(lp, out) => (
          for {
            qs  <- EitherT(convertToQScript(lp).point[Free[S, ?]])
            sql <- EitherT(qs.cataM(
                     Planner.Planner[QScriptProject[Fix, ?]].plan).point[Free[S, ?]])
            dst <- injectFT[Task, S].apply(evaluate(sql, out)).liftM[EitherT[?[_], PlannerError, ?]]
          } yield dst
        ).leftMap(FileSystemError.planningFailed(lp,_)).run.strengthL(Vector.empty)

      case EvaluatePlan(lp) =>
        ???

      case More(h) =>
        ???

      case Close(h) =>
        ???

      case Explain(lp) =>
        ???

      case ListContents(dir) =>
        ???

      case FileExists(file) =>
        // TODO: handle failures? might need to update the algebra term
        println(s"FileExists: $file")
        (for {
          dt  <- dbTableFromPath(file)
          cxn <- dbCxn(dt.db).liftM[FileSystemErrT]
          r   <- tableExists(cxn, dt.table).liftM[FileSystemErrT]
        } yield r).leftMap(κ(false)).merge[Boolean]
    }
  }
}
