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
import quasar.fs._

import scalaz._

object queryfile {
  import QueryFile._

  def interpret[S[_]]: QueryFile ~> Free[S, ?] = new (QueryFile ~> Free[S, ?]) {
    def apply[A](qf: QueryFile[A]) = qf match {
      case ExecutePlan(lp, out) =>
        ???

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
        ???
    }
  }
}
