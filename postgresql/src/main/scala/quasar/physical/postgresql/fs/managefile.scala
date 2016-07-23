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
import quasar.effect.MonotonicSeq
import quasar.fs._
import quasar.physical.postgresql.util._

import pathy.Path._
import scalaz._, Scalaz._

object managefile {
  import ManageFile._

  def interpret[S[_]](implicit
    S0: MonotonicSeq :<: S
    ): ManageFile ~> Free[S, ?] = new (ManageFile ~> Free[S, ?]) {
    val seq = MonotonicSeq.Ops[S]

    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) =>
        val Some((srcDbName, (srcTablePath, srcTableName))) =
          dbAndTableName(scenario.src).toOption
        val Some((dstDbName, (dstTablePath, dstTableName))) =
          dbAndTableName(scenario.dst).toOption

        println(s"move:\n  ${scenario.src}\n  ${scenario.dst}")

        if (srcDbName =/= dstDbName) {
          FileSystemError.pathErr(
            PathError.invalidPath(scenario.dst, "different db from src path"))
              .left
              .point[Free[S, ?]]
        } else {

          val conn = dbConn(srcDbName) // TODO: will except

          val q = s"""ALTER TABLE "$srcTableName" RENAME TO "$dstTableName" """
          val st = conn.createStatement()
          val _ = st.executeUpdate(q)
          ().right.point[Free[S, ?]]
        }

      case Delete(path) =>
        // TODO: impl
        Free.point(().right)

      case TempFile(path) =>
        seq.next.map { i =>
          val tmpFilename = file(s"__quasar_tmp_$i")
          refineType(path).fold(
            d => d </> tmpFilename,
            f => fileParent(f) </> tmpFilename
          ).right
        }
    }
  }
}
