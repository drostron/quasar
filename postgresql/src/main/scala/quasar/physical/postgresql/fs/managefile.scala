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
import scalaz.concurrent.Task

object managefile {
  import ManageFile._

  // TODO: move more into helper methods
  def interpret[S[_]](implicit
    S0: MonotonicSeq :<: S,
    // TODO: something more precise than Task?
    S1: Task :<: S
    ): ManageFile ~> Free[S, ?] = new (ManageFile ~> Free[S, ?]) {
    val seq = MonotonicSeq.Ops[S]

    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) =>
        (for {
          src <- dbTableFromPath(scenario.src)
          dst <- dbTableFromPath(scenario.dst)
          _   <- EitherT.fromDisjunction[Free[S, ?]] {
                   if (src.db =/= dst.db) FileSystemError.pathErr(
                     PathError.invalidPath(scenario.dst, "different db from src path")).left
                   else
                     ().right
                 }
          _   <- move(src.db, src.table, dst.table)
        } yield ()).run

      case Delete(path) =>
        delete(path)

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

  // TODO: more appropriate name
  // TODO: handle exceptions
  def move[S[_]](
      db: String, src: String, dst: String
    )(implicit
      S0: Task :<: S
    ): FileSystemErrT[Free[S, ?], Unit] =
    dbCxn(db).map { cxn =>
      val q = s"""ALTER TABLE "$src" RENAME TO "$dst" """
      val st = cxn.createStatement()
      val _ = st.executeUpdate(q)
    }.liftM[FileSystemErrT]

  def delete[S[_]](path: APath)(implicit S0: Task :<: S): Free[S, FileSystemError \/ Unit] = {
    for {
      dt  <- dbTableFromPath(path)
      cxn <- dbCxn(dt.db).liftM[FileSystemErrT]
      tbs <- tablesWithPrefix(cxn, dt.table).liftM[FileSystemErrT]
    } yield {
      println(s"managefile delete:\n  path:   $path\n  tables: $tbs")
      tbs.foreach { tableName =>
        val q = s"""DROP TABLE "$tableName" """
        val st = cxn.createStatement()
        val _ = st.executeUpdate(q)
        st.close()
      }
    }
  }.run

}
