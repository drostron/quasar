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
import quasar.effect.{MonotonicSeq, Read}
import quasar.fs._, mount.ConnectionUri
import quasar.physical.postgresql.util._

import pathy.Path._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object managefile {
  import ManageFile._

  // TODO: move more into helper methods
  def interpret[S[_]](implicit
    S0: MonotonicSeq :<: S,
    S1: Read[ConnectionUri, ?] :<: S,
    // TODO: something more precise than Task?
    S2: Task :<: S
  ): ManageFile ~> Free[S, ?] = new (ManageFile ~> Free[S, ?]) {
    val seq = MonotonicSeq.Ops[S]

    def apply[A](fs: ManageFile[A]) = fs match {
      case Move(scenario, semantics) =>
        // println(s"managefile Move: $scenario, $semantics")
        move(scenario, semantics)

      case Delete(path) =>
        // println(s"managefile Delete: $path")
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
    scenario: MoveScenario, semantics: MoveSemantics
  )(implicit
    S0: Task :<: S,
    S1: Read[ConnectionUri, ?] :<: S
  ): Free[S, FileSystemError \/ Unit] =
    (for {
      src       <- dbTableFromPath(scenario.src)
      dst       <- dbTableFromPath(scenario.dst)
      _         <- EitherT.fromDisjunction[Free[S, ?]] {
                     if (src.db =/= dst.db) FileSystemError.pathErr(
                       PathError.invalidPath(scenario.dst, "different db from src path")).left
                     else
                       ().right
                   }
      cxn       <- dbCxn.liftM[FileSystemErrT]
      srcExists <- tableExists(cxn, src.table).liftM[FileSystemErrT]
      srcTables <- tablesWithPrefix(cxn, src.table).liftM[FileSystemErrT]
      _         <- EitherT.fromDisjunction[Free[S, ?]] {
                     if (srcTables.isEmpty)
                       FileSystemError.pathErr(PathError.pathNotFound(scenario.src)).left
                     else
                       ().right
                   }
      dstExists <- tableExists(cxn, dst.table).liftM[FileSystemErrT]
      _         <- EitherT.fromDisjunction[Free[S, ?]](semantics match {
                     case MoveSemantics.FailIfExists if dstExists =>
                       FileSystemError.pathErr(PathError.pathExists(scenario.dst)).left
                     case MoveSemantics.FailIfMissing if !dstExists =>
                       FileSystemError.pathErr(PathError.pathNotFound(scenario.dst)).left
                     case _ =>
                       ().right[FileSystemError]
                   })
    } yield {
      val st = cxn.createStatement()
      cxn.setAutoCommit(false)

      val srcTablesToMove =
        if (MoveScenario.dirToDir.isMatching(scenario)) srcTables
        else List(src.table)

      (srcTablesToMove).foreach { srcTable =>
        val dstTable = dst.table + srcTable.stripPrefix(src.table)

        val _  = st.executeUpdate(s"""DROP TABLE IF EXISTS "$dstTable" """)

        val q = s"""ALTER TABLE "$srcTable" RENAME TO "$dstTable" """
        // println(s"q: $q")
        val __ = st.executeUpdate(q)
      }

      cxn.commit()
      st.close
      cxn.close
    }).run

  def delete[S[_]](
    path: APath
  )(implicit
    S0: Task :<: S,
    S1: Read[ConnectionUri, ?] :<: S
  ): Free[S, FileSystemError \/ Unit] = {
    for {
      dt  <- dbTableFromPath(path)
      cxn <- dbCxn.liftM[FileSystemErrT]
      tbs <- tablesWithPrefix(cxn, dt.table).liftM[FileSystemErrT]
      _   <- EitherT.fromDisjunction[Free[S, ?]] {
               if (tbs.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(path)).left
               else ().right
             }
    } yield {
      // println(s"managefile delete:\n  path:   $path\n  tables: $tbs")
      tbs.foreach { tableName =>
        val q = s"""DROP TABLE "$tableName" """
        val st = cxn.createStatement()
        val _ = st.executeUpdate(q)
        st.close()
      }
    }
  }.run

}
