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
import quasar.DataCodec
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp.κ
import quasar.fp.numeric.{Natural, Positive}
import quasar.fs._
import quasar.physical.postgresql.util._

import java.sql.{Connection, ResultSet, Statement}

import eu.timepit.refined.api.RefType.ops._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object readfile {
  import ReadFile._

  implicit val codec = DataCodec.Precise

  final case class PostgreSQLState(conn: Connection, st: Statement, rs: ResultSet)

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[ReadHandle, PostgreSQLState, ?] :<: S,
    S1: MonotonicSeq :<: S,
    S2: Task :<: S)
    : ReadFile ~> Free[S, ?] = {
    val kv = KeyValueStore.Ops[ReadHandle, PostgreSQLState, S]
    val seq = MonotonicSeq.Ops[S]

    // TODO: offload to helper methods once working
    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]): Free[S, A] = rf match {
        case Open(file, offset, limit) =>
          (for {
            dt   <- dbTableFromPath(file)
            cxn  <- dbCxn(dt.db).liftM[FileSystemErrT]
            _    <- tableExists(cxn, dt.table)
                      .map(_ either(()) or(FileSystemError.pathErr(PathError.pathNotFound(file))))
                      .liftM[FileSystemErrT]
            pgSt <- open(cxn, dt.table, offset, limit).liftM[FileSystemErrT]
            i    <- seq.next.liftM[FileSystemErrT]
            h    =  ReadHandle(file, i)
            _    <- kv.put(h, pgSt).liftM[FileSystemErrT]
          } yield h).run

        case Read(h) =>
          // println(s"read read: $h")
          kv.get(h)
            .toRight(FileSystemError.unknownReadHandle(h))
            .flatMapF(s => (
                if (!s.rs.next)
                  Vector.empty.right
                else
                  DataCodec.parse(s.rs.getString(1)).bimap(
                    // TODO: more appropriate error?
                    κ(FileSystemError.unknownReadHandle(h)),
                    Vector(_))
              ).point[Free[S, ?]])
            .run

        case Close(h) =>
          (for {
            s <- kv.get(h)
            _ =  s.rs.close
            _ =  s.st.close
            _ =  s.conn.close
            _ <- kv.delete(h).liftM[OptionT]
          } yield ()).run.void
      }
    }
  }

  // TODO: more appropriate name
  // TODO: val _'s
  def open[S[_]](
      cxn: Connection, tableName: String, offset: Natural, limit: Option[Positive]
    )(implicit
      S0: Task :<: S
    ): Free[S, PostgreSQLState] =
    Free.liftF(S0.inj(Task.delay {
      val st = cxn.createStatement(
        ResultSet.TYPE_SCROLL_INSENSITIVE,
        ResultSet.CONCUR_READ_ONLY)

      st.setFetchSize(1)

      val _ = limit.map(lim => st.setMaxRows(lim.unwrap.toInt)) // TODO: toInt

      val rs = st.executeQuery(s"""select v from "$tableName"""")

      val __ = rs.absolute(offset.unwrap.toInt) // TODO: toInt

      PostgreSQLState(cxn, st, rs)
    }))

}
