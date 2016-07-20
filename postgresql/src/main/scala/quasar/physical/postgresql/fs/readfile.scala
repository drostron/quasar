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
import quasar.fs._
import quasar.physical.postgresql.util._

import java.sql.{Connection, ResultSet, Statement}

import scalaz._, Scalaz._

object readfile {
  import ReadFile._

  implicit val codec = DataCodec.Precise

  final case class PostgreSQLState(conn: Connection, st: Statement, rs: ResultSet)

  def interpret[S[_]](
    implicit
    S0: KeyValueStore[ReadHandle, PostgreSQLState, ?] :<: S,
    S1: MonotonicSeq :<: S)
    : ReadFile ~> Free[S, ?] = {
    val kv = KeyValueStore.Ops[ReadHandle, PostgreSQLState, S]
    val seq = MonotonicSeq.Ops[S]

    // TODO: offload helper methods once working
    new (ReadFile ~> Free[S, ?]) {
      def apply[A](rf: ReadFile[A]): Free[S, A] = rf match {
        // TODO: handle offset and limit
        // TODO: going with posix path table names just for the moment, likely not what we want
        case Open(file, offset, limit) =>
          val Some((dbName, (tablePath, tableName))) = dbAndTableName(file).toOption // also dumb
          println(s"read open dbName: $dbName, tableName: $tableName")
          val conn = dbConn(dbName) // TODO: will except

          val tblExists = tableExists(conn, tableName)
          println(s"read open table exists: $tblExists")

          // TODO: so ugly!
          if (!tblExists) {
            // Sooo manch shortcuts!
            Free.point(FileSystemError.pathErr(
              PathError.pathNotFound(file)).left)
          }
          else {
            conn.setAutoCommit(false)
            val st = conn.createStatement()

            st.setFetchSize(1)
            val rs = st.executeQuery(s"""select row_to_json(row) from  (select * from "$tableName") row""")

            for {
              i <- seq.next
              h =  ReadHandle(file, i)
              _ <- kv.put(h, PostgreSQLState(conn, st, rs))
            } yield h.right
          }

        case Read(h) =>
          kv.get(h)
            .toRight(FileSystemError.unknownReadHandle(h))
            .flatMapF(s => (
                if (s.rs.next)
                  // TODO: more appropriate error?
                  FileSystemError.unknownReadHandle(h).left
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

}
