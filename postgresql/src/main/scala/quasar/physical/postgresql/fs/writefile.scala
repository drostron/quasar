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
import quasar.fs._
import quasar.physical.postgresql.util._

import java.sql.{Connection, Statement}

import scalaz._, Scalaz._

object writefile {
  import WriteFile._

  implicit val codec = DataCodec.Precise

  final case class PostgreSQLState(
    conn: Connection, st: Statement, tableName: String)

  @SuppressWarnings(Array("org.wartremover.warts.NonUnitStatements"))
  def interpret[S[_]](
    implicit
    S0: KeyValueStore[WriteHandle, PostgreSQLState, ?] :<: S,
    S1: MonotonicSeq :<: S)
    : WriteFile ~> Free[S, ?] = {
    val kv = KeyValueStore.Ops[WriteHandle, PostgreSQLState, S]
    val seq = MonotonicSeq.Ops[S]

    new (WriteFile ~> Free[S, ?]) {
      def apply[A](wf: WriteFile[A]) = wf match {
        case Open(file) =>
          println(s"write open file: $file")
          // TODO: o_O
          val Some((dbName, (tablePath, tableName))) = dbAndTableName(file).toOption // also dumb
          println(s"write open dbName: $dbName, tableName: $tableName")
          val conn = dbConn(dbName)
          // TODO: what was the origin intent for autocommit false?
          conn.setAutoCommit(true)
          val st = conn.createStatement()

          val tblExists = tableExists(conn, tableName)
          println(s"write open tblExists: $tblExists")

          if (!tblExists) {
            val iq = s"""create table "$tableName" (v json)"""
            println(s"write open iq: $iq")
            // TODO: incorrect: how to handle this? detect schema from data? a single json column? other?
            val r = st.executeUpdate(iq)
            println(s"write open create r: $r")
          }

          st.setFetchSize(1)

          for {
            i <- seq.next
            h =  WriteHandle(file, i)
            _ <- kv.put(h, PostgreSQLState(conn, st, tableName))
          } yield h.right

        // TODO: handle data
        case Write(h, data) =>
          println(s"write write: $h")
          kv.get(h)
            .toRight(Vector(FileSystemError.unknownWriteHandle(h)))
            .map { s =>
              def insert(json: String) =
                s"""insert into "${s.tableName}"
                   |  select * from
                   |  json_populate_record(NULL::"${s.tableName}", '{"v": $json}')
                   |""".stripMargin

               val _ = data.map { d =>
                 val q = insert {
                   val Some(v) = DataCodec.render(d).toOption
                   v
                 }
                //  println(s"write write q: $q")

                 // TODO: do something with r?
                 val r = s.st.executeUpdate(q)
               }

              Vector.empty }
            .merge[Vector[FileSystemError]]

        case Close(h) =>
          (for {
            s <- kv.get(h)
            _ =  s.st.close
            _ <- kv.delete(h).liftM[OptionT]
          } yield ()).run.void

      }
    }
  }

}
