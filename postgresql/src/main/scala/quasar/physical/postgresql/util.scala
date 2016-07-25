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

package quasar.physical.postgresql

import quasar.Predef._
import quasar.fs._

import java.sql.{Connection, DriverManager}

import pathy.Path
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: handle exceptions more explicitly
// TODO: something better than Free.liftF(S0.inj(Task.delay ... ?
object util {

  // TODO: find another way?
  val pgDriver = java.lang.Class.forName("org.postgresql.Driver")

  final case class DbTable(db: String, table: String)

  def dbCxn[S[_]](
    dbName: String
  )(implicit
    S0: Task :<: S
  ): Free[S, Connection] =
    Free.liftF(S0.inj(Task.delay {
      DriverManager.getConnection(
        s"jdbc:postgresql://192.168.99.100/$dbName?user=postgres&password=postgres")
    }))

  // TODO: going with ☠ style path table names for the moment, likely not what we want
  def dbTableFromPath[S[_]](f: APath): FileSystemErrT[Free[S, ?], DbTable] =
    EitherT.fromDisjunction[Free[S, ?]](
      Path.flatten(None, None, None, Some(_), Some(_), f)
        .toIList.unite.uncons(
          FileSystemError.pathErr(PathError.invalidPath(f, "no database specified")).left,
          (h, t) => DbTable(h, t.intercalate("☠")).right))

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def tableExists[S[_]](
      conn: Connection, tableName: String
    )(implicit
      S0: Task :<: S
    ): Free[S, Boolean] =
    Free.liftF(S0.inj(Task.delay {
      val m = conn.getMetaData
      val r = m.getTables(null, null, tableName, null)
      r.next
    }))

  @SuppressWarnings(Array("org.wartremover.warts.Var", "org.wartremover.warts.While"))
  def tablesWithPrefix[S[_]](
      conn: Connection, tableNamePrefix: String
    )(implicit
      S0: Task :<: S
    ): Free[S, List[String]] =
    Free.liftF(S0.inj(Task.delay {
      val st = conn.createStatement()
      val rs = st.executeQuery(
        s"""select table_name from information_schema.tables where table_name like '$tableNamePrefix%'""")
      var bleh = Vector[String]()
      while(rs.next) {
        bleh = bleh :+ rs.getString(1)
      }
      bleh.toList
  }))

}
