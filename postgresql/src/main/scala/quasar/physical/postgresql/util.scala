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

object util {

  val bleh = java.lang.Class.forName("org.postgresql.Driver")

  // TODO: naive for the moment
  def dbConn(dbName: String) = DriverManager.getConnection(
    s"jdbc:postgresql://192.168.99.100/$dbName?user=postgres&password=postgres")

  // culted from mongodb Collection, TODO: move to common place if becomes common
  def dbAndTableName(f: APath): String \/ (String, (IList[String], String)) =
      Path.flatten(None, None, None, Some(_), Some(_), f)
        .toIList.unite.uncons(
          "no database specified".left,
          //Path.posixCodec.printPath(f)
          (h, t) => (h, (t, t.intercalate("☠"))).right) // even more naive

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  def tableExists(conn: Connection, tableName: String): Boolean = {
    val m = conn.getMetaData
    val r = m.getTables(null, null, tableName, null)
    r.next
  }

}
