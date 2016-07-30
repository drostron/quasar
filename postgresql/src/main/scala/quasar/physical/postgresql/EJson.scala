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
import quasar.physical.postgresql.SQLData._
import quasar.ejson, ejson._
import quasar.Planner.PlannerError

import matryoshka._
import scalaz._, Scalaz._

object EJson {

  def fromCommon: Algebra[Common, SQLData] = {
    // TODO: SQL Arrays is homogeneous but value here could be any of SQLData?
    // further arrays are just a postgres thing
    // if v is all the same type then, otherwise PSQLJson
    // case Arr(v: List[PostgreSQLArr]) => ???
    case Arr(v)  => PostgreSQLArr(List(42), v.toVector)
    case Null()  => SQLNull
    case Bool(v) => SQLBool(v)
    case Str(v)  => SQLStr(v)
    case Dec(v)  => SQLDec(v)
  }

  def fromExtension: AlgebraM[PlannerError \/ ?, Extension, SQLData] = {
    // use meta to get determine if timestamps are contained within map, convert accordingly
    case ejson.Meta(value /*: A */, meta /*: A */)  => ???
    case ejson.Map(value /*: List[(A, A)] */)       => ??? /* PostgreSQLJson */
    case ejson.Byte(value)                          => PostgreSQLByteArray(List(value)).right
    case ejson.Char(value)                          => SQLStr(value.toString).right // likely, not quite right
    case ejson.Int(value)                           => SQLInt(value).right
  }

  val fromEJson: AlgebraM[PlannerError \/ ?, EJson, SQLData] =
    _.run.fold(fromExtension, fromCommon(_).right)

  def toEJson[F[_]](implicit C: ejson.Common :<: F, E: ejson.Extension :<: F):
      CoalgebraM[PlannerError \/ ?, F, SQLData] = {
    case PostgreSQLArr(dim, value) => ???
      // C.inj {
      //   // TODO: handle dimension slicing
      //   ejson.Arr(value.toList)
      // }.right[PlannerError]
    case SQLNull              => C.inj(ejson.Null()).right
    case SQLBool(value)       => C.inj(ejson.Bool(value)).right
    case _ => ???
  }

}
