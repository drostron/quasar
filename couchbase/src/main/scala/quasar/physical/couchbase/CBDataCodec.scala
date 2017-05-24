/*
 * Copyright 2014–2017 SlamData Inc.
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

package quasar.physical.couchbase

import slamdata.Predef._
import quasar._, fp._, ski._
import quasar.DataCodec.Precise

import argonaut._, Argonaut._
import scalaz._, Scalaz._

object CBDataCodec extends Precise {
  val NAKey  = "$na"

  override def encode(data: Data): Option[Json] = {
    import Data._
    data match {
      case NA => Json.obj(NAKey -> jNull).some
      case d  => super.encode(d)
    }
  }

  override def decode(json: Json): DataEncodingError \/ Data =
    json.fold(
      super.decode(json),
      κ(super.decode(json)),
      κ(super.decode(json)),
      κ(super.decode(json)),
      κ(super.decode(json)),
      obj => obj.toList match {
        case (`NAKey`, value) :: Nil => Data.NA.right
        case _                       => super.decode(json)
      })
}
