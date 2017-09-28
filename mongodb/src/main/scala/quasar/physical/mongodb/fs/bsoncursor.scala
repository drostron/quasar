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

package quasar.physical.mongodb.fs

import slamdata.Predef._
import quasar.Data
import quasar.fs.DataCursor
import quasar.physical.mongodb._
import quasar.physical.mongodb.workflow.{ExprLabel, WrapperSigilLabel}

import scala.Option
import scala.collection.JavaConverters._

import org.bson.BsonValue
import scalaz.Kleisli
import scalaz.concurrent.Task
import scalaz.syntax.compose._
import scalaz.syntax.monad._
import scalaz.syntax.plus._
import scalaz.syntax.std.option._
import scalaz.std.function._
import scalaz.std.option._
import scalaz.std.vector._

object bsoncursor {
  implicit val bsonCursorDataCursor: DataCursor[MongoDbIO, BsonCursor] =
    new DataCursor[MongoDbIO, BsonCursor] {
      def nextChunk(cursor: BsonCursor) = {
        // NB: `null` is used as a sentinel value to indicate input is
        //     exhausted, because Java.
        def nextChunk0 =
          MongoDbIO.async(cursor.next) map (r =>
            Option(r).map(_.asScala.toVector).orZero.map(toData))

        isClosed(cursor) ifM (Vector[Data]().point[MongoDbIO], nextChunk0)
      }

      def close(cursor: BsonCursor) =
        MongoDbIO.liftTask(Task.delay(cursor.close()))

      ////

      /** Returns the value of the named field or `None` if the input isn't
        * a document or the field doesn't exist.
        */
      def fieldValue(name: String): BsonValue => Option[BsonValue] =
        v => bsonvalue.document.getOption(v).flatMap(d => Option(d.get(name)))

      val sigilValue: BsonValue => Option[BsonValue] =
        fieldValue(WrapperSigilLabel)

      val mapReduceSigilValue: BsonValue => Option[BsonValue] =
        Kleisli(sigilValue) <==< fieldValue(ExprLabel)

      val elideSigil: BsonValue => BsonValue =
        v => (sigilValue(v) <+> mapReduceSigilValue(v)) | v

      val toData: BsonValue => Data =
        (BsonCodec.toData _) <<< Bson.fromRepr <<< elideSigil

      def isClosed(cursor: BsonCursor): MongoDbIO[Boolean] =
        MongoDbIO.liftTask(Task.delay(cursor.isClosed))
    }
}
