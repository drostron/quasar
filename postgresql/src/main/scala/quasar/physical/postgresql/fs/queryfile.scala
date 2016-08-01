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
import quasar.fs._
import quasar.effect.Read
import quasar.fp.κ
import quasar.fs._, mount.ConnectionUri
import quasar.physical.postgresql.util._
import quasar.qscript._

import pathy.Path.{DirName, FileName}
import scalaz._, Scalaz._
import scalaz.concurrent.Task

object queryfile {
  import QueryFile._

  def interpret[S[_]](
    implicit
    S0: Read[ConnectionUri, ?] :<: S,
    S1: Task :<: S
  ): QueryFile ~> Free[S, ?] = new (QueryFile ~> Free[S, ?]) {
    def apply[A](qf: QueryFile[A]) = qf match {
      case ExecutePlan(lp, out) =>
        ???

      case EvaluatePlan(lp) =>
        ???

      case More(h) =>
        ???

      case Close(h) =>
        ???

      case Explain(lp) =>
        ???

      case ListContents(dir) =>
        // println(s"queryfile ListContents: $dir")
        listContents(dir)

      case FileExists(file) =>
        // TODO: handle failures? might need to update the algebra term
        // println(s"queryfile FileExists: $file")
        (for {
          dt  <- dbTableFromPath(file)
          cxn <- dbCxn.liftM[FileSystemErrT]
          r   <- tableExists(cxn, dt.table).liftM[FileSystemErrT]
        } yield r).leftMap(κ(false)).merge[Boolean]
    }
  }

  // TODO: handle ☠
  def listContents[S[_]](
    dir: APath
  )(implicit
    S0: Read[ConnectionUri, ?] :<: S,
    S1: Task :<: S
  ): Free[S, FileSystemError \/ Set[PathSegment]] =
    (for {
      dt  <- dbTableFromPath(dir)
      cxn <- dbCxn.liftM[FileSystemErrT]
      r   <- tablesWithPrefix(cxn, dt.table).liftM[FileSystemErrT]
      _   <- EitherT.fromDisjunction[Free[S, ?]] {
               if (r.isEmpty) FileSystemError.pathErr(PathError.pathNotFound(dir)).left
               else ().right
             }
    } yield r
      .map(_.stripPrefix(dt.table).stripPrefix("☠").split("☠").toList)
      .collect {
        case h :: Nil => FileName(h).right
        case h :: _   => DirName(h).left
      }.toSet).run

}
