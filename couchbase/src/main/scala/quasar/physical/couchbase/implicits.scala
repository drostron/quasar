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
import quasar.common.PhaseResultT
import quasar.connector.EnvironmentError
import quasar.fs.{FileSystemError, FileSystemErrT}
import quasar.fs.mount.FileSystemDef.DefErrT

import scalaz._, Scalaz._, concurrent.Task

object implicits {
  import Couchbase.{Eff, M}

  implicit class LiftF[F[_]: Monad, A](p: F[A]) {
    val lift: FileSystemErrT[PhaseResultT[F, ?], A] = p.liftM[PhaseResultT].liftM[FileSystemErrT]
  }

  implicit class LiftFE[A](d: FileSystemError \/ A) {
    def lift: M[A] = EitherT(d.η[Free[Eff, ?]].liftM[PhaseResultT])
  }

  // TODO: Remove or reintroduce if needed
  // implicit class LiftPE[A](d: PlannerError \/ A) {
  //   def lift: M[A] = d.leftMap(FileSystemError.qscriptPlanningFailed(_)).lift
  // }

  implicit class LiftDT[A](v: => NonEmptyList[String] \/ A) {
    // TODO: Switch to liftDT if possible. Above F[A] might be switchable to Task[A]
    def liftDT: DefErrT[Task, A] = EitherT(Task.delay(v.leftMap(_.left[EnvironmentError])))
  }
}
