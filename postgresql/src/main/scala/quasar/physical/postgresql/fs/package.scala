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

package quasar.physical
package postgresql

import quasar.Predef._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.fp.{reflNT, TaskRef}
import quasar.fp.free.{injectNT, injectFT, mapSNT, EnrichNT}
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount.FileSystemDef, FileSystemDef.DefErrT

import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object fs {
  val FsType = FileSystemType("postgresql")

  // TODO: names

  type Woes1[A] = Coproduct[
                    KeyValueStore[ReadHandle,  readfile.PostgreSQLState,  ?],
                    KeyValueStore[WriteHandle, writefile.PostgreSQLState, ?],
                    A]
  type Woes0[A]  = Coproduct[MonotonicSeq, Woes1, A]
  type Woes[A]   = Coproduct[Task,         Woes0, A]

  def ζ[S[_]](
    implicit
    S0: Task :<: S,
    S1: PhysErr :<: S)
    : Free[S, Free[Woes, ?] ~> Free[S, ?]] = {

    val ε =
      (TaskRef(Map.empty[ReadHandle,  readfile.PostgreSQLState])  |@|
       TaskRef(Map.empty[WriteHandle, writefile.PostgreSQLState]) |@|
       TaskRef(0L)
      )((kvR, kvW, i) => (
       KeyValueStore.fromTaskRef(kvR),
       KeyValueStore.fromTaskRef(kvW),
       MonotonicSeq.fromTaskRef(i)
      ))

    val α: Task[Free[Woes, ?] ~> Free[S, ?]] =
      ε.map { case (kvR, kvW, seq) =>
        new (Free[Woes, ?] ~> Free[S, ?]) {
          def apply[A](fa: Free[Woes, A]): Free[S, A] =
            mapSNT(injectNT[Task, S] compose (reflNT[Task] :+: seq :+: kvR :+: kvW))(fa)
        }
      }

    injectFT[Task, S].apply(α)
  }

  def definition[S[_]](implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        ζ.map { i =>
          println("bleh")
          FileSystemDef.DefinitionResult[Free[S, ?]](
            i compose interpretFileSystem(
              queryfile.interpret,
              readfile.interpret,
              writefile.interpret,
              managefile.interpret),
            Free.point(()))}.liftM[DefErrT]
    }
}
