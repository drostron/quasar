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
import quasar.effect.{KeyValueStore, MonotonicSeq, Read}
import quasar.fp.{reflNT, TaskRef}
import quasar.fp.free.{injectNT, injectFT, mapSNT, EnrichNT}
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount.{ConnectionUri, FileSystemDef}, FileSystemDef.DefErrT

import doobie.imports._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

package object fs {
  val FsType = FileSystemType("postgresql")

  // TODO: names

  type ψ3[A] = Coproduct[
                    KeyValueStore[ReadHandle,  impl.ReadStream[ConnectionIO], ?],
                    KeyValueStore[WriteHandle, writefile.PostgreSQLState,     ?],
                    A]
  type ψ2[A]  = Coproduct[MonotonicSeq,           ψ3, A]
  type ψ1[A]  = Coproduct[Read[ConnectionUri, ?], ψ2, A]
  type ψ0[A]  = Coproduct[ConnectionIO,           ψ1, A]
  type ψ[A]   = Coproduct[Task,                   ψ0, A]

  def ζ[S[_]](
      uri: ConnectionUri
    )(implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): Free[S, Free[ψ, ?] ~> Free[S, ?]] = {

    val transactor: Transactor[Task] =
      DriverManagerTransactor[Task]("org.postgresql.Driver", uri.value)

    def ε: Task[Free[ψ, ?] ~> Free[S, ?]]  =
      // TODO: add KeyValueStore.fromEmpty
      (TaskRef(Map.empty[ReadHandle,  impl.ReadStream[ConnectionIO]]) |@|
       TaskRef(Map.empty[WriteHandle, writefile.PostgreSQLState])     |@|
       TaskRef(0L)
     )((kvR, kvW, i) =>
       mapSNT(injectNT[Task, S] compose (
         reflNT[Task]                            :+:
         transactor.trans                        :+:
         Read.constant[Task, ConnectionUri](uri) :+:
         MonotonicSeq.fromTaskRef(i)             :+:
         KeyValueStore.fromTaskRef(kvR)          :+:
         KeyValueStore.fromTaskRef(kvW))))

    injectFT[Task, S].apply(ε)
  }

  def definition[S[_]](implicit
      S0: Task :<: S,
      S1: PhysErr :<: S
    ): FileSystemDef[Free[S, ?]] =
    FileSystemDef.fromPF {
      case (FsType, uri) =>
        ζ(uri).map { i =>
          FileSystemDef.DefinitionResult[Free[S, ?]](
            i compose interpretFileSystem(
              queryfile.interpret,
              readfile.interpret,
              writefile.interpret,
              managefile.interpret),
            Free.point(()))}.liftM[DefErrT]
    }
}
