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
import quasar._
import quasar.common.{Int => _, Map => _, _}
import quasar.connector.BackendModule
import quasar.contrib.pathy._
import quasar.contrib.scalaz.eitherT._
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp._, free._, numeric._
import quasar.fs._, QueryFile.ResultHandle, ReadFile.ReadHandle, WriteFile.WriteHandle
import quasar.fs.mount._, FileSystemDef.DefErrT
import quasar.physical.couchbase.common._
import quasar.physical.couchbase.fs._
import quasar.physical.couchbase.planner.Planner
import quasar.qscript.{Map => _, _}

import scala.Predef.implicitly

import com.couchbase.client.java.Bucket
import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: Rename. Find Home. Needs will change once new Bkt Mount mapping is in place.
final case class State(bucket: Bucket, collection: String)

// TODO: Split to multiple files
object Couchbase extends BackendModule {
  import implicits._

  type Eff[A] = (
    Task                                    :\:
    MonotonicSeq                            :\:
    GenUUID                                 :\:
    KeyValueStore[ReadHandle,   Cursor,  ?] :\:
    KeyValueStore[WriteHandle,  State,  ?]  :/:
    KeyValueStore[ResultHandle, Cursor, ?]
  )#M[A]

  type QS[T[_[_]]] = QScriptCore[T, ?] :\: EquiJoin[T, ?] :/: Const[ShiftedRead[AFile], ?]

  implicit def qScriptToQScriptTotal[T[_[_]]]: Injectable.Aux[QSM[T, ?], QScriptTotal[T, ?]] =
    ::\::[QScriptCore[T, ?]](::/::[T, EquiJoin[T, ?], Const[ShiftedRead[AFile], ?]])

  type Repr[T[_[_]]] = T[N1QL]

  type M[A] = FileSystemErrT[PhaseResultT[Free[Eff, ?], ?], A]

  def FunctorQSM[T[_[_]]] = Functor[QSM[T, ?]]
  def DelayRenderTreeQSM[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    implicitly[Delay[RenderTree, QSM[T, ?]]]
  def ExtractPathQSM[T[_[_]]: RecursiveT] = ExtractPath[QSM[T, ?], APath]
  def QSCoreInject[T[_[_]]] = implicitly[QScriptCore[T, ?] :<: QSM[T, ?]]
  def MonadM = Monad[M]
  def MonadFsErrM = MonadFsErr[M]
  def PhaseResultTellM = PhaseResultTell[M]
  def PhaseResultListenM = PhaseResultListen[M]
  def UnirewriteT[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    implicitly[Unirewrite[T, QS[T]]]
  def UnicoalesceCap[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT] =
    Unicoalesce.Capture[T, QS[T]]

  type Config = Context

  def parseConfig(uri: ConnectionUri): DefErrT[Task, Config] = context(uri)

  def compile(cfg: Config): DefErrT[Task, (M ~> Task, Task[Unit])] = interp(cfg)

  val Type: FileSystemType = FileSystemType("couchbase")

  def plan[T[_[_]]: BirecursiveT: EqualT: ShowT: RenderTreeT](cp: T[QSM[T, ?]]): M[Repr[T]] =
    cp.cataM(
      Planner[T, Free[Eff, ?], QSM[T, ?]].plan
    ).leftMap(FileSystemError.qscriptPlanningFailed(_))

  object QueryFileModule extends QueryFileModule {
    import QueryFile._

    def executePlan[T[_[_]]](repr: Repr[T], out: AFile): Kleisli[M, Config, AFile] = ???
    def evaluatePlan[T[_[_]]](repr: Repr[T]): Kleisli[M, Config, ResultHandle] = ???
    def more(h: ResultHandle): Kleisli[M, Config, Vector[Data]] = ???
    def close(h: ResultHandle): Kleisli[M, Config, Unit] = ???
    def explain[T[_[_]]](repr: Repr[T]): Kleisli[M, Config, String] = ???
    def listContents(dir: ADir): Kleisli[M, Config, Set[PathSegment]] = ???
    def fileExists(file: AFile): Kleisli[M, Config, Boolean] = ???
  }

  object ReadFileModule extends ReadFileModule {
    import ReadFile._

    def open(file: AFile, offset: Natural, limit: Option[Positive]): Kleisli[M, Config, ReadHandle] = ???
    def read(h: ReadHandle): Kleisli[M, Config, Vector[Data]] = ???
    def close(h: ReadHandle): Kleisli[M, Config, Unit] = ???
  }

  object WriteFileModule extends WriteFileModule {
    import WriteFile._

    // TODO: Inline
    val writeHandles = KeyValueStore.Ops[WriteHandle, State, Eff]

    def open(file: AFile): Kleisli[M, Config, WriteHandle] = {
      type KC[F[_], A] = Kleisli[F, Config, A]

      for {
        ctx      <- Kleisli.ask[M, Config]
        bktCol   <- bucketCollectionFromPath(file).lift.liftM[KC]
        _        <- lift(
                      Task.delay(ctx.manager.hasBucket(bktCol.bucket).booleanValue).ifM(
                        Task.now(().right),
                        Task.now(FileSystemError.pathErr(PathError.pathNotFound(file)).left)
                    )).into[Eff].lift.liftM[KC]
        bkt      <- lift(Task.delay(
                      ctx.cluster.openBucket(bktCol.bucket)
                    )).into[Eff].lift.liftM[KC]
        i        <- MonotonicSeq.Ops[Eff].next.lift.liftM[KC]
        handle   =  WriteHandle(file, i)
        _        <- writeHandles.put(handle, State(bkt, bktCol.collection)).lift.liftM[KC]
      } yield handle
    }

    def write(h: WriteHandle, chunk: Vector[Data]): Kleisli[M, Config, Vector[FileSystemError]] = ???

    def close(h: WriteHandle): Kleisli[M, Config, Unit] = ???
  }

  object ManageFileModule extends ManageFileModule {
    import ManageFile._

    def move(scenario: MoveScenario, semantics: MoveSemantics): Kleisli[M, Config, Unit] = ???
    def delete(path: APath): Kleisli[M, Config, Unit] = ???
    def tempFile(near: APath): Kleisli[M, Config, AFile] = ???
  }
}
