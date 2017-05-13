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
import quasar.connector.EnvironmentError, EnvironmentError.{connectionFailed, invalidCredentials}
import quasar.contrib.pathy.APath
import quasar.effect.{KeyValueStore, MonotonicSeq}
import quasar.effect.uuid.GenUUID
import quasar.fp._, free._, ski.κ
import quasar.fs._, ReadFile.ReadHandle, WriteFile.WriteHandle, QueryFile.ResultHandle
import quasar.fs.mount._, FileSystemDef.DefErrT
import quasar.physical.couchbase.common.{BucketCollection, Context, Cursor}

import java.net.ConnectException
import java.time.Duration
import scala.math

import com.couchbase.client.java.CouchbaseCluster
import com.couchbase.client.java.env.{CouchbaseEnvironment, DefaultCouchbaseEnvironment}
import com.couchbase.client.java.error.InvalidPasswordException
import com.couchbase.client.java.util.features.Version
import org.http4s.Uri
import scalaz._, Scalaz._
import scalaz.concurrent.Task

// TODO: Injection? Parameterized queries could help but don't appear to handle bucket names within ``
// TODO: Handle query returned errors field

package object fs {
  import Couchbase.{Eff, M}
  import implicits._

  val FsType = FileSystemType("couchbase")

  val minimumRequiredVersion = new Version(4, 5, 1)

  val defaultSocketConnectTimeout: Duration = Duration.ofSeconds(1)
  val defaultQueryTimeout: Duration         = Duration.ofSeconds(300)

  object CBConnectException {
    def unapply(ex: Throwable): Option[ConnectException] =
      ex.getCause match {
        case ex: ConnectException => ex.some
        case _                    => none
      }
  }

  // TODO: Re-home now that single usage is from Couchbase?
  def context(connectionUri: ConnectionUri): DefErrT[Task, Context] = {
    final case class ConnUriParams(user: String, pass: String, socketConnectTimeout: Duration, queryTimeout: Duration)

    def env(p: ConnUriParams): Task[CouchbaseEnvironment] = Task.delay(
      DefaultCouchbaseEnvironment
        .builder()
        .socketConnectTimeout(
          math.min(p.socketConnectTimeout.toMillis, Int.MaxValue.toLong).toInt)
        .queryTimeout(p.queryTimeout.toMillis)
        .build())

    def duration(uri: Uri, name: String, default: Duration) =
      (uri.params.get(name) ∘ (parseLong(_) ∘ Duration.ofSeconds))
         .getOrElse(default.success)
         .leftMap(κ(s"$name must be a valid long".wrapNel))

    for {
      uri     <- Uri.fromString(connectionUri.value).leftMap(_.message.wrapNel).liftDT
      params  <- (
                   uri.params.get("username").toSuccessNel("No username in ConnectionUri")   |@|
                   uri.params.get("password").toSuccessNel("No password in ConnectionUri")   |@|
                   duration(uri, "socketConnectTimeoutSeconds", defaultSocketConnectTimeout) |@|
                   duration(uri, "queryTimeoutSeconds", defaultQueryTimeout)
                 )(ConnUriParams).disjunction.liftDT
      ev      <- env(params).liftM[DefErrT]
      cluster <- EitherT(Task.delay(
                   CouchbaseCluster.fromConnectionString(ev, uri.renderString).right
                 ).handle {
                   case e: Exception => e.getMessage.wrapNel.left[EnvironmentError].left
                 })
      cm      =  cluster.clusterManager(params.user, params.pass)
      _       <- EitherT(Task.delay(
                   (cm.info.getMinVersion.compareTo(minimumRequiredVersion) >= 0).unlessM(
                     s"Couchbase Server must be ${minimumRequiredVersion}+"
                       .wrapNel.left[EnvironmentError].left)
                 ).handle {
                   case _: InvalidPasswordException =>
                     invalidCredentials(
                       "Unable to obtain a ClusterManager with provided credentials."
                     ).right[NonEmptyList[String]].left
                   case CBConnectException(ex) =>
                     connectionFailed(
                       ex
                     ).right[NonEmptyList[String]].left
                 })
    } yield Context(cluster, cm)
  }

  def interp(ctx: Context): DefErrT[Task, (M ~> Task, Task[Unit])] = {
    val taskInterp: Task[(Free[Eff, ?] ~> Task, Task[Unit])]  =
      (TaskRef(Map.empty[ReadHandle,   Cursor]) |@|
       TaskRef(Map.empty[WriteHandle,  State])  |@|
       TaskRef(Map.empty[ResultHandle, Cursor]) |@|
       TaskRef(0L)                              |@|
       GenUUID.type1[Task]
      )((kvR, kvW, kvQ, i, genUUID) => (
        foldMapNT[Couchbase.Eff, Task](
          reflNT[Task]                        :+:
          MonotonicSeq.fromTaskRef(i)         :+:
          genUUID                             :+:
          KeyValueStore.impl.fromTaskRef(kvR) :+:
          KeyValueStore.impl.fromTaskRef(kvW) :+:
          KeyValueStore.impl.fromTaskRef(kvQ)),
        Task.delay(ctx.cluster.disconnect()).void))

    def f = λ[M ~> Free[Eff, ?]](_.run.run >>= {
      case (_, errA) => lift(errA.fold(e => Task.fail(new RuntimeException(e.shows)), Task.now)).into
    })

    (taskInterp ∘ (_.leftMap(_ compose f))).liftM[DefErrT]
  }

  // TODO: remove
  def bucketCollectionFromPath(p: APath): FileSystemError \/ BucketCollection =
    BucketCollection.fromPath(p) leftMap (FileSystemError.pathErr(_))
}
