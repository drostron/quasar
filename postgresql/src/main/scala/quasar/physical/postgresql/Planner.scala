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
import quasar.Planner.PlannerError
import quasar.qscript, qscript._, MapFuncs._

import matryoshka._
import scalaz._, Scalaz._
import simulacrum._

object Planner {

  def mapFunc[T[_[_]]]: Algebra[MapFunc[T, ?], String] = {
    case ToString(a1) => ""
    case _ => ""
  }

  @typeclass trait Planner[QS[_]] {
    def plan: AlgebraM[PlannerError \/ ?, QS, String]
  }
  object Planner {
    implicit def coproduct[F[_]: Planner, G[_]: Planner]: Planner[Coproduct[F, G, ?]] =
      new Planner[Coproduct[F, G, ?]] {
        def plan: AlgebraM[PlannerError \/ ?, Coproduct[F, G, ?], String] =
          _.run.fold(Planner[F].plan, Planner[G].plan)
      }
  }

  implicit def qscriptCore[T[_[_]]]: Planner[QScriptCore[T, ?]] =
    new Planner[QScriptCore[T, ?]] {
      val plan: AlgebraM[PlannerError \/ ?, QScriptCore[T, ?], String] = {
        // case qscript.Map(src, f) => "".right
        case qscript.Filter(src, f) =>
          // val mapFuncStr = f.foldMap(new (MapFunc[T, ?] ~> Const[String, ?]) {
          //   def apply[A](fa: MapFunc[T, A]): Const[String, A] = Const[String, A](mapFunc[T](fa))
          // })
          //  s"($src) where (${mapFuncStr.get})"
          s"($src) where ...".right
        case _ => "".right
      }
    }

  implicit def sourcedPathable[T[_[_]]]: Planner[SourcedPathable[T, ?]] =
    new Planner[SourcedPathable[T, ?]] {
      val plan: AlgebraM[PlannerError \/ ?, SourcedPathable[T, ?], String] = {
        case LeftShift(src, struct, repair) => "".right
        case Union(src, lBranch, rBranch) => "".right
      }
    }

  implicit def const: Planner[Const[DeadEnd, ?]] =
    new Planner[Const[DeadEnd, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, Const[DeadEnd, ?], String] = {
        case Const(Root) => "null".right
        case Const(Empty) => "".right
      }
    }

  implicit def projectBucket[T[_[_]]]: Planner[ProjectBucket[T, ?]] =
    new Planner[ProjectBucket[T, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, ProjectBucket[T, ?], String] = {
        case BucketField(src, value, name)  => "".right
        case BucketIndex(src, value, index) => "".right
      }
    }

  implicit def thetajoin[T[_[_]]]: Planner[ThetaJoin[T, ?]] =
    new Planner[ThetaJoin[T, ?]] {
      def plan: AlgebraM[PlannerError \/ ?, ThetaJoin[T, ?], String] = {
        case ThetaJoin(src, lBranch, rBranch, on, f, combine) => "".right
      }
    }


  // type QScriptProject[T[_[_]], A] = Coproduct[ProjectBucket[T, ?], QScriptPure[T, ?], A]
  // type QScriptPure[T[_[_]], A] = Coproduct[ThetaJoin[T, ?], QScriptPrim[T, ?], A]


}
