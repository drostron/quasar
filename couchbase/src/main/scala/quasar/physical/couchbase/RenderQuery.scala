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

package quasar.physical.couchbase

import quasar.Predef._
import quasar.{Data => QData, _}
import quasar.Planner.{NonRepresentableData, PlannerError}
import quasar.common.SortDir, SortDir.{Ascending, Descending}
import quasar.DataCodec.Precise.{TimeKey, TimestampKey}

import matryoshka._
import matryoshka.implicits._
import scalaz._, Scalaz._

object RenderQuery {
  import N1QL._, Case._, Select._

  implicit val codec = common.CBDataCodec

  def compact[T[_[_]]: BirecursiveT](a: T[N1QL]): PlannerError \/ String = {
    val q = a.cataM(alg)

    a.project match {
      case s: Select[T[N1QL]] => q ∘ (c => Cord("select value v from ", c, " v").toString)
      case _                  => q ∘ (c => Cord("select value ", c).toString)
    }
  }

  def func(name: Cord, params: Cord*): Cord =
    Cord(name, "(", params.toList.intercalate(", "), ")")

  def unary(op: Cord, a1: Cord): Cord =
    Cord("(", op, " ", a1, ")")

  def binary(a1: Cord, op: Cord, a2: Cord): Cord =
    Cord("(", a1, " ", op, " ", a2, ")")

  def escape(s: String): Cord =
    Cord.fromStrings(s.toVector.map {
      case ''' => "''"
      case '\\' => "\\\\"
      case '"'  => "\\u0022"
      case '\b' => "\\b"
      case '\f' => "\\f"
      case '\n' => "\\n"
      case '\r' => "\\r"
      case '\t' => "\\t"
      case v => v.toString
    })

  val alg: AlgebraM[PlannerError \/ ?, N1QL, Cord] = {
    case Data(QData.Str(v)) =>
      Cord("'", escape(v), "'").right
    case Data(v) =>
      DataCodec.render(v).leftAs(NonRepresentableData(v)) ∘ (Cord(_))
    case Id(v) =>
      Cord(s"`$v`").right
    case Obj(m) =>
      Cord(
        "{",
        m.map {
          case (k, v) => k ⊹ ": " ⊹ v
        }.toList.intercalate(", "),
        "}").right
    case Arr(l) =>
      Cord("[", l.intercalate(", "), "]").right
    case Time(a1) =>
      Cord("{ '", TimeKey, "': ", a1, " }").right
    case Timestamp(a1) =>
      Cord("{ '", TimestampKey, "': ", a1, " }").right
    case Null() =>
      Cord("null").right
    case SelectField(a1, a2) =>
      Cord(a1, ".[", a2, "]").right
    case SelectElem(a1, a2) =>
      Cord(a1, "[", a2, "]").right
    case Slice(a1, a2) =>
      Cord(a1, ":", ~a2).right
    case ConcatStr(a1, a2) =>
      binary(a1, "||", a2).right
    case Not(a1) =>
      unary("not", a1).right
    case Eq(a1, a2) =>
      binary(a1, "=", a2).right
    case Neq(a1, a2) =>
      binary(a1, "!=", a2).right
    case Lt(a1, a2) =>
      binary(a1, "<", a2).right
    case Lte(a1, a2) =>
      binary(a1, "<=", a2).right
    case Gt(a1, a2) =>
      binary(a1, ">", a2).right
    case Gte(a1, a2) =>
      binary(a1, ">=", a2).right
    case IsNull(a1) =>
      Cord("(", a1, " is null)").right
    case IsNotNull(a1) =>
      Cord("(", a1, " is not null)").right
    case Neg(a1) =>
      unary("-", a1).right
    case Add(a1, a2) =>
      binary(a1, "+", a2).right
    case Sub(a1, a2) =>
      binary(a1, "-", a2).right
    case Mult(a1, a2) =>
      binary(a1, "*", a2).right
    case Div(a1, a2) =>
      binary(a1, "/", a2).right
    case Mod(a1, a2) =>
      binary(a1, "%", a2).right
    case And(a1, a2) =>
      binary(a1, "and", a2).right
    case Or(a1, a2) =>
      binary(a1, "or", a2).right
    case Meta(a1) =>
      func("meta", a1).right
    case ConcatArr(a1, a2) =>
      func("array_concat", a1, a2).right
    case ConcatObj(a1, a2) =>
      func("object_concat", a1, a2).right
    case IfNull(a) =>
      func("ifnull", a.toList: _*).right
    case IfMissing(a) =>
      func("ifmissing", a.toList: _*).right
    case IfMissingOrNull(a) =>
      func("ifmissingornull", a.toList: _*).right
    case Type(a1) =>
      func("type", a1).right
    case ToString(a1) =>
      func("tostring", a1).right
    case ToNumber(a1) =>
      func("tonumber", a1).right
    case Floor(a1) =>
      func("floor", a1).right
    case Length(a1) =>
      func("length", a1).right
    case LengthArr(a1) =>
      func("array_length", a1).right
    case LengthObj(a1) =>
      func("object_length", a1).right
    case IsString(a1) =>
      func("isstring", a1).right
    case Lower(a1) =>
      func("lower", a1).right
    case Upper(a1) =>
      func("upper", a1).right
    case Split(a1, a2) =>
      func("split", a1, a2).right
    case Substr(a1, a2, a3) =>
      val l = ~(a3 ∘ (Cord(", ", _)))
      func("substr", a1, a2 ⊹ l).right
    case RegexContains(a1, a2) =>
      func("regex_contains", a1, a2).right
    case Least(a) =>
      func("least", a.toList: _*).right
    case Pow(a1, a2) =>
      func("power", a1, a2).right
    case Ceil(a1) =>
      func("ceil", a1).right
    case Millis(a1) =>
      func("millis", a1).right
    case MillisToUTC(a1, a2) =>
      val fmt = ~(a2 ∘ (Cord(", ", _)))
      func("millis_to_utc", a1 ⊹ fmt).right
    case DatePartStr(a1, a2) =>
      func("date_part_str", a1, a2).right
    case DateDiffStr(a1, a2, a3) =>
      func("date_diff_str", a1, a2, a3).right
    case NowStr() =>
      func("now_str").right
    case ArrContains(a1, a2) =>
      func("array_contains", a1, a2).right
    case ArrRange(a1, a2, a3) =>
      val step = ~(a3 ∘ (Cord(", ", _)))
      func("array_range", a1, a2 ⊹ step).right
    case IsArr(a1) =>
      func("isarray", a1).right
    case ObjNames(a1) =>
      func("object_names", a1).right
    case ObjValues(a1) =>
      func("object_values", a1).right
    case ObjRemove(a1, a2) =>
      func("object_remove", a1, a2).right
    case IsObj(a1) =>
      func("isobject", a1).right
    case Avg(a1) =>
      func("avg", a1).right
    case Count(a1) =>
      func("count", a1).right
    case Max(a1) =>
      func("max", a1).right
    case Min(a1) =>
      func("min", a1).right
    case Sum(a1) =>
      func("sum", a1).right
    case ArrAgg(a1) =>
      func("array_agg", a1).right
    case Union(a1, a2) =>
      binary(a1, "union", a2).right
    case ArrFor(a1, a2, a3) =>
      Cord("(array ", a1, " for ", a2, " in ", a3, " end)").right
    case Select(v, re, ks, un, ft, gb, ob) =>
      def alias(a: Option[Id[Cord]]) = ~(a ∘ (i => s" as `${i.v}`"))
      val value       = v.v.fold[Cord]("value ", "")
      val resultExprs = (re ∘ (r => r.expr ⊹ alias(r.alias))).intercalate(", ")
      val kSpace      = ~(ks ∘ (k => Cord(" from ", k.expr, alias(k.alias))))
      val unnest      = ~(un ∘ (u => Cord(" unnest ", u.expr, alias(u.alias))))
      val filter      = ~(ft ∘ (f  => Cord(" where ", f.v)))
      val groupBy     = ~(gb ∘ (g  => Cord(" group by ", g.v)))
      val orderBy     =
        ~((ob ∘ {
          case OrderBy(a, Ascending)  => Cord(a, " ASC")
          case OrderBy(a, Descending) => Cord(a, " DESC")
        }).toNel ∘ (o => Cord(" order by ", o.intercalate(", "))))
      Cord("(select ", value, resultExprs, kSpace, unnest, filter, groupBy, orderBy, ")").right
    case Case(wt, e) =>
      val wts = wt ∘ { case WhenThen(w, t) => Cord("when ", w, " then ", t) }
      Cord("(case ", wts.intercalate(" "), " else ", e.v, " end)").right
  }
}
