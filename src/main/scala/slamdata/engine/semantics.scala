package slamdata.engine

import slamdata.engine.analysis._
import slamdata.engine.std.Library

import scalaz._
import scalaz.std.map._
import scalaz.std.string._
import scalaz.std.list._

import scalaz.syntax.apply._

trait SemanticAnalysis {
  import slamdata.engine.sql._
  import SemanticError._

  type Failure = NonEmptyList[SemanticError]

  private def fail[A](e: SemanticError) = Validation.failure[NonEmptyList[SemanticError], A](NonEmptyList(e))
  private def succeed[A](s: A) = Validation.success[NonEmptyList[SemanticError], A](s)

  def tree(root: Node): AnnotatedTree[Node, Unit] = AnnotatedTree.unit(root, n => n.children)

  /**
   * This analyzer looks for function invocations (including operators), 
   * and binds them to their associated function definitions in the
   * provided library. If a function definition cannot be found, 
   * produces an error with details on the failure.
   */
  def FunctionBind[A](library: Library) = {
    def findFunction(name: String) = {
      val lcase = name.toLowerCase

      library.functions.find(f => f.name.toLowerCase == lcase).map(f => Validation.success(Some(f))).getOrElse(
        fail(FunctionNotFound(name))
      )
    }

    Analysis.annotate[Node, A, Option[Func], Failure] { 
      case (InvokeFunction(name, args)) => findFunction(name)          

      case (Unop(expr, op)) => findFunction(op.name)

      case (Binop(left, right, op)) => findFunction(op.name)

      case _ => Validation.success(None)
    }
  }

  final case class TableScope(scope: Map[String, SqlRelation])

  implicit val ShowTableScope = new Show[TableScope] {
    override def show(v: TableScope) = Show[Map[String, Node]].show(v.scope)
  }

  /**
   * This analysis identifies all the named tables within scope at each node in 
   * the tree. If two tables are given the same name within the same scope, then
   * because this leads to an ambiguity, an error is produced containing details
   * on the duplicate name.
   */
  def ScopeTables[A] = Analysis.readTree[Node, A, TableScope, Failure] { tree =>
    import Validation.{success, failure}

    Analysis.fork[Node, A, TableScope, Failure]((scopeOf, node) => {
      def parentScope(node: Node) = tree.parent(node).map(scopeOf).getOrElse(TableScope(Map()))

      node match {
        case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
          val parentMap = parentScope(node).scope

          (relations.foldLeft[Validation[Failure, Map[String, SqlRelation]]](success(Map.empty[String, SqlRelation])) {
            case (v, relation) =>
              implicit val sg = Semigroup.firstSemigroup[SqlRelation]

              v +++ tree.subtree(relation).foldDown[Validation[Failure, Map[String, SqlRelation]]](success(Map.empty[String, SqlRelation])) {
                case (v, relation : SqlRelation) =>
                  v.fold(
                    failure,
                    acc => {
                      val name = relation match {
                        case r @ TableRelationAST(name, aliasOpt) => Some(aliasOpt.getOrElse(name))
                        case r @ SubqueryRelationAST(subquery, alias) => Some(alias)
                        case r @ JoinRelation(left, right, join, clause) => None
                      }

                      (name.map { name =>
                        (acc.get(name).map{ relation2 =>
                          failure(NonEmptyList(DuplicateRelationName(name, relation2)))
                        }).getOrElse(success(acc + (name -> relation)))
                      }).getOrElse(success(acc))
                    }
                  )

                case (v, _) => v // We're only interested in relations                
              }
          }).map(map => TableScope(parentMap ++ map))

        case _ => success(parentScope(node))
      }
    })
  }

  sealed trait Provenance {
    def & (that: Provenance): Provenance = Provenance.Both(this, that)

    def | (that: Provenance): Provenance = Provenance.Either(this, that)
  }
  trait ProvenanceInstances {
    implicit val ProvenanceShow = new Show[Provenance] { self =>
      import Provenance._

      override def show(v: Provenance): Cord = v match {
        case Unknown => Cord("Unknown")
        case Value => Cord("Value")
        case Relation(value) => Show[Node].show(value)
        case Either(left, right) => Cord("(") ++ self.show(left) ++ Cord(" | ") ++ self.show(right) ++ Cord(")")
        case Both(left, right)  => Cord("(") ++ self.show(left) ++ Cord(" & ") ++ self.show(right) ++ Cord(")")
      }
    }
  }
  object Provenance extends ProvenanceInstances {
    case object Unknown extends Provenance
    case object Value extends Provenance
    case class Relation(value: SqlRelation) extends Provenance
    case class Either(left: Provenance, right: Provenance) extends Provenance
    case class Both(left: Provenance, right: Provenance) extends Provenance

    private def strict[A, B, C](f: (A, B) => C): (A, => B) => C = (a, b) => f(a, b)

    val BothMonoid   = Monoid.instance(strict[Provenance, Provenance, Provenance](Both.apply), Unknown)
    val EitherMonoid = Monoid.instance(strict[Provenance, Provenance, Provenance](Either.apply), Unknown)

    def allOf(xs: List[Provenance]): Provenance = Foldable[List].foldMap(xs)(identity)(BothMonoid)

    def anyOf(xs: List[Provenance]): Provenance = Foldable[List].foldMap(xs)(identity)(EitherMonoid)
  }

  /**
   * This phase infers the provenance of every expression, issuing errors
   * if identifiers are used with unknown provenance. The phase requires
   * TableScope annotations on the tree.
   */
  def ProvenanceInfer = Analysis.readTree[Node, TableScope, Provenance, Failure] { tree =>
    Analysis.join[Node, TableScope, Provenance, Failure]((provOf, node) => {
      import Validation.{success, failure}

      def propagate(n: Node) = success(provOf(n))

      def NA = success(Provenance.Unknown)

      node match {
        case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
          success(Provenance.allOf(relations.toList.map(provOf)))

        case Proj(expr, alias) => propagate(expr)

        case Subselect(select) => propagate(select)

        case SetLiteral(values) => success(Provenance.Value)

        case Wildcard => NA // FIXME

        case Binop(left, right, op) => success(provOf(left) & provOf(right))

        case Unop(expr, op) => success(provOf(expr))

        case ident @ Ident(name) => 
          val tableScope = tree.attr(node).scope

          (tableScope.get(name).map((Provenance.Relation.apply _) andThen success)).getOrElse {
            Provenance.anyOf(tableScope.values.toList.map(Provenance.Relation.apply)) match {
              case Provenance.Unknown => failure(NonEmptyList(NoTableDefined(ident)))

              case x => println(name + " provenance: " + x); success(x)
            }
          }

        case InvokeFunction(name, args) => success(Provenance.allOf(args.toList.map(provOf)))

        case Case(cond, expr) => propagate(expr)

        case Match(expr, cases, default) => success(cases.map(provOf).reduce(_ & _))

        case Switch(cases, default) => success(cases.map(provOf).reduce(_ & _))

        case IntLiteral(value) => success(Provenance.Value)

        case FloatLiteral(value) => success(Provenance.Value)

        case StringLiteral(value) => success(Provenance.Value)

        case NullLiteral() => success(Provenance.Value)

        case r @ TableRelationAST(name, alias) => success(Provenance.Relation(r))

        case r @ SubqueryRelationAST(subquery, alias) => success(Provenance.Relation(r))

        case r @ JoinRelation(left, right, tpe, clause) => success(Provenance.Relation(r))

        case GroupBy(keys, having) => success(Provenance.allOf(keys.toList.map(provOf)))

        case OrderBy(keys) => success(Provenance.allOf(keys.map(_._1).toList.map(provOf)))

        case _ : BinaryOperator => NA

        case _ : UnaryOperator => NA
      }
    })
  }

  /**
   * This phase works top-down to push out known types to types with unknowable
   * types (such as columns and wildcards). The annotation is a map, which maps from
   * child node (of the annotated node) to the type of that child node.
   */
  def TypeInfer = Analysis.readTree[Node, Option[Func], Map[Node, Type], Failure] { tree =>
    import Validation.{success, failure}

    Analysis.fork[Node, Option[Func], Map[Node, Type], Failure]((mapOf, node) => {
      /**
       * Retrieves the inferred type of the current node being annotated.
       */
      def inferredType = (for {
        parent   <- tree.parent(node)
        selfType <- mapOf(parent).get(node)
      } yield selfType).getOrElse(Type.Top)

      /**
       * Propagates the inferred type of this node to its sole child node.
       */
      def propagate(child: Node) = propagateAll(child :: Nil)

      /**
       * Propagates the inferred type of this node to its identically-typed
       * children nodes.
       */
      def propagateAll(children: Seq[Node]) = success(Map(children.map(_ -> inferredType): _*))

      /**
       * Indicates no information content for the children of this node.
       */
      def NA = success(Map.empty[Node, Type])

      node match {
        case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
          inferredType match {
            // TODO: If there's enough type information in the inferred type to do so, push it
            //       down to the projections.

            case _ => NA
          }

        case Proj(expr, alias) => propagate(expr)

        case Subselect(select) => propagate(select)

        case SetLiteral(values) => 
          inferredType match {
            // Push the set type down to the children:
            case Type.Set(tpe) => success(values.map(_ -> tpe).toMap)

            case _ => NA
          }

        case Wildcard => 
          println("* :: " + inferredType)

          NA

        case Binop(left, right, op) =>
          (tree.attr(node).map { func =>
            func.unapply(inferredType).map { types =>
              List[Node](left, right).zip(types).toMap
            }
          }).getOrElse(fail(FunctionNotBound(node)))

        case Unop(expr, op) =>
          (tree.attr(node).map { func =>
            func.unapply(inferredType).map { types =>
              List[Node](expr).zip(types).toMap
            }
          }).getOrElse(fail(FunctionNotBound(node)))

        case Ident(name) => 
          println(name + " :: " + inferredType)
          NA

        case InvokeFunction(name, args) =>
          (tree.attr(node).map { func =>
            func.unapply(inferredType).map { types =>
              List[Node](args: _*).zip(types).toMap
            }
          }).getOrElse(fail(FunctionNotBound(node)))

        case Case(cond, expr) => propagate(expr)

        case Match(expr, cases, default) => propagateAll(cases ++ default)

        case Switch(cases, default) => propagateAll(cases ++ default)

        case IntLiteral(value) => NA

        case FloatLiteral(value) => NA

        case StringLiteral(value) => NA

        case NullLiteral() => NA

        case TableRelationAST(name, alias) => NA

        case SubqueryRelationAST(subquery, alias) => propagate(subquery)

        case JoinRelation(left, right, tpe, clause) => NA

        case GroupBy(keys, having) => NA

        case OrderBy(keys) => NA

        case _ : BinaryOperator => NA

        case _ : UnaryOperator => NA
      }
    })
  }

  /**
   * This phase works bottom-up to check the type of all expressions.
   * In the event of a type error, an error will be produced containing
   * details on the expected versus actual type.
   */
  def TypeCheck = {
    Analysis.readTree[Node, Option[Func], Type, Failure] { tree =>
      Analysis.join[Node, Option[Func], Type, Failure]((typeOf, node) => {
        def succeed(v: Type): ValidationNel[SemanticError, Type] = Validation.success(v)

        def fail(error: SemanticError): ValidationNel[SemanticError, Type] = Validation.failure(NonEmptyList(error))

        def func(node: Node): ValidationNel[SemanticError, Func] = {
          tree.attr(node).map(Validation.success).getOrElse(Validation.failure(NonEmptyList(FunctionNotBound(node))))
        }

        def NA = succeed(Type.Bottom)

        def propagate(n: Node) = succeed(typeOf(n))

        node match {
          case SelectStmt(projections, relations, filter, groupBy, orderBy, limit, offset) =>
            // TODO: Use object instead of array so we can hang onto names:
            succeed(Type.makeArray(projections.map(typeOf)))

          case Proj(expr, alias) => propagate(expr)

          case Subselect(select) => propagate(select)

          case SetLiteral(values) => succeed(Type.makeArray(values.map(typeOf)))

          case Wildcard => succeed(Type.Top)

          case Binop(left, right, op) =>
            func(node).fold(
              Validation.failure,
              _.apply(typeOf(left), typeOf(right))
            )

          case Unop(expr, op) =>
            func(node).fold(
              Validation.failure,
              _.apply(typeOf(expr))
            )

          case Ident(name) => ???

          case InvokeFunction(name, args) =>
            func(node).fold(
              Validation.failure,
              _.apply(args.toList.map(typeOf))
            )

          case Case(cond, expr) => succeed(typeOf(expr))

          case Match(expr, cases, default) => 
            succeed(cases.map(typeOf).foldLeft[Type](Type.Top)(_ | _).lub)

          case Switch(cases, default) => 
            succeed(cases.map(typeOf).foldLeft[Type](Type.Top)(_ | _).lub)

          case IntLiteral(value) => succeed(Type.Const(Data.Int(value)))

          case FloatLiteral(value) => succeed(Type.Const(Data.Dec(value)))

          case StringLiteral(value) => succeed(Type.Const(Data.Str(value)))

          case NullLiteral() => succeed(Type.Const(Data.Null))

          case TableRelationAST(name, alias) => NA

          case SubqueryRelationAST(subquery, alias) => propagate(subquery)

          case JoinRelation(left, right, tpe, clause) => succeed(Type.Bool)

          case GroupBy(keys, having) => succeed(Type.makeArray(keys.map(typeOf)))

          case OrderBy(keys) => NA

          case _ : BinaryOperator => NA

          case _ : UnaryOperator => NA
        }
      })
    }
  }

}
object SemanticAnalysis extends SemanticAnalysis