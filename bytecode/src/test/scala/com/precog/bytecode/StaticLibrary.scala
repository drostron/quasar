/*
 *  ____    ____    _____    ____    ___     ____ 
 * |  _ \  |  _ \  | ____|  / ___|  / _/    / ___|        Precog (R)
 * | |_) | | |_) | |  _|   | |     | |  /| | |  _         Advanced Analytics Engine for NoSQL Data
 * |  __/  |  _ <  | |___  | |___  |/ _| | | |_| |        Copyright (C) 2010 - 2013 SlamData, Inc.
 * |_|     |_| \_\ |_____|  \____|   /__/   \____|        All Rights Reserved.
 *
 * This program is free software: you can redistribute it and/or modify it under the terms of the 
 * GNU Affero General Public License as published by the Free Software Foundation, either version 
 * 3 of the License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY; 
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See 
 * the GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License along with this 
 * program. If not, see <http://www.gnu.org/licenses/>.
 *
 */
package com.precog.bytecode

trait StaticLibrary extends Library {
  case class Morphism(namespace: Vector[String], name: String, opcode: Int, arity: Arity) extends MorphismLike
  case class Op1(namespace: Vector[String], name: String, opcode: Int) extends Op1Like
  case class Op2(namespace: Vector[String], name: String, opcode: Int) extends Op2Like
  case class Reduction(namespace: Vector[String], name: String, opcode: Int) extends ReductionLike

  lazy val libReduction = Set(
    Reduction(Vector(), "count", 0x0000),
    Reduction(Vector(), "max", 0x0001),
    Reduction(Vector(), "min", 0x0004),
    Reduction(Vector(), "sum", 0x0002),
    Reduction(Vector(), "mean", 0x0013),
    Reduction(Vector(), "geometricMean", 0x0003),
    Reduction(Vector(), "sumSq", 0x0005),
    Reduction(Vector(), "variance", 0x0006),
    Reduction(Vector(), "stdDev", 0x0007),
    Reduction(Vector(), "median", 0x0008),
    Reduction(Vector(), "mode", 0x0009),
    Reduction(Vector("std", "lib"), "sum", 0x0010),
    Reduction(Vector("ack"), "ook", 0x0011),
    Reduction(Vector("one", "two", "three"), "qnd", 0x0012))
  
  lazy val lib1 = Set(
    Op1(Vector(), "bin", 0x0000),
    Op1(Vector("std"), "bin", 0x0001),
    Op1(Vector("std"), "lib", 0x0004),     // weird shadowing ahoy!
    Op1(Vector(), "bar", 0x0002),
    Op1(Vector("std", "lib"), "baz", 0x0003))
  
  lazy val lib2 = Set(
    Op2(Vector(), "bin2", 0x0000),
    Op2(Vector("std"), "bin2", 0x0001),
    Op2(Vector(), "bar2", 0x0002),
    Op2(Vector("std", "lib"), "baz2", 0x0003))
  
  lazy val libMorphism = Set(
    Morphism(Vector(), "bin5", 0x0000, Arity.One),
    Morphism(Vector("std"), "bin9", 0x0001, Arity.Two),
    Morphism(Vector(), "bar33", 0x0002, Arity.One),
    Morphism(Vector("std", "lib9"), "baz2", 0x0003, Arity.Two))
}
