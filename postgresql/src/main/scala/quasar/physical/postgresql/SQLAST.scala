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

sealed trait SQLAST

object SQLAST {
  final case class Literal(v: SQLData) extends SQLAST
}

sealed trait SQLData

object SQLData {
  // where A is a primitive type
  // TODO: do we add the notion of dimensionality?
  final case class  PostgreSQLArr[A](dimension: List[scala.Int], value: Vector[A]) extends SQLData
  final case class  PostgreSQLByteArray(value: List[scala.Byte])                   extends SQLData
  final case class  PostgreSQLVarLenBitStr(value: List[scala.Byte])                extends SQLData
  final case object SQLNull                                                        extends SQLData
  final case class  SQLBool(value: Boolean)                                        extends SQLData
  final case class  SQLChar(value: List[scala.Char])                               extends SQLData
  final case class  SQLStr(value: String)                                          extends SQLData
  // TODO: expand to actual sql numeric types
  final case class  SQLDec(value: BigDecimal)                                      extends SQLData
  final case class  SQLInt(value: BigInt)                                          extends SQLData
}

object SQLTypes {
  sealed trait SQLType
  final case class BigInt() extends SQLType          // signed eight-byte integer
  final case class BigSerial() extends SQLType       // autoincrementing eight-byte integer
  final case class FixLenBitString() extends SQLType // fixed-length bit string
  final case class VarBitString() extends SQLType    // variable-length bit string
  final case class Bool() extends SQLType            // logical Boolean (true/false)
  final case class Box() extends SQLType             // rectangular box on a plane
  final case class Bytea() extends SQLType           // binary data ("byte array")
  final case class FixLenChar() extends SQLType      // [ (n) ]	char [ (n) ]	fixed-length character string
  final case class VarLenChar() extends SQLType     // varying [ (n) ]	varchar [ (n) ]	variable-length character string
  final case class cidr() extends SQLType          //	 	IPv4 or IPv6 network address
  final case class circle() extends SQLType        //	 	circle on a plane
  final case class date() extends SQLType          //	 	calendar date (year, month, day)
  final case class double() extends SQLType        // precision	float8	double precision floating-point number (8 bytes)
  final case class inet() extends SQLType          //	 	IPv4 or IPv6 host address
  final case class integer() extends SQLType       //	int, int4	signed four-byte integer
  final case class interval() extends SQLType      // [ fields ] [ (p) ]	 	time span
  final case class json() extends SQLType          //	 	textual JSON data
  final case class jsonb() extends SQLType         //	 	binary JSON data, decomposed
  final case class line() extends SQLType          //	 	infinite line on a plane
  final case class lseg() extends SQLType          //	 	line segment on a plane
  final case class macaddr() extends SQLType       //	 	MAC (Media Access Control) address
  final case class money() extends SQLType         //	 	currency amount
  final case class numeric() extends SQLType       // [ (p, s) ]	decimal [ (p, s) ]	exact numeric of selectable precision
  final case class path() extends SQLType          //	 	geometric path on a plane
  final case class pg_lsn() extends SQLType        //	 	PostgreSQL Log Sequence Number
  final case class point() extends SQLType         //	 	geometric point on a plane
  final case class polygon() extends SQLType       //	 	closed geometric path on a plane
  final case class real() extends SQLType          //	float4	single precision floating-point number (4 bytes)
  final case class smallint() extends SQLType      //	int2	signed two-byte integer
  final case class smallserial() extends SQLType   //	serial2	autoincrementing two-byte integer
  final case class serial() extends SQLType        //	serial4	autoincrementing four-byte integer
  final case class text() extends SQLType          //	 	variable-length character string
  final case class time() extends SQLType          // [ (p) ] [ without time zone ]	 	time of day (no time zone)
  // final case class time() extends SQLType          // [ (p) ] with time zone	timetz	time of day, including time zone
  final case class timestamp() extends SQLType     // [ (p) ] [ without time zone ]	 	date and time (no time zone)
  // final case class timestamp() extends SQLType     // [ (p) ] with time zone	timestamptz	date and time, including time zone
  final case class tsquery() extends SQLType       //	 	text search query
  final case class tsvector() extends SQLType      //	 	text search document
  final case class txid_snapshot() extends SQLType //	 	user-level transaction ID snapshot
  final case class uuid() extends SQLType          //	 	universally unique identifier
  final case class xml() extends SQLType           //	 	XML data
}
