package roc.types.integrations

import java.time._
import java.time.temporal.{ChronoField, JulianFields}

import org.scalacheck._
import org.scalacheck.Prop.forAll
import org.scalacheck.Arbitrary.arbitrary
import org.specs2._
import roc.integrations.Client
import roc.postgresql.{ElementDecoder, Request, Text}
import com.twitter.util.Await
import roc.types._
import roc.types.decoders._
import jawn.ast.{JNull, JParser, JValue}

class BinaryDecodersIntegrationSpec extends Specification with Client with ScalaCheck {

  //need a more sensible BigDecimal generator, because ScalaCheck goes crazy with it and we can't even stringify them
  //this will be sufficient to test the decoder
  implicit val arbBD: Arbitrary[BigDecimal] = Arbitrary(for {
    precision <- Gen.choose(1, 32)
    scale <- Gen.choose(-32, 32)
    digits <- Gen.listOfN[Char](precision, Gen.numChar)
  } yield BigDecimal(BigDecimal(digits.mkString).bigDecimal.movePointLeft(scale)))

  implicit val arbCirce: Arbitrary[_root_.io.circe.Json] = JsonGenerators.arbJson
  //arbitrary Json for Jawn
  implicit val arbJson = Arbitrary[Json](Gen.resize(4, for {
    circe <- arbitrary[_root_.io.circe.Json]
  } yield JParser.parseFromString(circe.noSpaces).toOption.getOrElse(JNull)))

  implicit val arbDate = Arbitrary[Date](for {
    julian <- Gen.choose(1721060, 5373484)  //Postgres date parser doesn't like dates outside year range 0000-9999
  } yield LocalDate.now().`with`(JulianFields.JULIAN_DAY, julian))

  implicit val arbTime = Arbitrary[Time](for {
    usec <- Gen.choose(0L, 24L * 60 * 60 * 1000000 - 1)
  } yield LocalTime.ofNanoOfDay(usec * 1000))

  implicit val arbTimestampTz = Arbitrary[TimestampWithTZ](for {
    milli <- Gen.posNum[Long]
  } yield ZonedDateTime.ofInstant(Instant.ofEpochMilli(milli), ZoneId.systemDefault()))

  def is = sequential ^ s2"""
  Decoders
    must decode string from binary $testString
    must decode short from binary $testShort
    must decode int from binary $testInt
    must decode long from binary $testLong
    must decode float from binary $testFloat
    must decode double from binary $testDouble
    must decode numeric from binary $testNumeric
    must decode boolean from binary $testBoolean
    must decode json from binary $testJson
    must decode jsonb from binary $testJsonb
    must decode date from binary $testDate
    must decode time from binary $testTime
    must decode timestamptz from binary $testTimestampTz
  """

  def test[T : Arbitrary : ElementDecoder](
    send: String,
    typ: String,
    toStr: T => String = (t: T) => t.toString,
    tester: (T, T) => Boolean = (a: T, b: T) => a == b) = forAll {
    (t: T) =>
      //TODO: change this once prepared statements are available
      val escaped = toStr(t).replaceAllLiterally("'", "\\'")
      val query = s"SELECT $send('$escaped'::$typ) AS out"
      val result = Await.result(Postgres.query(Request(query)))
      val Text(name, oid, string) = result.head.get('out)
      val bytes = string.stripPrefix("\\x").sliding(2,2).toArray.map(Integer.parseInt(_, 16)).map(_.toByte)
      val out = implicitly[ElementDecoder[T]].binaryDecoder(bytes)
      tester(t, out)
  }

  def testString = test[String]("textsend", "text")
  def testShort = test[Short]("int2send", "int2")
  def testInt = test[Int]("int4send", "int4")
  def testLong = test[Long]("int8send", "int8")
  def testFloat = test[Float]("float4send", "float4")
  def testDouble = test[Double]("float8send", "float8")
  def testNumeric = test[BigDecimal]("numeric_send", "numeric", _.bigDecimal.toPlainString)
  def testBoolean = test[Boolean]("boolsend", "boolean")
  def testJson = test[Json]("json_send", "json")
  def testJsonb = test[Json]("jsonb_send", "jsonb")
  def testDate = test[Date]("date_send", "date")
  def testTime = test[Time]("time_send", "time")
  def testTimestampTz = test[TimestampWithTZ](
    "timestamptz_send",
    "timestamptz",
    ts => java.sql.Timestamp.from(ts.toInstant).toString,
    (a, b) => a.getLong(ChronoField.MICRO_OF_DAY) == b.getLong(ChronoField.MICRO_OF_DAY)  //postgres only keeps microsecond precision
  )

  //disabled - test server might not have hstore extension
  def testHstore = test[HStore]("hstore_send", "hstore", { strMap =>
    strMap.map {
      case (k, v) =>
        s""""${k.replaceAllLiterally("\"", "\\\"")}"=>"${v.getOrElse("NULL").replaceAllLiterally("\"", "\\\"")}""""
    }.mkString(",")
  })
}
