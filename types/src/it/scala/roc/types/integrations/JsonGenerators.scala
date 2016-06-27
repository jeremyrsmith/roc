package roc.types.integrations

import io.circe.{Json, JsonObject}
import io.circe.syntax._
import org.scalacheck.Arbitrary._
import org.scalacheck.{Arbitrary, Gen}

object JsonGenerators {

  val genJsonScalarValue : Gen[Json] = Gen.oneOf(
    arbitrary[Long] map (_.asJson),
    Gen.choose(Int.MinValue.toDouble, Int.MaxValue.toDouble) map (_.asJson),  //avoid parsing craziness
    Gen.alphaStr map Json.fromString,
    arbitrary[Boolean] map Json.fromBoolean
  )

  def genJsonArray(genValue: Gen[Json]) = Gen.listOf(genValue).map(Json.arr(_:_*))

  def genJsonObject(genValue: Gen[Json]) = Gen.nonEmptyMap(for {
    keySize <- Gen.choose(10, 20)
    key <- Gen.resize(keySize, Gen.nonEmptyListOf(Gen.alphaNumChar).map(_.mkString))
    value <- genValue
  } yield (key, value)) map JsonObject.fromMap map Json.fromJsonObject

  def genJson(level: Int) : Gen[Json] = if(level == 0)
    genJsonScalarValue
  else Gen.oneOf(
    genJsonScalarValue,
    genJsonArray(genJson(level - 1)),
    genJsonObject(genJson(level - 1)))

  implicit val arbJson : Arbitrary[Json] = Arbitrary(for {
    size <- Gen.size
    level <- Gen.choose(0, size)
    json <- genJson(level)
  } yield json)


}
