package roc
package types

import cats.data.{Validated, Xor}
import io.netty.buffer.Unpooled
import java.nio.{BufferUnderflowException, ByteBuffer}
import java.nio.charset.StandardCharsets
import java.time.format.{DateTimeFormatter, DateTimeFormatterBuilder}
import java.time.temporal.{ChronoField, JulianFields}
import java.time._

import jawn.ast.JParser
import roc.postgresql.ElementDecoder
import roc.types.failures._

import scala.util.Failure

object decoders {

  implicit def optionElementDecoder[A](implicit f: ElementDecoder[A]) = 
    new ElementDecoder[Option[A]] {
      def textDecoder(text: String): Option[A]         = Some(f.textDecoder(text))
      def binaryDecoder(bytes: Array[Byte]): Option[A] = Some(f.binaryDecoder(bytes))
      def nullDecoder(): Option[A]                     = None
    }

  implicit val stringElementDecoder: ElementDecoder[String] = new ElementDecoder[String] {
    def textDecoder(text: String): String         = text
    def binaryDecoder(bytes: Array[Byte]): String = new String(bytes, StandardCharsets.UTF_8)
    def nullDecoder(): String                     = throw new NullDecodedFailure("STRING")
  }

  implicit val shortElementDecoder: ElementDecoder[Short] = new ElementDecoder[Short] {
    def textDecoder(text: String): Short         = Xor.catchNonFatal(
      text.toShort
    ).fold(
      {l => throw new ElementDecodingFailure("SHORT", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Short = Xor.catchNonFatal({
      val buffer = Unpooled.buffer(2)
      buffer.writeBytes(bytes.take(2))
      buffer.readShort
    }).fold(
      {l => throw new ElementDecodingFailure("SHORT", l)},
      {r => r}
    )
    def nullDecoder(): Short                     = throw new NullDecodedFailure("SHORT")
  }

  implicit val intElementDecoder: ElementDecoder[Int] = new ElementDecoder[Int] {
    def textDecoder(text: String): Int         = Xor.catchNonFatal(
      text.toInt
    ).fold(
      {l => throw new ElementDecodingFailure("INT", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Int = Xor.catchNonFatal({
      val buffer = Unpooled.buffer(4)
      buffer.writeBytes(bytes.take(4))
      buffer.readInt
    }).fold(
      {l => throw new ElementDecodingFailure("INT", l)},
      {r => r}
    )
    def nullDecoder(): Int =                     throw new NullDecodedFailure("INT")
  }

  implicit val longElementDecoder: ElementDecoder[Long] = new ElementDecoder[Long] {
    def textDecoder(text: String): Long         = Xor.catchNonFatal(
      text.toLong
    ).fold(
      {l => throw new ElementDecodingFailure("LONG", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Long = Xor.catchNonFatal({
      val buffer = Unpooled.buffer(8)
      buffer.writeBytes(bytes.take(8))
      buffer.readLong
    }).fold(
      {l => throw new ElementDecodingFailure("LONG", l)},
      {r => r}
    )
    def nullDecoder: Long                       = throw new NullDecodedFailure("LONG") 
  }

  implicit val floatElementDecoder: ElementDecoder[Float] = new ElementDecoder[Float] {
    def textDecoder(text: String): Float         = Xor.catchNonFatal(
      text.toFloat
    ).fold(
      {l => throw new ElementDecodingFailure("FLOAT", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Float = Xor.catchNonFatal({
      val buffer = Unpooled.buffer(4)
      buffer.writeBytes(bytes.take(4))
      buffer.readFloat
    }).fold(
      {l => throw new ElementDecodingFailure("FLOAT", l)},
      {r => r}
    )
    def nullDecoder: Float                       = throw new NullDecodedFailure("FLOAT")
  }

  implicit val doubleElementDecoder: ElementDecoder[Double] = new ElementDecoder[Double] {
    def textDecoder(text: String): Double         = Xor.catchNonFatal(
      text.toDouble
    ).fold(
      {l => throw new ElementDecodingFailure("DOUBLE", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Double = Xor.catchNonFatal({
      val buffer = Unpooled.buffer(8)
      buffer.writeBytes(bytes.take(8))
      buffer.readDouble
    }).fold(
      {l => throw new ElementDecodingFailure("DOUBLE", l)},
      {r => r}
    )
    def nullDecoder: Double                       = throw new NullDecodedFailure("DOUBLE")
  }

  implicit val bigDecimalDecoder: ElementDecoder[BigDecimal] = new ElementDecoder[BigDecimal] {

    def textDecoder(text: String): BigDecimal = Xor.catchNonFatal(BigDecimal(text)).fold(
      l => throw new ElementDecodingFailure("NUMERIC", l),
      identity
    )

    private val NUMERIC_POS = 0x0000
    private val NUMERIC_NEG = 0x4000
    private val NUMERIC_NAN = 0xC000
    private val NUMERIC_NULL = 0xF000
    private val NumericHeaderSize = 16
    private val NumericDigitBaseExponent = 4
    private val NumericDigitBase = Math.pow(10, NumericDigitBaseExponent) // 10,000

    /**
      * Read only 16 bits, but returned the unsigned number as a 32-bit Integer
      *
      * @param buf The Byte Buffer
      */
    private def getUnsignedShort(buf: ByteBuffer) = {
      val high = buf.get().toInt
      val low = buf.get()
      (high << 8) | low
    }

    def binaryDecoder(bytes: Array[Byte]): BigDecimal = Xor.catchNonFatal {
      val buf = ByteBuffer.wrap(bytes)
      val len = getUnsignedShort(buf)
      val weight = buf.getShort()
      val sign = getUnsignedShort(buf)
      val displayScale = getUnsignedShort(buf)

      //digits are actually unsigned base-10000 numbers (not straight up bytes)
      val digits = new Array[Short](len)
      buf.asShortBuffer().get(digits)
      val bdDigits = digits.map(BigDecimal(_))

      if(bdDigits.length > 0) {
        val unscaled = bdDigits.tail.foldLeft(bdDigits.head) {
          case (accum, digit) => BigDecimal(accum.bigDecimal.scaleByPowerOfTen(NumericDigitBaseExponent)) + digit
        }

        val firstDigitSize =
          if (digits.head < 10) 1
          else if (digits.head < 100) 2
          else if (digits.head < 1000) 3
          else 4

        val scaleFactor = if (weight >= 0)
          weight * NumericDigitBaseExponent + firstDigitSize
        else
          weight * NumericDigitBaseExponent + firstDigitSize
        val unsigned = unscaled.bigDecimal.movePointLeft(unscaled.precision).movePointRight(scaleFactor).setScale(displayScale)

        sign match {
          case NUMERIC_POS => Xor.right(BigDecimal(unsigned))
          case NUMERIC_NEG => Xor.right(BigDecimal(unsigned.negate()))
          case NUMERIC_NAN => Xor.left(new NumberFormatException("Value is NaN; cannot be represented as BigDecimal"))
          case NUMERIC_NULL => Xor.left(new NumberFormatException("Value is NULL within NUMERIC"))
        }
      } else {
        Xor.right(BigDecimal(0))
      }
    }.flatMap(identity).fold(
      l => throw new ElementDecodingFailure("NUMERIC", l),
      identity
    )

    def nullDecoder: BigDecimal = throw new NullDecodedFailure("NUMERIC")
  }

  implicit val booleanElementDecoder: ElementDecoder[Boolean] = new ElementDecoder[Boolean] {
    def textDecoder(text: String): Boolean         = Xor.catchNonFatal(text.head match {
      case 't' => true
      case 'f' => false
    }).fold(
     {l => throw new ElementDecodingFailure("BOOLEAN", l)},
     {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Boolean = Xor.catchNonFatal(bytes.head match {
      case 0x00 => false
      case 0x01 => true
    }).fold(
      {l => throw new ElementDecodingFailure("BOOLEAN", l)},
      {r => r}
    )
    def nullDecoder: Boolean                       = throw new NullDecodedFailure("BOOLEAN")
  }

  implicit val charElementDecoder: ElementDecoder[Char] = new ElementDecoder[Char] {
    def textDecoder(text: String): Char         = Xor.catchNonFatal(text.head.toChar).fold(
      {l => throw new ElementDecodingFailure("CHAR", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Char = Xor.catchNonFatal(bytes.head.toChar).fold(
      {l => throw new ElementDecodingFailure("CHAR", l)},
      {r => r}
    )
    def nullDecoder: Char                       = throw new NullDecodedFailure("CHAR")
  }

  implicit val jsonElementDecoder: ElementDecoder[Json] = new ElementDecoder[Json] {
    def textDecoder(text: String): Json         = Validated.fromTry(
      JParser.parseFromString(text)
    ).fold(
      {l => throw new ElementDecodingFailure("JSON", l)},
      {r => r }
    )
    def binaryDecoder(bytes: Array[Byte]): Json = Validated.fromTry({
      //this could be either json or jsonb - if it's jsonb, the first byte is a number representing the jsonb version.
      //currently the version is 1, but we can say that if the first byte is unprintable, it must be a jsonb version.
      //otherwise we have gone through 31 versions of jsonb, which hopefully won't happen before this is updated!
      val buffer = ByteBuffer.wrap(bytes)
      if(buffer.remaining() <= 0)
        Failure(new BufferUnderflowException())
      else if(buffer.get(0) < 32) {
        buffer.get()  //advance one byte
        JParser.parseFromByteBuffer(buffer)
      }
      else
        JParser.parseFromByteBuffer(buffer)
    }).fold(
     {l => throw new ElementDecodingFailure("JSON", l)},
     {r => r}
    )
    def nullDecoder: Json                       = throw new NullDecodedFailure("JSON")
  }

  implicit val dateElementDecoders: ElementDecoder[Date] = new ElementDecoder[Date] {
    def textDecoder(text: String): Date         = Xor.catchNonFatal(LocalDate.parse(text)).fold(
      {l => throw new ElementDecodingFailure("DATE", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Date = Xor.catchNonFatal({
      val buf = ByteBuffer.wrap(bytes)
      // Postgres represents this as Julian Day since Postgres Epoch (2000-01-01)
      val julianDay = buf.getInt()
      LocalDate.now().`with`(JulianFields.JULIAN_DAY, julianDay + 2451545)
    }).fold(
      {l => throw new ElementDecodingFailure("DATE", l)},
      {r => r}
    )
    def nullDecoder: Date                       = throw new NullDecodedFailure("DATE")
  }

  implicit val localTimeElementDecoders: ElementDecoder[Time] = new ElementDecoder[Time] {
    def textDecoder(text: String): Time         = Xor.catchNonFatal(LocalTime.parse(text)).fold(
      {l => throw new ElementDecodingFailure("TIME", l)},
      {r => r}
    )
    def binaryDecoder(bytes: Array[Byte]): Time = Xor.catchNonFatal({
      val buf = ByteBuffer.wrap(bytes)
      // Postgres uses either an 8-byte float representing seconds since midnight, or an 8-byte long representing
      // microseconds since midnight.
      // We don't know which one to use inside this decoder, so we'll just assume HAVE_INT64_TIMESTAMP
      // Some design changes would be required to make the decision appropriately.

      val microSecs = buf.getLong()
      LocalTime.ofNanoOfDay(microSecs * 1000)
    }).fold(
      {l => throw new ElementDecodingFailure("TIME", l)},
      {r => r}
    )
    def nullDecoder: Time                       = throw new NullDecodedFailure("TIME")
  }

  implicit val zonedDateTimeElementDecoders: ElementDecoder[TimestampWithTZ] = 
    new ElementDecoder[TimestampWithTZ] {

      private val POSTGRES_TIMESTAMP_EPOCH =
        OffsetDateTime.of(2000, 1, 1, 0, 0, 0, 0, ZoneOffset.UTC).toInstant.toEpochMilli

      //Postgres uses milliseconds since its own custom epoch (see above)
      private def timestampToInstant(microseconds: Long) =
        Instant.ofEpochMilli(microseconds / 1000L).plusMillis(POSTGRES_TIMESTAMP_EPOCH)

      private val zonedDateTimeFmt = new DateTimeFormatterBuilder()
        .appendPattern("yyyy-MM-dd HH:mm:ss")
        .appendFraction(ChronoField.MICRO_OF_SECOND, 0, 6, true)
        .appendOptional(DateTimeFormatter.ofPattern("X"))
        .toFormatter()

      def textDecoder(text: String): TimestampWithTZ = Xor.catchNonFatal({
        ZonedDateTime.parse(text, zonedDateTimeFmt)
      }).fold(
        {l => throw new ElementDecodingFailure("TIMESTAMP WITH TIME ZONE", l)},
        {r => r}
      )

      def binaryDecoder(bytes: Array[Byte]): TimestampWithTZ = Xor.catchNonFatal({
        val buf = ByteBuffer.wrap(bytes)
        timestampToInstant(buf.getLong).atZone(ZoneId.systemDefault())
      }).fold(
        {l => throw new ElementDecodingFailure("TIMESTAMP WITH TIME ZONE", l)},
        {r => r}
      )
      def nullDecoder: TimestampWithTZ = throw new NullDecodedFailure("TIMSTAMP WITH TIME ZONE")
    }

  implicit val hstoreDecoders: ElementDecoder[HStore] = new ElementDecoder[HStore] {
    val hstoreStringRegex = """"([^"]*)"=>(?:NULL|"([^"]*))"""".r
    def textDecoder(text: String): HStore = Xor.catchNonFatal {
      hstoreStringRegex.findAllMatchIn(text).map {
        m => m.group(1) -> Option(m.group(2))
      }.toMap
    }.fold(
      l => throw new ElementDecodingFailure("HSTORE", l),
      identity
    )

    def nullDecoder(): HStore = throw new NullDecodedFailure("HSTORE")

    def binaryDecoder(bytes: Array[Byte]): HStore = {
      val buf = ByteBuffer.wrap(bytes)
      Xor.catchNonFatal {
        val count = buf.getInt()
        val charset = StandardCharsets.UTF_8 //TODO: are we going to support other charsets?
        Array.fill(count) {
          val keyLength = buf.getInt()
          val key = new Array[Byte](keyLength)
          buf.get(key)
          val valueLength = buf.getInt()
          val value = valueLength match {
            case -1 => None
            case l =>
              val valueBytes = new Array[Byte](l)
              buf.get(valueBytes)
              Some(valueBytes)
          }
          new String(key, charset) -> value.map(new String(_, charset))
        }.toMap
      }.fold (
        l => throw new ElementDecodingFailure("HSTORE", l),
        identity
      )
    }
  }
}
