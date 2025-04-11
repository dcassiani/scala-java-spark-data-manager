package dml.util

import java.util.{Calendar, Date, TimeZone}
import java.text.SimpleDateFormat
import java.time.Instant

object DateUtil {
  def validateOrDefaultUpdateDate(str: String): String = {
    var ultimaAtualizacao = ""
    try {
      ultimaAtualizacao = str
    } catch { case _: Exception =>
        val c = Calendar.getInstance(TimeZone.getTimeZone("GMT-3"))
        c.setTimeInMillis(System.currentTimeMillis())
        val dayValue = new java.text.SimpleDateFormat("yyyyMMdd")
        dayValue.setTimeZone(TimeZone.getTimeZone("GMT-3"))
        val updateDate = dayValue.format(c.getTime)
        ultimaAtualizacao = updateDate
    }
    ultimaAtualizacao
  }

    def data_refer: String = {
        val updateDate = current_timestamp()
        val date = parser(timeEpoch = updateDate)
        date.toString
    }

    private def parser(formatPattern: String = "yyyyMMdd", timeEpoch:Long): Long = {
        val format = new SimpleDateFormat(formatPattern)
        val today = new Date(timeEpoch)
        val formattedDate = format.format(today).trim
        formattedDate.toLong
    }

    private def current_timestamp():Long = {
        val timestamp = Instant.now().getEpochSecond*1000
        timestamp
    }
}
