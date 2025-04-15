package dmt.util

import java.util.{Calendar, TimeZone}

object DateUtil {
  def validateUpdateDate(str: String): String = {
    var updateDate = ""
    try {
      updateDate = str
    } catch { case _: Exception =>
        val c = Calendar.getInstance(TimeZone.getTimeZone("GMT-3"))
        c.setTimeInMillis(System.currentTimeMillis())
        val dayValue = new java.text.SimpleDateFormat("yyyyMMdd")
        dayValue.setTimeZone(TimeZone.getTimeZone("GMT-3"))
        val updateDateFormatted = dayValue.format(c.getTime)
        updateDate = updateDateFormatted
    }
    updateDate
  }
}
