package com.dapeng.hbase.util

import com.alibaba.fastjson.{JSON, JSONObject}
import com.dapeng.hbase.bean.XF
import com.google.gson.Gson

object JsonToBean {
  def gson(str: String):XF ={
    val jsonOBJ : JSONObject = JSON.parseObject(str)
    val string: String = jsonOBJ.getJSONArray("data").getJSONObject(0).toString
    val gson = new Gson()
    gson.fromJson(string, classOf[XF])
  }
}
