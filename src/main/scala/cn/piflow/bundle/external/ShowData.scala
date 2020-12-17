package cn.piflow.bundle.external

import cn.piflow.conf.bean.PropertyDescriptor
import cn.piflow.conf.util.{ImageUtil, MapUtil}
import cn.piflow.{JobContext, JobInputStream, JobOutputStream, ProcessContext}
import cn.piflow.conf.{ConfigurableStop, Port, StopGroup}


//your Stop should extends "ConfigurableStop" and implement the methods
class ShowData extends ConfigurableStop{

  // the email of author
  val authorEmail: String = "xjzhu@cnic.cn"
  // the description of Stop
  val description: String = "Show Data"
  //the inport list of Stop
  val inportList: List[String] = List(Port.DefaultPort)
  //the outport list of Stop
  val outportList: List[String] = List(Port.DefaultPort)

  //the customized properties of your Stop
  var showNumber: String = _


  // core logic function of Stop
  // read data by "in.read(inPortName)", the default port is ""
  // write data by "out.write(data, outportName)", the default port is ""
  def perform(in: JobInputStream, out: JobOutputStream, pec: JobContext): Unit = {

    val df = in.read()
    df.show(showNumber.toInt)
    out.write(df)
  }

  def initialize(ctx: ProcessContext): Unit = {

  }

  //set customized properties of your Stop
  def setProperties(map : Map[String, Any]): Unit = {
    showNumber = MapUtil.get(map,"showNumber").asInstanceOf[String]
  }

  //get descriptor of customized properties
  override def getPropertyDescriptor(): List[PropertyDescriptor] = {
    var descriptor : List[PropertyDescriptor] = List()

    val showNumber = new PropertyDescriptor()
      .name("showNumber")
      .displayName("showNumber")
      .description("The count to show.")
      .defaultValue("")
      .required(false)
      .example("10")
    descriptor = showNumber :: descriptor
    descriptor
  }

  // get icon of Stop
  override def getIcon(): Array[Byte] = {
    ImageUtil.getImage("icon/csv/CsvParser.png")
  }

  // get group of Stop
  override def getGroup(): List[String] = {
    List("External")
  }

}

