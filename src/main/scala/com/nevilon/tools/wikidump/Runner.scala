package com.nevilon.tools.wikidump

import Predef._
import io.Source
import java.io.BufferedInputStream
import xml.pull.XMLEventReader


/**
 * Created with IntelliJ IDEA.
 * User: hudvin
 * Date: 10/30/12
 * Time: 3:12 PM
 * To change this template use File | Settings | File Templates.
 */

object Runner {

  val usage = """
    Usage: wikidump [--table] [--to]
              """

  def main(args: Array[String]) {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def nextOption(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map


        case "--table" :: value :: tail =>
          nextOption(map ++ Map('table -> value), tail)
        case "--to" :: value :: tail =>
          nextOption(map ++ Map('to -> value), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          nextOption(map ++ Map('infile -> string), list.tail)
        case string :: Nil => nextOption(map ++ Map('infile -> string), list.tail)
        case option :: tail => println("Unknown option " + option)
        sys.exit(1)
      }
    }
    val options = nextOption(Map(), arglist)
    val dataType:String = options.get('table).get.toString
    val toType:String = options.get('to).get.toString

    var parser:Parser = null

    if (dataType.equals("categorylinks")){
      var importer:DataImporter[PageLink] = null
      if (toType.equals("mongo")){
        importer = new MongoCategoryLinksImporter
      }else if (toType.equals("mysql")){
      }
      parser = new CategoryLinksParser(importer)

    }else if (dataType.equals("pages")){
      var importer:DataImporter[WikiPage] = null
      if (toType.equals("mongo")){
        importer = new MongoPageImporter
      }else if (toType.equals("mysql")){
        importer = new MySQlPageImporter(args) //fix!
      }else if (toType.equals("hbase")){
        importer = new HBasePageImporter()
      }
      parser = new PageParser(importer)
    }

    val xmlReader =  new XMLEventReader(Source.fromInputStream(new BufferedInputStream(java.lang.System.in), "utf-8"))
    parser.parse(xmlReader)


    options.foreach {
      case (key, value) => {
        println(">>> key=" + key + ", value=" + value)



      }
    }


    println(options)

  }


}


