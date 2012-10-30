package com.nevilon.tools.wikidump
import collection.mutable
import collection.mutable.ListBuffer
import scala.xml.pull._
import scala.io.Source
import Predef._
import java.io._
import org.apache.hadoop.hbase.{KeyValue, HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes

/**
 * Created with IntelliJ IDEA.
 * User: hudvin
 * Date: 10/30/12
 * Time: 3:12 PM
 * To change this template use File | Settings | File Templates.
 */

object Parser {

  private val PAGE_PATH = List("page", "mediawiki")
  private val REVISION_PATH = List("revision", "page", "mediawiki")
  private val CONTRIBUTOR_PATH = List("contributor", "revision", "page", "mediawiki")


  private val PAGE_TAG: String = "page"
  private val REVISION_TAG = "revision_tag"
  private val PARENTID_TAG = "parentid"
  private val TIMESTAMP_TAG = "timestamp"
  private val TITLE_TAG = "title"
  private val NS_TAG = "ns"
  private val ID_TAG = "id"
  private val CONTRIBUTOR_TAG = "contributor"
  private val MINOR_TAG = "minor"
  private val COMMENT_TAG = "comment"
  private val SHA1_TAG = "sha1"
  private val TEXT_TAG = "text"
  private val REDIRECT_TAG: String = "redirect"
  private val TITLE_ATTR: String = "title"
  private val USERNAME_TAG = "username"

  private var hbase:HBaseAdmin = null
  private var conf:Configuration = null
  private var table:HTable = null

  private def createHbaseConnect(){
    conf = HBaseConfiguration.create()
    conf.set("hbase.master","localhost:60000")
    hbase = new HBaseAdmin(conf)
 }

  def createTable(tableName:String){
    val tableDesc:HTableDescriptor = new HTableDescriptor(tableName)

    tableDesc.addFamily( new HColumnDescriptor("revison_contribitorusername"))
    tableDesc.addFamily( new HColumnDescriptor("title"))
    tableDesc.addFamily( new HColumnDescriptor("ns"))
    tableDesc.addFamily( new HColumnDescriptor("id"))
    tableDesc.addFamily( new HColumnDescriptor("redirect_title"))
    tableDesc.addFamily( new HColumnDescriptor("revision_id"))
    tableDesc.addFamily( new HColumnDescriptor("revision_parent_id"))
    tableDesc.addFamily( new HColumnDescriptor("revision_timestamp"))
    tableDesc.addFamily( new HColumnDescriptor("revision_comment"))
    tableDesc.addFamily( new HColumnDescriptor("revision_sha1"))
    tableDesc.addFamily( new HColumnDescriptor("revision_text"))
    tableDesc.addFamily( new HColumnDescriptor("revision_minor"))
    tableDesc.addFamily( new HColumnDescriptor("revision_contributor_id"))
    tableDesc.addFamily( new HColumnDescriptor("revision_contributor_username"))

    hbase.createTable(tableDesc)
  }

  def openTable(tableName:String){
    table = new HTable(conf, tableName)
  }

  def tableExists(tableName:String):Boolean={
        hbase.tableExists(tableName)
  }

  def insert(wikiPage:WikiPage){
    val p = new Put(Bytes.toBytes(wikiPage.title))//name of record!
    p.add(Bytes.toBytes("revision_contributor_username"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.contributor.username))
    p.add(Bytes.toBytes("title"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.title))
    p.add(Bytes.toBytes("id"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.id))
    p.add(Bytes.toBytes("ns"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.ns))
    p.add(Bytes.toBytes("redirect_title"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.redirectTitle))
    p.add(Bytes.toBytes("revision_id"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.id))
    p.add(Bytes.toBytes("revision_parent_id"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.parentId))
    p.add(Bytes.toBytes("revision_timestamp"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.timestamp))
    p.add(Bytes.toBytes("revision_comment"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.comment))
    p.add(Bytes.toBytes("revision_sha1"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.sha1))
    p.add(Bytes.toBytes("revision_text"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.text))
    p.add(Bytes.toBytes("revision_minor"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.minor))
    p.add(Bytes.toBytes("revision_contributor_id"), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.contributor.id))

    table.put(p)
  }


  def main(args: Array[String]) {
    createHbaseConnect()
    val tableName = "wiki"
    if (!tableExists(tableName)){
      createTable(tableName)
    }
    openTable(tableName)


//    var p = new WikiPage
//    p.title = "title"
//    p.id = 0
//    insert(p)

    var counter:Int = 0


    val xmlReader =
      new XMLEventReader(Source.fromInputStream(
        new BufferedInputStream(java.lang.System.in), "utf-8")).buffered


    var wikiPage: WikiPage = null
    val wikiPages = new ListBuffer[WikiPage]()
    var buffer: StringBuilder = null
    val path: mutable.Stack[String] = new mutable.Stack[String]
    while (xmlReader.hasNext) {
      val v = xmlReader.next

      v match {

        case EvElemStart(_, PAGE_TAG, _, _) => {
          path.push(PAGE_TAG)
          wikiPage = new WikiPage
        }

        case EvElemStart(_, label, attr_, _) => {
          path.push(label)
          buffer = new StringBuilder

          label match {

            case REDIRECT_TAG => {
              val redirect = attr_.get(TITLE_ATTR).get.toString()
              if (attr_.get(TITLE_ATTR) != None) {
                wikiPage.redirectTitle = redirect
              }
            }

            case _ => {}

          }
        }

        case EvElemEnd(_, label) => {
          path.pop()
          label match {
            case PAGE_TAG => {
              counter+=1
              println(counter)
              insert(wikiPage)
              //wikiPages.append(wikiPage)
              //println(wikiPage)
            }

            case TITLE_TAG => {
              wikiPage.title = buffer.toString()
            }

            case NS_TAG => {
              wikiPage.ns = buffer.toString()
            }
            case ID_TAG => {
              path.toList match {
                case PAGE_PATH => {
                  wikiPage.id = getIntValue(buffer.toString())
                }
                case REVISION_PATH => {
                  wikiPage.revision.id = getIntValue(buffer.toString())
                }
                case CONTRIBUTOR_PATH => {
                  wikiPage.revision.contributor.id = getIntValue(buffer.toString())
                }
              }
            }
            case REDIRECT_TAG => {
              wikiPage.redirectTitle = buffer.toString()
            }

            case REVISION_TAG => {
            }
            case PARENTID_TAG =>
              wikiPage.revision.parentId = getIntValue(buffer.toString)

            case TIMESTAMP_TAG => {
              wikiPage.revision.timestamp = buffer.toString()
            }


            case USERNAME_TAG => {
              wikiPage.revision.contributor.username = buffer.toString()
            }

            case MINOR_TAG => {
              wikiPage.revision.minor = buffer.toString()
            }
            case COMMENT_TAG => {
              wikiPage.revision.comment = buffer.toString()
            }
            case SHA1_TAG => {
              wikiPage.revision.sha1 = buffer.toString()
            }
            case TEXT_TAG => {
              wikiPage.revision.text = buffer.toString()
            }

            case _ => {}
          }
        }


        case EvText(text) => {
          buffer.append(text)
        }
        case _ => {}
      }
    }
  }

  private def getIntValue(strValue: String): Int = strValue match {
    case "" => -1
    case _ => strValue.toInt
  }

}

class WikiPage {

  var title: String = ""
  var ns: String = ""
  var id: Int = -1
  var redirectTitle: String = ""

  val revision = new Revision()

  override def toString = "title: " + title + ", ns: " + ns + ", redirectTitle: " + redirectTitle + ", revision:" + revision.toString
}


class Revision {

  var id: Int = -1
  var parentId: Int = -1
  var timestamp: String = ""
  var comment: String = ""
  var sha1: String = ""
  var text: String = ""
  var minor: String = ""

  val contributor = new Contributor()

  override def toString = "id: " + id + ",parentId: " + parentId + ",timestamp: " +
    " " + timestamp + ", contributor: " + contributor.toString + ",comment: " +
    comment + ", sha1: " + sha1 + ", text: " + text + ",minor: " + minor
}

class Contributor {

  var username: String = ""
  var id: Int = -1

  override def toString = "username: " + username + ",id: " + id
}

