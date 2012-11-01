package com.nevilon.tools.wikidump
import collection.mutable
import scala.xml.pull._
import scala.io.Source
import Predef._
import java.io._
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer

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


  private val TITLE_FAMILY = "title"
  private val NS_FAMILY = "ns"
  private val ID_FAMILY = "id"
  private val REDIRECT_TITLE_FAMILY = "redirect_title"
  private val REVISION_ID_FAMILY = "revision_id"
  private val REVISION_PARENT_ID_FAMILY = "revision_parent_id"
  private val REVISION_TIMESTAMP_FAMILY = "revision_timestamp"
  private val REVISION_COMMENT_FAMILY = "revision_comment"
  private val REVISION_SHA1_FAMILY = "revision_sha1"
  private val REVISION_TEXT_FAMILY = "revison_text"
  private val REVISION_MINOR_FAMILY = "revision_minor"
  private val REVISION_CONTRIBUTOR_ID_FAMILY = "revision_contributor_id"
  private val REVISION_CONTRIBUTOR_USERNAME_FAMILY = "revision_contributor_username"

  private var hbase:HBaseAdmin = null
  private var conf:Configuration = null
  private var table:HTable = null

  private val HBASE_MASTER_KEY  = "hbase.master"
  private val HBASE_MASTER_VALUE = "localhost:60000"


  private val DEFAULT_TABLE_NAME = "wiki_test"

  private val recordsCache:java.util.List[Put] = new java.util.LinkedList[Put]
  private val cacheSize = 500


  private def createHbaseConnect(){
    conf = HBaseConfiguration.create()
    conf.set(HBASE_MASTER_KEY,HBASE_MASTER_VALUE)
    hbase = new HBaseAdmin(conf)
 }

  def createTable(tableName:String){
    val tableDesc:HTableDescriptor = new HTableDescriptor(tableName)

    tableDesc.addFamily( new HColumnDescriptor(TITLE_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(NS_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(ID_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REDIRECT_TITLE_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_ID_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_PARENT_ID_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_TIMESTAMP_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_COMMENT_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_SHA1_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_TEXT_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_MINOR_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_CONTRIBUTOR_ID_FAMILY))
    tableDesc.addFamily( new HColumnDescriptor(REVISION_CONTRIBUTOR_USERNAME_FAMILY))

    hbase.createTable(tableDesc)
  }

  def openTable(tableName:String){
    table = new HTable(conf, tableName)
  }

  def tableExists(tableName:String):Boolean={
        hbase.tableExists(tableName)
  }

  def insert(wikiPage:WikiPage){
    val p = new Put(Bytes.toBytes(wikiPage.title))
    p.add(Bytes.toBytes(REVISION_CONTRIBUTOR_USERNAME_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.contributor.username))
    p.add(Bytes.toBytes(TITLE_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.title))
    p.add(Bytes.toBytes(ID_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.id))
    p.add(Bytes.toBytes(NS_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.ns))
    p.add(Bytes.toBytes(REDIRECT_TITLE_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.redirectTitle))
    p.add(Bytes.toBytes(REVISION_ID_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.id))
    p.add(Bytes.toBytes(REVISION_PARENT_ID_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.parentId))
    p.add(Bytes.toBytes(REVISION_TIMESTAMP_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.timestamp))
    p.add(Bytes.toBytes(REVISION_COMMENT_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.comment))
    p.add(Bytes.toBytes(REVISION_SHA1_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.sha1))
    p.add(Bytes.toBytes(REVISION_TEXT_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.text))
    p.add(Bytes.toBytes(REVISION_MINOR_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.minor))
    p.add(Bytes.toBytes(REVISION_CONTRIBUTOR_ID_FAMILY), Bytes.toBytes(""),Bytes.toBytes(wikiPage.revision.contributor.id))

    recordsCache+=p
    if (recordsCache.size>cacheSize){
      table.setAutoFlush(false    )
      table.put(recordsCache)
      table.flushCommits()
      recordsCache.clear()
    }
  }


  def main(args: Array[String]) {
    createHbaseConnect()
    val tableName = DEFAULT_TABLE_NAME
    if (!tableExists(tableName)){
      createTable(tableName)
    }
    openTable(tableName)

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

