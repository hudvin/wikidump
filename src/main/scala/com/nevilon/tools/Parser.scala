package com.nevilon.tools.wikidump

import collection.mutable
import scala.xml.pull._
import scala.io.Source
import Predef._
import java.io._
import org.apache.hadoop.hbase.{HTableDescriptor, HColumnDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.util.Bytes
import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import java.sql.{PreparedStatement, Connection, DriverManager}
import java.util.UUID
import com.mongodb.{BasicDBObject, DBCollection, DB, Mongo}

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

  private val cacheSize = 200

  private var importer: Importer = null
  private val wikiPages = new ListBuffer[WikiPage]()
  var counter = 0


  def main(args: Array[String]) {
    importer  = new MongoImporter(args)

    val xmlReader = new XMLEventReader(Source.fromInputStream(new BufferedInputStream(java.lang.System.in), "utf-8")).buffered
    var wikiPage: WikiPage = null
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
              wikiPages.append(wikiPage)
              if (wikiPages.length > cacheSize) {
                counter+=cacheSize
                println(counter)
                importer.saveData(wikiPages)
                wikiPages.clear()
              }
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

abstract class Importer(args: Array[String]) {

  def saveData(pages: ListBuffer[WikiPage])

}


class MongoImporter(val args: Array[String]) extends Importer(args){


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

  private var db:DB = null
  private var coll:DBCollection = null

  connect()

  private def connect(){
    val mongo = new Mongo( "localhost" , 27017 )
    db =  mongo.getDB( "wiki" )
    coll = db.getCollection("pages")

  }

  override def saveData(pages: ListBuffer[WikiPage]){
    pages.foreach(page => {

      val doc = new BasicDBObject();

      doc.put(TITLE_FAMILY, page.title)
      doc.put(NS_FAMILY, page.ns)
      doc.put(ID_FAMILY, page.id)
      doc.put(REDIRECT_TITLE_FAMILY, page.redirectTitle)
      doc.put(REVISION_ID_FAMILY, page.revision.id)
      doc.put(REVISION_PARENT_ID_FAMILY,  page.revision.parentId)
      doc.put(REVISION_TIMESTAMP_FAMILY, page.revision.timestamp )
      doc.put(REVISION_COMMENT_FAMILY, page.revision.comment)
      doc.put(REVISION_SHA1_FAMILY, page.revision.sha1)
      doc.put(REVISION_TEXT_FAMILY, page.revision.text )
      doc.put(REVISION_MINOR_FAMILY, page.revision.minor)
      doc.put(REVISION_CONTRIBUTOR_ID_FAMILY, page.revision.contributor.id)
      doc.put(REVISION_CONTRIBUTOR_USERNAME_FAMILY,page.revision.contributor.username )
      coll.insert(doc)

    })
  }

}

class MySQlImporter(val args: Array[String]) extends Importer(args) {


  private val INSERT_PAGE_SQL = "INSERT INTO pages (page_id,ns, title,redirect_title, rev_id,rev_parent_id," +
    " rev_timestamp, rev_contributor_username, rev_contributor_id, comment, sha1, page_guid)" +
    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "

  private val INSERT_TEXT_SQL = "INSERT INTO pages_text(page_guid, page_text) VALUES(?,?)"

  private var prep: PreparedStatement = null

  private var insertTextPrep: PreparedStatement = null

  private var conn: Connection = null

  initJDBC(args)

  private def initJDBC(args:Array[String]) {
    val password = args(0)
    val uri = "jdbc:mysql://localhost:3306/wiki_dump?user=root&password=" + password
    classOf[com.mysql.jdbc.Driver]
    conn = DriverManager.getConnection(uri)
    prep = conn.prepareStatement(INSERT_PAGE_SQL)
    insertTextPrep = conn.prepareStatement(INSERT_TEXT_SQL)
    conn.setAutoCommit(false)
  }

  override def saveData(pages: ListBuffer[WikiPage]) {
    conn.setAutoCommit(false)
    try {

      pages.foreach(page => {
        prep.clearParameters()
        insertTextPrep.clearParameters()

        prep.setInt(1, page.id)
        prep.setString(2, page.ns)
        prep.setString(3, page.title)
        prep.setString(4, page.redirectTitle)
        prep.setLong(5, page.revision.id)
        prep.setLong(6, page.revision.parentId)
        prep.setString(7, page.revision.timestamp)
        prep.setString(8, page.revision.contributor.username)
        prep.setLong(9, page.revision.contributor.id)
        prep.setString(10, page.revision.comment)
        prep.setString(11, page.revision.sha1)
        //   prep.setString(12, page.revision.text)
        val pageGuid = UUID.randomUUID().toString
        prep.setString(12, pageGuid)

        insertTextPrep.setString(1, pageGuid)
        insertTextPrep.setString(2, page.revision.text)
        insertTextPrep.addBatch()

        prep.addBatch()

      }
      )
      prep.executeBatch()
      insertTextPrep.executeBatch()

      conn.commit()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }

    } finally {

    }
  }


}

class HBaseImporter(val args: Array[String]) extends Importer(args) {

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

  private var hbase: HBaseAdmin = null
  private var conf: Configuration = null
  private var table: HTable = null

  private val HBASE_MASTER_KEY = "hbase.master"
  private val HBASE_MASTER_VALUE = "localhost:60000"

  private val DEFAULT_TABLE_NAME = "wiki"

  private val recordsCache: java.util.List[Put] = new java.util.LinkedList[Put]

  connect()

  private def connect() {
    conf = HBaseConfiguration.create()
    conf.set(HBASE_MASTER_KEY, HBASE_MASTER_VALUE)
    hbase = new HBaseAdmin(conf)
    if (!hbase.tableExists(DEFAULT_TABLE_NAME)) {
      val tableDesc: HTableDescriptor = new HTableDescriptor(DEFAULT_TABLE_NAME)
      tableDesc.addFamily(new HColumnDescriptor(TITLE_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(NS_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(ID_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REDIRECT_TITLE_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_ID_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_PARENT_ID_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_TIMESTAMP_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_COMMENT_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_SHA1_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_TEXT_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_MINOR_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_CONTRIBUTOR_ID_FAMILY))
      tableDesc.addFamily(new HColumnDescriptor(REVISION_CONTRIBUTOR_USERNAME_FAMILY))
      hbase.createTable(tableDesc)
    }
    table = new HTable(conf, DEFAULT_TABLE_NAME)
  }

  override def saveData(pages: ListBuffer[WikiPage]) {
    pages.foreach(page => {
      val p = new Put(Bytes.toBytes(page.title))
      p.add(Bytes.toBytes(REVISION_CONTRIBUTOR_USERNAME_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.contributor.username))
      p.add(Bytes.toBytes(TITLE_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.title))
      p.add(Bytes.toBytes(ID_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.id))
      p.add(Bytes.toBytes(NS_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.ns))
      p.add(Bytes.toBytes(REDIRECT_TITLE_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.redirectTitle))
      p.add(Bytes.toBytes(REVISION_ID_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.id))
      p.add(Bytes.toBytes(REVISION_PARENT_ID_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.parentId))
      p.add(Bytes.toBytes(REVISION_TIMESTAMP_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.timestamp))
      p.add(Bytes.toBytes(REVISION_COMMENT_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.comment))
      p.add(Bytes.toBytes(REVISION_SHA1_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.sha1))
      p.add(Bytes.toBytes(REVISION_TEXT_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.text))
      p.add(Bytes.toBytes(REVISION_MINOR_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.minor))
      p.add(Bytes.toBytes(REVISION_CONTRIBUTOR_ID_FAMILY), Bytes.toBytes(""), Bytes.toBytes(page.revision.contributor.id))

      recordsCache += p
    })
    table.setAutoFlush(false)
    table.put(recordsCache)
    table.flushCommits()
    recordsCache.clear()
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

  class Contributor {

    var username: String = ""
    var id: Int = -1

    override def toString = "username: " + username + ",id: " + id
  }

}
