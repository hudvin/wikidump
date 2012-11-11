package com.nevilon.tools.wikidump

import collection.mutable.ListBuffer
import com.mongodb.{BasicDBObject, Mongo, DBCollection, DB}
import java.sql.{DriverManager, Connection, PreparedStatement}
import java.util.UUID
import org.apache.hadoop.hbase.client.{Put, HTable, HBaseAdmin}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HColumnDescriptor, HTableDescriptor, HBaseConfiguration}
import org.apache.hadoop.hbase.util.Bytes


trait DataImporter[Record] {

  protected var buffer = new ListBuffer[Record]

  private var maxBufferSize = 0

  def setBufferSize(size: Int)

  def saveData()

  def add(record: Record) {
    buffer.append(record)
    if (buffer.length > maxBufferSize) {
      saveData()
    }
  }

}


class MongoCategoryLinksImporter extends DataImporter[PageLink] {


  private var db: DB = null
  private var coll: DBCollection = null

  connect()

  private def connect() {
    val mongo = new Mongo("localhost", 27017)
    db = mongo.getDB("wiki")
    coll = db.getCollection("categorylinks")

  }


  def saveData() {
    buffer.foreach(pagelink => {
      val doc = new BasicDBObject();
      doc.put("cl_from", pagelink.from)
      doc.put("cl_to", pagelink.to)
      doc.put("cl_sortkey", pagelink.sortKey)
      doc.put("cl_timestamp", pagelink.timestamp)
      doc.put("cl_sortkey_prefix", pagelink.sortKeyPrefix)
      doc.put("cl_collation", pagelink.collation)
      doc.put("cl_type", pagelink.type_)
      coll.insert(doc)
    })
    buffer.clear()
  }

  def setBufferSize(size: Int) {}
}

class MongoPageImporter() extends DataImporter[WikiPage] {

  def setBufferSize(size: Int) {}


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

  private var db: DB = null
  private var coll: DBCollection = null

  connect()

  private def connect() {
    val mongo = new Mongo("localhost", 27017)
    db = mongo.getDB("wiki")
    coll = db.getCollection("pages")

  }

  override def saveData() {
    buffer.foreach(page => {

      val doc = new BasicDBObject();

      doc.put(TITLE_FAMILY, page.title)
      doc.put(NS_FAMILY, page.ns)
      doc.put(ID_FAMILY, page.id)
      doc.put(REDIRECT_TITLE_FAMILY, page.redirectTitle)
      doc.put(REVISION_ID_FAMILY, page.revision.id)
      doc.put(REVISION_PARENT_ID_FAMILY, page.revision.parentId)
      doc.put(REVISION_TIMESTAMP_FAMILY, page.revision.timestamp)
      doc.put(REVISION_COMMENT_FAMILY, page.revision.comment)
      doc.put(REVISION_SHA1_FAMILY, page.revision.sha1)
      doc.put(REVISION_TEXT_FAMILY, page.revision.text)
      doc.put(REVISION_MINOR_FAMILY, page.revision.minor)
      doc.put(REVISION_CONTRIBUTOR_ID_FAMILY, page.revision.contributor.id)
      doc.put(REVISION_CONTRIBUTOR_USERNAME_FAMILY, page.revision.contributor.username)
      coll.insert(doc)

    })
    buffer.clear()
  }

}


class MySQlPageImporter(val args: Array[String]) extends DataImporter[WikiPage] {

  def setBufferSize(size: Int) {}


  private val INSERT_PAGE_SQL = "INSERT INTO pages (page_id,ns, title,redirect_title, rev_id,rev_parent_id," +
    " rev_timestamp, rev_contributor_username, rev_contributor_id, comment, sha1, page_guid)" +
    " VALUES (?,?,?,?,?,?,?,?,?,?,?,?) "

  private val INSERT_TEXT_SQL = "INSERT INTO pages_text(page_guid, page_text) VALUES(?,?)"

  private var prep: PreparedStatement = null

  private var insertTextPrep: PreparedStatement = null

  private var conn: Connection = null

  initJDBC(args)

  private def initJDBC(args: Array[String]) {
    val password = args(0)
    val uri = "jdbc:mysql://localhost:3306/wiki_dump?user=root&password=" + password
    classOf[com.mysql.jdbc.Driver]
    conn = DriverManager.getConnection(uri)
    prep = conn.prepareStatement(INSERT_PAGE_SQL)
    insertTextPrep = conn.prepareStatement(INSERT_TEXT_SQL)
    conn.setAutoCommit(false)
  }

  override def saveData() {
    conn.setAutoCommit(false)
    try {

      buffer.foreach(page => {
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
      buffer.clear()
    } catch {
      case e: Exception => {
        e.printStackTrace()
      }

    } finally {

    }
  }


}


class HBasePageImporter extends DataImporter[WikiPage] {

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

  def setBufferSize(size: Int) {}


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

  override def saveData() {
    buffer.foreach(page => {
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

      recordsCache.add(p)
    })
    table.setAutoFlush(false)
    table.put(recordsCache)
    table.flushCommits()
    recordsCache.clear()
    buffer.clear()
  }


}

