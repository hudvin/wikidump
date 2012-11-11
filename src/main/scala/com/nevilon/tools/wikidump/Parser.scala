package com.nevilon.tools.wikidump

import collection.mutable
import scala.xml.pull._
import Predef._


/**
 * Created with IntelliJ IDEA.
 * User: hudvin
 * Date: 10/30/12
 * Time: 3:12 PM
 * To change this template use File | Settings | File Templates.
 */


trait Parser {

  def parse(xmlReader: XMLEventReader)

}

class CategoryLinksParser(importer:DataImporter[PageLink] ) extends Parser {

 // val importer: DataImporter[PageLink] = new MongoCategoryLinksImporter


  def parse(xmlReader: XMLEventReader) {

    var pageLink: PageLink = null
    var buffer: StringBuilder = new StringBuilder
    val path: mutable.Stack[String] = new mutable.Stack[String]
    var currentFieldName = ""
    while (xmlReader.hasNext) {
      val v = xmlReader.next
      v match {
        case EvElemStart(_, label, attr_, _) => {
          path.push(label)
          path.slice(0, 3).toList match {
            case List("field", "row", "table_data") => {
              buffer = new StringBuilder
              currentFieldName = attr_.get("name").get.toString()
            }
            case List("row", "table_data", "database") => {
              pageLink = new PageLink
            }
            case _ => {}
          }
        }
        case EvElemEnd(_, label) => {

          path.slice(0, 3).toList match {
            case List("field", "row", "table_data") => {
              currentFieldName match {

                case "cl_from" => {
                  pageLink.from = buffer.toString()
                }
                case "cl_to" => {
                  pageLink.to = buffer.toString()
                }
                case "cl_sortkey" => {
                  pageLink.sortKey = buffer.toString()
                }
                case "cl_timestamp" => {
                  pageLink.timestamp = buffer.toString()
                }
                case "cl_sortkey_prefix" => {
                  pageLink.sortKeyPrefix = buffer.toString()
                }
                case "cl_collation" => {
                  pageLink.collation = buffer.toString()
                }
                case "cl_type" => {
                  pageLink.type_ = buffer.toString()
                }

              }
            }

            case List("row", "table_data", "database") => {
              importer.add(pageLink)
            }
            case _ => {}

          }
          path.pop()
        }
        case EvText(text) => {
          buffer.append(text)
        }
        case _ => {}
      }
    }
  }


}

class PageParser(importer: DataImporter[WikiPage]) extends Parser {

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

  val path: mutable.Stack[String] = new mutable.Stack[String]


  def parse(xmlReader: XMLEventReader) {
    var wikiPage: WikiPage = null
    var buffer: StringBuilder = null
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
              importer.add(wikiPage)
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