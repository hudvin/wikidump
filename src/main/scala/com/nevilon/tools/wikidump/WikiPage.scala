package com.nevilon.tools.wikidump

/**
 * Created with IntelliJ IDEA.
 * User: hudvin
 * Date: 11/11/12
 * Time: 6:27 PM
 * To change this template use File | Settings | File Templates.
 */
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
