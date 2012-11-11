package com.nevilon.tools.wikidump

/**
 * Created with IntelliJ IDEA.
 * User: hudvin
 * Date: 11/11/12
 * Time: 6:27 PM
 * To change this template use File | Settings | File Templates.
 */
class PageLink {

  var from = ""
  var to = ""
  var sortKey = ""
  var timestamp = ""
  var sortKeyPrefix = ""
  var collation = ""
  var type_ = ""

  override def toString = "from: " + from + " to: " + to + " sortkey: " + sortKey + " timestamp: " + timestamp +
    " sortkeyprefix: " + sortKeyPrefix + " collation: " + collation + " type: " + type_
}
