/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.storage.kv

import java.util

import org.apache.samza.task.MessageCollector
import org.apache.samza.util.Logging
import org.apache.samza.system.OutgoingMessageEnvelope
import org.apache.samza.system.SystemStream

class AccessLoggedStore[K, V](
                               val store: KeyValueStore[K, V],
                               val collector: MessageCollector,
                               val profilingSystemStream: SystemStream)
  extends KeyValueStore[K, V] with Logging {

  object DBOperations extends Enumeration {
    type DBOperations = Value
    val READ = Value("read")
    val WRITE = Value("write")
    val DELETE = Value("delete") ;
  }

  def get(key: K): V = {
    val toPrint = "read, "  + key ;
    measureLatencyAndWriteToStream(DBOperations.READ, toPrint, store.get(key))
  }

  def getAll(keys: util.List[K]): util.Map[K, V] = {
    store.getAll(keys)
  }

  def put(key: K, value: V): Unit = {
    val toPrint = "write,"  + key + ", "  + value ;
    measureLatencyAndWriteToStream(DBOperations.WRITE, toPrint, store.put(key, value))
  }

  def putAll(entries: util.List[Entry[K, V]]): Unit = {
    val iter = entries.iterator
    store.putAll(entries)
  }


  def delete(key: K): Unit = {
    val toPrint = "delete, " + key ;
    measureLatencyAndWriteToStream(DBOperations.DELETE, toPrint, store.delete(key))

  }

  def deleteAll(keys: util.List[K]): Unit = {
    store.deleteAll(keys)
  }

  def range(from: K, to: K): KeyValueIterator[K, V] = {
    store.range(from, to)
  }

  def all(): KeyValueIterator[K, V] = {
    store.all()
  }

  def close(): Unit = {
    trace("Closing.")

    store.close
  }

  def flush(): Unit = {
    trace("Flushing store.")

    store.flush
    trace("Flushed store.")
  }

  def measureLatencyAndWriteToStream[R](operation: DBOperations.Value, message: String, block: => R):R = {
    val time1 = System.nanoTime() ;
    val result = block
    val time2 = System.nanoTime()
    val latency = time2 - time1 ;
    var msg = message ;

    if(operation == DBOperations.READ) {
      msg += ", " + result
    } else if (operation == DBOperations.DELETE) {
      msg += ", "
    }

    msg += ", " + latency + ", " + System.nanoTime();
    collector.send(new OutgoingMessageEnvelope(profilingSystemStream, msg))
    result
  }
}
