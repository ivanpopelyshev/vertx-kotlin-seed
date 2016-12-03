package com.awesome.db

import io.vertx.ext.asyncsql.AsyncSQLClient

import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.SQLConnection
import io.vertx.ext.sql.UpdateResult
import io.vertx.kotlin.lang.AsyncResult
import io.vertx.kotlin.lang.sendToFuture
import io.vertx.kotlin.lang.toAsyncResultK
import mu.KLogging

fun SQLConnection.query(sql: String, params: List<Any>): Future<ResultSet> {
    val res = Future.future<ResultSet>()
    AsyncDbClient.logger.debug("executing query $sql with params $params");
    queryWithParams(sql, JsonArray(params), { it.toAsyncResultK().sendToFuture(res) })
    return res;
}

fun <T> SQLConnection.query(sql: String, params: List<Any>, handler: (AsyncResult<ResultSet>) -> AsyncResult<T>): Future<T> {
    val res = Future.future<T>()
    AsyncDbClient.logger.debug("executing query $sql with params $params");
    queryWithParams(sql, JsonArray(params), { handler(it.toAsyncResultK()).sendToFuture(res) })
    return res;
}

fun SQLConnection.update(sql: String, params: List<Any>): Future<UpdateResult> {
    val res = Future.future<UpdateResult>()
    AsyncDbClient.logger.debug("executing update $sql with params $params");
    updateWithParams(sql, JsonArray(params), { it.toAsyncResultK().sendToFuture(res) })
    return res;
}

fun <T> SQLConnection.update(sql: String, params: List<Any>, handler: (AsyncResult<UpdateResult>) -> AsyncResult<T>): Future<T> {
    val res = Future.future<T>()
    AsyncDbClient.logger.debug("executing update $sql with params $params");
    updateWithParams(sql, JsonArray(params), { handler(it.toAsyncResultK()).sendToFuture(res) })
    return res;
}

class AsyncDbController<T>(val conn: SQLConnection) {
    val future: Future<T> = Future.future<T>()
    val dbHandler = DbHandler(conn)

    suspend fun <V> await(f: Future<V>): V =
            suspendWithCurrentContinuation<V> {
                f.setHandler({
                    result ->
                    if (result.succeeded()) {
                        it.resume(result.result())
                    } else {
                        it.resumeWithException(result.cause())
                    }
                })
                Suspend
            }

    operator fun handleResult(value: T, c: Continuation<Nothing>) {
        future.complete(value)
    }

    operator fun handleException(t: Throwable, c: Continuation<Nothing>) {
        future.fail(t)
    }
}

object AsyncDbClient : KLogging() {

    var dbClient: AsyncSQLClient? = null;

    fun <T> asyncDb(coroutine c: AsyncDbController<T>.() -> Continuation<Unit>): Future<T> {
        val res = Future.future<T>()
        dbClient!!.getConnection {
            connResult ->
            if (connResult.failed()) {
                res.fail(connResult.cause())
            } else {
                val conn = connResult.result()
                val controller = AsyncDbController<T>(conn)
                controller.c().resume(Unit)
                controller.future.setHandler {
                    res2 ->
                    conn.close()
                    res2.toAsyncResultK().sendToFuture(res);
                }
            }
        }
        return res
    }

    fun <T> withConnection(handler: (SQLConnection) -> Future<T>): Future<T> {
        val res = Future.future<T>();
        dbClient!!.getConnection {
            connResult ->
            if (connResult.failed()) {
                res.fail(connResult.cause())
            } else {
                val conn = connResult.result()
                handler.invoke(conn).setHandler {
                    res2 ->
                    conn.close()
                    res2.toAsyncResultK().sendToFuture(res);
                }
            }
        }
        return res;
    }
}