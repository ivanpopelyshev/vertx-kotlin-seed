package com.awesome.db

import io.vertx.core.Handler
import io.vertx.core.Future
import io.vertx.core.json.JsonArray
import io.vertx.core.json.JsonObject
import io.vertx.ext.sql.ResultSet
import io.vertx.ext.sql.UpdateResult
import io.vertx.kotlin.lang.*
import io.vertx.core.AsyncResult as JAsyncResult
import java.util.*

open class FieldParser<T>(val reader: (JsonArray) -> T) {
    fun parse(rs: ResultSet): T? {
        if (rs.numRows > 1) {
            throw Throwable("More than one row inside singleRowParser")
        }
        if (rs.numRows == 0) {
            return null
        } else {
            return reader(rs.results[0])
        }
    }

    fun parseAsync(ar: AsyncResult<ResultSet>): AsyncResult<T?> {
        if (ar is AsyncErrorResult) {
            return AsyncErrorResult<T?>(ar.error)
        }
        val rs = (ar as AsyncSuccessResult).result
        if (rs.numRows > 1) {
            return AsyncErrorResult(Throwable("More than one row inside singleRowParser"))
        }
        if (rs.numRows == 0) {
            return AsyncSuccessResult<T?>(null)
        } else {
            return AsyncSuccessResult(reader(rs.results[0]))
        }
    }

    fun sendToFuture(fut: Future<T?>): Handler<JAsyncResult<ResultSet>> {
        return Handler<JAsyncResult<ResultSet>> {
            ares: JAsyncResult<ResultSet> ->
            ares.toAsyncResultK().mapIfSuccess(this::parse).sendToFuture(fut)
        }
    }

    fun compose(handler: (AsyncResult<T?>) -> Unit) = Handler<JAsyncResult<ResultSet>> {
        handler.invoke(it.toAsyncResultK().mapIfSuccess(this::parse))
    }
}

open class UpdateParser<T>(val reader: (JsonArray) -> T?) {
    fun parse(rs: UpdateResult): T? {
        if (rs.updated > 1) {
            throw Throwable("More than one row inside singleRowParser")
        }
        if (rs.updated == 0) {
            return null;
        } else {
            return reader(rs.keys)
        }
    }

    fun parseAsync(ar: AsyncResult<UpdateResult>): AsyncResult<T?> {
        if (ar is AsyncErrorResult) {
            return AsyncErrorResult<T?>(ar.error)
        }
        val rs = (ar as AsyncSuccessResult).result
        if (rs.updated > 1) {
            return AsyncErrorResult(Throwable("More than one row inside singleRowParser"))
        }
        if (rs.updated == 0) {
            return AsyncSuccessResult<T?>(null)
        } else {
            return AsyncSuccessResult(reader(rs.keys))
        }
    }

    fun sendToFuture(fut: Future<T?>): Handler<JAsyncResult<UpdateResult>> {
        return Handler<JAsyncResult<UpdateResult>> {
            ares: JAsyncResult<UpdateResult> ->
            ares.toAsyncResultK().mapIfSuccess(this::parse).sendToFuture(fut)
        }
    }

    fun compose(handler: (AsyncResult<T?>) -> Unit): Handler<JAsyncResult<UpdateResult>> {
        return Handler<JAsyncResult<UpdateResult>> {
            ares: JAsyncResult<UpdateResult> ->
            handler.invoke(ares.toAsyncResultK().mapIfSuccess(this::parse))
        }
    }
}


open class RowParser<T>(val reader: (JsonObject) -> T) {

    fun single(rs: ResultSet): T? {
        if (rs.numRows > 1) {
            throw Throwable("More than one row inside singleRowParser")
        }
        if (rs.numRows == 0) {
            return null
        } else {
            setResult(rs.columnNames, rs.results[0])
            return reader(row)
        }
    }

    fun singleAsync(ar: AsyncResult<ResultSet>): AsyncResult<T?> {
        if (ar is AsyncErrorResult) {
            return AsyncErrorResult<T?>(ar.error)
        }
        val rs = (ar as AsyncSuccessResult).result
        if (rs.numRows > 1) {
            return AsyncErrorResult(Throwable("More than one row inside singleRowParser"))
        }
        if (rs.numRows == 0) {
            return AsyncSuccessResult<T?>(null)
        } else {
            setResult(rs.columnNames, rs.results[0])
            return AsyncSuccessResult(reader(row))
        }
    }

    fun multi(rs: ResultSet): List<T> {
        val res = rs.results
        val list = ArrayList<T>()
        val sz = rs.numRows
        for (i in 0..sz - 1) {
            setResult(rs.columnNames, res[i])
            list.add(reader(row))
        }
        return list
    }

    fun multiAsync(ar: AsyncResult<ResultSet>): AsyncResult<List<T>> {
        if (ar is AsyncErrorResult) {
            return AsyncErrorResult<List<T>>(ar.error)
        }
        val rs = (ar as AsyncSuccessResult).result
        val res = rs.results
        val list = ArrayList<T>()
        val sz = rs.numRows
        for (i in 0..sz - 1) {
            setResult(rs.columnNames, res[i])
            list.add(reader(row))
        }
        return AsyncSuccessResult(list)
    }

    fun singleToFuture(fut: Future<T?>): Handler<JAsyncResult<ResultSet>> {
        return Handler<JAsyncResult<ResultSet>> {
            ares: JAsyncResult<ResultSet> ->
            ares.toAsyncResultK().mapIfSuccess(this::single).sendToFuture(fut)
        }
    }

    fun singleCompose(handler: (AsyncResult<T?>) -> Unit): Handler<JAsyncResult<ResultSet>> {
        return Handler<JAsyncResult<ResultSet>> {
            ares: JAsyncResult<ResultSet> ->
            handler.invoke(ares.toAsyncResultK().mapIfSuccess(this::single))
        }
    }

    var row = JsonObject();

    fun setResult(columnNames: List<String>, result: JsonArray) {
        val sz = columnNames.size
        for (i in 0..sz - 1) {
            row.put(columnNames.get(i), result.getValue(i))
        }
    }
}
