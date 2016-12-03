package com.awesome

import io.vertx.core.Future
import io.vertx.core.shareddata.SharedData
import io.vertx.kotlin.lang.sendToFuture
import io.vertx.kotlin.lang.toAsyncResultK
import mu.KLogging

object AsyncVertx {
    fun <T> asyncVx(coroutine c: VertxFutureController<T>.() -> Continuation<Unit>): Future<T> {
        val controller = VertxFutureController<T>()
        controller.c().resume(Unit)
        return controller.future
    }

    fun <T> asyncF(c: (fut: Future<T>) -> Unit): Future<T> {
        val fut = Future.future<T>()
        c(fut)
        return fut
    }

    fun <T> asyncLock(sharedData: SharedData, lockKey: String, coroutine c: VertxFutureController<T>.() -> Continuation<Unit>): Future<T> {
        val res = Future.future<T>()
        sharedData.getLock(lockKey) {
            lockResult ->
            if (lockResult.failed()) {
                res.fail(lockResult.cause())
            } else {
                val lock = lockResult.result()
                val controller = VertxFutureController<T>()
                controller.c().resume(Unit)
                controller.future.setHandler {
                    res2 ->
                    lock.release()
                    res2.toAsyncResultK().sendToFuture(res);
                }
            }
        }
        return res
    }
}

class VertxFutureController<T>() {
    companion object: KLogging()

    val future: Future<T> = Future.future<T>()

    suspend fun <V> await(f: Future<V>): V =
            suspendWithCurrentContinuation {
                f.setHandler({
                    result ->
                    if (result.succeeded()) {
                        it.resume(result.result())
                    } else {
                        logger.error("error returned in await in asyncVc block", result.cause())
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

