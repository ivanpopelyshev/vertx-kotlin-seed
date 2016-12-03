package com.awesome

import com.awesome.db.AsyncDbClient
import com.awesome.db.AsyncDbClient.asyncDb
import com.awesome.db.User
import io.vertx.core.AbstractVerticle
import io.vertx.core.Future
import io.vertx.core.Launcher
import io.vertx.ext.asyncsql.MySQLClient
import io.vertx.kotlin.lang.sendToFutureVoid
import io.vertx.kotlin.lang.toAsyncResultK
import mu.KLogging

class RootVerticle : AbstractVerticle() {
    var users = listOf<User>()

    companion object : KLogging()

    override fun start(fut: Future<Void>) {
        AsyncDbClient.dbClient = MySQLClient.createShared(vertx, config().getJsonObject("db"))

        // we need all users to start this verticle

        // like for any action from user, we take connection,
        // and then do all the things inside the block

        asyncDb<Unit>() {
            users = await(dbHandler.get_all_users())
            logger.info("Got users: $users")
            logger.info("Now main verticle can be started")
        }.setHandler {
            it.toAsyncResultK().sendToFutureVoid(fut)
        }

        // asyncLock(vertx.sharedData(), "all_users_lock") - that's how lock on something is done
    }
}

fun main(args: Array<String>) {
    val argList = mutableListOf("run", "com.awesome.RootVerticle")
    argList.addAll(args)

    Launcher().dispatch(argList.toTypedArray())
}
