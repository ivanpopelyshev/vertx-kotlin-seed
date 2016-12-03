package com.awesome.db

import io.vertx.core.Future
import io.vertx.ext.sql.SQLConnection
import com.awesome.AsyncVertx.asyncVx
import io.vertx.kotlin.lang.mapIfSuccess
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.time.format.DateTimeFormatterBuilder

open class DbHandler(val conn: SQLConnection) {
    object LocalDateTimeEncoderDecoder {

        private val ZeroedTimestamp = "0000-00-00 00:00:00"

        private val optional = DateTimeFormatterBuilder()
                .appendPattern(".SSSSSS").toFormatter()

        private val format = DateTimeFormatterBuilder()
                .append(DateTimeFormatter.ISO_LOCAL_DATE_TIME)
                .appendOptional(optional)
                .toFormatter()

        fun encode(value: Any): String =
                format.format(value as LocalDateTime)

        fun decode(value: String): LocalDateTime? =
                if (ZeroedTimestamp == value) {
                    null
                } else {
                    LocalDateTime.from(format.parse(value))
                }
    }


    companion object {
        val longParser = FieldParser<Long> { arr -> arr.getLong(0) }

        val userParser = RowParser<User> {
            json ->
            User(id = json.getLong("id"),
                    name = json.getString("name"),
                    score = json.getInteger("score"))
        }
    }

    fun get_user_by_name(user_name: String) =
            conn.query("""SELECT * FROM `users` WHERE name = ?""",
                    listOf(user_name),
                    userParser::singleAsync)

    fun get_user_by_id(user_id: Long) =
            conn.query("""SELECT * FROM `users` WHERE id = ?""",
                    listOf(user_id),
                    userParser::singleAsync)

    fun add_user(user: User) =
            conn.update("""INSERT INTO `users` (name, score) VALUES (?, ?)""",
                    listOf(user.name, user.score))

    fun get_all_users() =
            conn.query("""SELECT * FROM `users`""",
                    listOf(),
                    userParser::multiAsync)

    fun get_or_create_user(user: User): Future<Long?> =
            asyncVx<Long?> {
                await(conn.query("SELECT id FROM users WHERE name = ?",
                        listOf(user.name),
                        longParser::parseAsync)) ?:
                        await(conn.query("INSERT INTO `users`(name, score) VALUES (?, ?)",
                                listOf(user.name, user.score),
                                longParser::parseAsync))
            }
}