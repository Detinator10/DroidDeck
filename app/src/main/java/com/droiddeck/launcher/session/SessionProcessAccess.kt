package com.droiddeck.launcher.session

import android.os.Process
import android.os.SystemClock
import android.system.ErrnoException
import android.system.Os
import android.system.OsConstants
import android.util.Log
import java.io.File
import java.io.IOException

internal data class SessionThreadInfo(
    val tid: Int,
    val started: Long,
    val state: Char,
    val cpuTicks: Long = 0,
    val stopSignal: Int = 0,
)

internal data class SessionProcessInfo(
    val pid: Int,
    val parent: Int,
    val started: Long,
    val uid: Int,
    val tracer: Int,
    val state: Char,
    val threads: List<SessionThreadInfo> = emptyList(),
)

internal interface SessionProcessAccess {
    val myPid: Int
    val myUid: Int
    fun nowMillis(): Long
    fun sleep(millis: Long)
    fun processes(): List<SessionProcessInfo>
    fun read(pid: Int, includeThreads: Boolean = false): SessionProcessInfo?
    fun signal(pid: Int, signal: Int)
    fun report(message: String) {}
}

internal class AndroidSessionProcessAccess : SessionProcessAccess {
    override val myPid = Process.myPid()
    override val myUid = Process.myUid()
    override fun nowMillis() = SystemClock.elapsedRealtime()
    override fun sleep(millis: Long) = Thread.sleep(millis)
    override fun signal(pid: Int, signal: Int) {
        try { Os.kill(pid, signal) } catch (error: ErrnoException) { throw IOException(error) }
    }
    override fun report(message: String) { Log.i("SessionSuspend", message) }

    override fun processes(): List<SessionProcessInfo> {
        val entries = File("/proc").listFiles() ?: throw IOException("Cannot enumerate processes")
        return entries.mapNotNull { entry ->
            val pid = entry.name.toIntOrNull() ?: return@mapNotNull null
            try {
                read(pid)?.takeIf { it.uid == myUid }
            } catch (error: IOException) {
                // Android may hide other UIDs' proc records; owned records must be readable.
                val owner = try { stat(entry.path) } catch (_: IOException) { null }
                    ?: return@mapNotNull null
                if (owner.st_uid == myUid) throw error
                null
            }
        }
    }

    override fun read(pid: Int, includeThreads: Boolean): SessionProcessInfo? {
        val base = File("/proc/$pid")
        val raw = readText(File(base, "stat")) ?: return null
        val fields = parseStat(raw)
        val status = readText(File(base, "status")) ?: return null
        val uid = statusNumber(status, "Uid:")
        val tracer = statusNumber(status, "TracerPid:")
        val started = number(fields[19])
        val threads = if (includeThreads) {
            val entries = File(base, "task").listFiles()
            if (entries == null) {
                if (stat(base.path) == null) return null
                throw IOException("Cannot enumerate threads: $pid")
            }
            entries.mapNotNull { entry ->
                val tid = entry.name.toIntOrNull() ?: return@mapNotNull null
                val text = readText(File(entry, "stat")) ?: return@mapNotNull null
                val thread = parseStat(text)
                SessionThreadInfo(tid, number(thread[19]), thread[0].single(),
                    number(thread[11]) + number(thread[12]), number(thread[49]).toInt())
            }.also {
                if (it.isEmpty() && fields[0].single() !in setOf('Z', 'X')) throw IOException("Empty thread list: $pid")
                val check = readText(File(base, "stat")) ?: return null
                if (number(parseStat(check)[19]) != started) throw IOException("Process changed during inspection: $pid")
            }
        } else emptyList()
        return SessionProcessInfo(pid, number(fields[1]).toInt(), started, uid, tracer, fields[0].single(), threads)
    }

    private fun readText(file: File): String? = try {
        file.readText()
    } catch (error: IOException) {
        if (stat(file.path) == null) null else throw error
    }

    private fun stat(path: String) = try {
        Os.stat(path)
    } catch (error: ErrnoException) {
        if (error.errno == OsConstants.ENOENT || error.errno == OsConstants.ESRCH) null else throw IOException(error)
    }

    private fun parseStat(text: String): List<String> {
        val close = text.lastIndexOf(')')
        if (close < 0 || close + 2 >= text.length) throw IOException("Malformed proc stat")
        val fields = text.substring(close + 2).trim().split(WHITESPACE)
        if (fields.size < 50 || fields[0].length != 1) throw IOException("Incomplete proc stat")
        return fields
    }

    private fun number(text: String): Long = text.toLongOrNull() ?: throw IOException("Malformed proc number")
    private fun statusNumber(text: String, key: String): Int {
        val value = text.lineSequence().firstOrNull { it.startsWith(key) }
            ?.substring(key.length)?.trim()?.split(WHITESPACE)?.firstOrNull()
            ?: throw IOException("Missing proc status field: $key")
        return value.toIntOrNull() ?: throw IOException("Malformed proc status field: $key")
    }

    private companion object {
        val WHITESPACE = Regex("\\s+")
    }
}
