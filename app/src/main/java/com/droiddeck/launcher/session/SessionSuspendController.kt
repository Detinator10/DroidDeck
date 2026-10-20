package com.droiddeck.launcher.session

import android.os.Process
import java.io.File
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors

internal class SessionSuspendController(
    private val sessionRoot: () -> Int,
    private val helperRoots: () -> List<Int>,
    private val suspendAudio: () -> Unit,
    private val resumeAudio: () -> Unit,
    private val procRoot: File = File("/proc"),
    private val signal: (Int, Int) -> Unit = Process::sendSignal,
    private val sleep: (Long) -> Unit = Thread::sleep,
    private val nowMillis: () -> Long = { System.nanoTime() / 1_000_000 },
    private val executor: ExecutorService = Executors.newSingleThreadExecutor(),
) {
    private enum class Role { SESSION_ROOT, SESSION_CHILD, HELPER_ROOT, HELPER_CHILD }

    private data class ProcessRecord(
        val pid: Int,
        val started: Long,
        val role: Role,
    )

    private data class ProcessInfo(
        val pid: Int,
        val parent: Int,
        val started: Long,
        val state: Char,
        val cpuTicks: Long,
    )

    private data class TaskIdentity(val process: ProcessRecord, val tid: Int, val started: Long)
    @Volatile private var closing = false
    private var frozen = emptyList<ProcessRecord>()

    fun freeze(completion: (Boolean) -> Unit) {
        submit({ completion(false) }) {
            if (closing) {
                completion(false)
                return@submit
            }
            if (frozen.isNotEmpty()) {
                val before = stoppedSnapshot(frozen)
                sleep(35L)
                val fullyFrozen = before != null && stoppedSnapshot(frozen) == before
                if (!fullyFrozen && restore(frozen)) frozen = emptyList()
                completion(fullyFrozen)
                return@submit
            }
            val records = LinkedHashMap<Pair<Int, Long>, ProcessRecord>()
            val root = sessionRoot()
            val started = readProcess(root)?.started
            if (root <= 1 || started == null) {
                completion(false)
                return@submit
            }
            runAudio(suspendAudio)
            val helpers = helperRoots().filter { it > 1 && it != root }.distinct()
            var previous: Map<TaskIdentity, Long>? = null
            var complete = false
            val deadline = nowMillis() + FREEZE_TIMEOUT_MS
            for (pass in 0 until FREEZE_MAX_PASSES) {
                if (closing) {
                    restore(records.values.toList())
                    runAudio(resumeAudio)
                    completion(false)
                    return@submit
                }
                if (nowMillis() >= deadline) break
                val includeRoots = pass >= 2
                if (!addSnapshot(records, root, started, helpers, includeRoots)) break
                stop(records.values.toList(), includeRoots)
                sleep(if (pass == 0) 70L else 35L)
                val current = stoppedSnapshot(records.values.toList())
                if (closing || nowMillis() >= deadline) break
                if (current != null && current == previous) {
                    complete = true
                    break
                }
                previous = current
            }
            if (complete) {
                frozen = records.values.toList()
                completion(true)
            } else {
                val restored = restore(records.values.toList())
                runAudio(resumeAudio)
                if (!restored) frozen = records.values.toList()
                completion(false)
            }
        }
    }

    fun resume(completion: (Boolean) -> Unit) {
        submit {
            val records = frozen
            runAudio(resumeAudio)
            val resumed = restore(records)
            if (resumed) frozen = emptyList()
            completion(resumed)
        }
    }

    fun closeAndResume(completion: () -> Unit) {
        closing = true
        submit {
            restore(frozen)
            frozen = emptyList()
            completion()
            executor.shutdown()
        }
    }

    private fun submit(rejected: (() -> Unit)? = null, task: () -> Unit) {
        try {
            executor.execute {
                try {
                    task()
                } catch (_: InterruptedException) {
                    Thread.currentThread().interrupt()
                }
            }
        } catch (_: RuntimeException) {
            if (rejected == null) task() else rejected()
        }
    }

    private fun runAudio(action: () -> Unit) {
        try {
            action()
        } catch (_: Exception) {
        }
    }

    private fun addSnapshot(
        records: MutableMap<Pair<Int, Long>, ProcessRecord>,
        root: Int,
        started: Long,
        helpers: List<Int>,
        includeRoots: Boolean,
    ): Boolean {
        val processes = readProcesses()
        val byPid = processes.associateBy { it.pid }
        if (byPid[root]?.started != started) return false
        for (process in descendants(root, processes, byPid)) {
            records[process.pid to process.started] = ProcessRecord(process.pid, process.started, Role.SESSION_CHILD)
        }
        for (helper in helpers) {
            for (process in descendants(helper, processes, byPid)) {
                records[process.pid to process.started] = ProcessRecord(process.pid, process.started, Role.HELPER_CHILD)
            }
        }
        if (includeRoots) {
            byPid[root]?.let { records[it.pid to it.started] = ProcessRecord(it.pid, it.started, Role.SESSION_ROOT) }
            for (helper in helpers) {
                byPid[helper]?.let { records[it.pid to it.started] = ProcessRecord(it.pid, it.started, Role.HELPER_ROOT) }
            }
        }
        return true
    }

    private fun descendants(root: Int, processes: List<ProcessInfo>, byPid: Map<Int, ProcessInfo>): List<ProcessInfo> {
        if (root !in byPid) return emptyList()
        val children = HashMap<Int, MutableList<ProcessInfo>>()
        for (process in processes) children.getOrPut(process.parent) { ArrayList() }.add(process)
        val excluded = Process.myPid()
        val found = ArrayList<ProcessInfo>()
        val seen = HashSet<Int>()
        val queue = ArrayDeque<Int>().apply { add(root) }
        while (queue.isNotEmpty()) {
            val parent = queue.removeFirst()
            for (child in children[parent].orEmpty()) {
                if (child.pid <= 1 || child.pid == excluded || !seen.add(child.pid)) continue
                found.add(child)
                queue.add(child.pid)
            }
        }
        return found
    }

    private fun readProcesses(): List<ProcessInfo> =
        procRoot.listFiles().orEmpty().mapNotNull { entry ->
            val pid = entry.name.toIntOrNull() ?: return@mapNotNull null
            readProcess(pid)
        }

    private fun readProcess(pid: Int, statFile: File = File(procRoot, "$pid/stat")): ProcessInfo? {
        val stat = try {
            statFile.readText()
        } catch (_: Exception) {
            return null
        }
        val close = stat.lastIndexOf(')')
        if (close < 0 || close + 2 >= stat.length) return null
        val fields = stat.substring(close + 2).trim().split(Regex("\\s+"))
        if (fields.size < 20) return null
        return try {
            ProcessInfo(pid, fields[1].toInt(), fields[19].toLong(), fields[0].first(), fields[11].toLong() + fields[12].toLong())
        } catch (_: NumberFormatException) {
            null
        }
    }

    private fun stop(records: List<ProcessRecord>, includeRoots: Boolean) {
        val descendants = records.filter { it.role == Role.SESSION_CHILD || it.role == Role.HELPER_CHILD }
        val session = records.filter { it.role == Role.SESSION_ROOT }
        val helpers = records.filter { it.role == Role.HELPER_ROOT }
        for (record in descendants) send(record, SIGSTOP)
        if (includeRoots) {
            for (record in session) send(record, SIGSTOP)
            for (record in helpers) send(record, SIGSTOP)
        }
    }

    private fun restore(records: List<ProcessRecord>): Boolean {
        val ordered = records.sortedBy {
            when (it.role) {
                Role.SESSION_ROOT -> 0
                Role.HELPER_ROOT -> 1
                Role.SESSION_CHILD -> 2
                Role.HELPER_CHILD -> 3
            }
        }
        for (record in ordered) send(record, SIGCONT)
        val waiting = ordered.toMutableList()
        repeat(RESTORE_CHECKS) { check ->
            sleep(25L)
            waiting.removeAll { isRunningOrGone(it) }
            if (waiting.isEmpty()) return true
            if (check % 8 == 7) for (record in waiting) send(record, SIGCONT)
        }
        return false
    }

    private fun send(record: ProcessRecord, signal: Int) {
        val current = readProcess(record.pid) ?: return
        if (current.started != record.started) return
        try {
            this.signal(record.pid, signal)
        } catch (_: Throwable) {
        }
    }

    private fun stoppedSnapshot(records: List<ProcessRecord>): Map<TaskIdentity, Long>? {
        // PRoot can leave the leader stopped while native worker threads keep running.
        try {
            val tasks = LinkedHashMap<TaskIdentity, Long>()
            var liveRoot = false
            for (record in records) {
                val current = readProcess(record.pid)
                if (current == null) {
                    if (procRoot.list()?.contains(record.pid.toString()) != false) return null
                    continue
                }
                if (current.started != record.started) continue
                val entries = File(procRoot, "${record.pid}/task").listFiles() ?: return null
                if (entries.none { it.name.toIntOrNull() != null }) return null
                for (entry in entries) {
                    val tid = entry.name.toIntOrNull() ?: continue
                    val task = readProcess(tid, File(entry, "stat")) ?: return null
                    if (task.state != 'T' && task.state != 't' && task.state != 'Z' && task.state != 'X') return null
                    if (record.role == Role.SESSION_ROOT && task.state != 'Z' && task.state != 'X') liveRoot = true
                    tasks[TaskIdentity(record, tid, task.started)] = task.cpuTicks
                }
                if (readProcess(record.pid)?.started != record.started) return null
            }
            return tasks.takeIf { liveRoot && it.isNotEmpty() }
        } catch (_: Exception) {
            return null
        }
    }

    private fun isRunningOrGone(record: ProcessRecord): Boolean {
        val current = readProcess(record.pid) ?: return procRoot.list()?.contains(record.pid.toString()) == false
        if (current.started != record.started) return true
        return current.state != 'T' && current.state != 't'
    }

    private companion object {
        const val SIGSTOP = 19
        const val SIGCONT = 18
        const val RESTORE_CHECKS = 80
        const val FREEZE_TIMEOUT_MS = 8_000L
        const val FREEZE_MAX_PASSES = 256
    }
}
