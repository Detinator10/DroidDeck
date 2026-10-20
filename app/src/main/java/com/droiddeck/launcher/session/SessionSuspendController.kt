package com.droiddeck.launcher.session

import java.io.IOException
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.atomic.AtomicLong

internal class SessionSuspendController(
    private val sessionRoot: () -> Int,
    private val helperRoots: () -> List<Int>,
    private val suspendAudio: () -> Unit,
    private val resumeAudio: () -> Unit,
    private val access: SessionProcessAccess = AndroidSessionProcessAccess(),
    private val executor: ExecutorService = Executors.newSingleThreadExecutor(),
) {
    private enum class Role { SESSION_ROOT, SESSION_CHILD, HELPER_ROOT, HELPER_CHILD }
    private data class Identity(val pid: Int, val started: Long)
    private data class ProcessRecord(val identity: Identity, val role: Role)
    private data class TaskIdentity(val process: Identity, val tid: Int, val started: Long)
    private data class Scope(val root: Identity?, val roots: List<ProcessRecord>)
    private data class Snapshot(val processes: Map<ProcessRecord, SessionProcessInfo>) {
        val tasks get() = processes.flatMap { (record, process) ->
            process.threads.map { TaskIdentity(record.identity, it.tid, it.started) to it }
        }.toMap()
        val stopped get() = processes.isNotEmpty() && processes.values.all { process ->
            process.threads.isNotEmpty() && process.threads.all { it.state in STOPPED_STATES }
        }
        val fingerprint get() = tasks.mapValues { it.value.cpuTicks }
    }
    private class ResumeProgress(thread: SessionThreadInfo) {
        var cpu = thread.cpuTicks
        var signal = thread.stopSignal
        var executed = thread.state !in TRACED_STATES
        fun observe(thread: SessionThreadInfo): Boolean {
            if (thread.state == 'T' || thread.state == 't' && thread.stopSignal in STOP_SIGNALS) {
                cpu = thread.cpuTicks
                signal = thread.stopSignal
                executed = false
                return false
            }
            if (thread.state != 't' || thread.cpuTicks > cpu || thread.stopSignal != signal) executed = true
            return executed
        }
    }

    @Volatile private var closing = false
    private val requests = AtomicLong()
    private var frozen = emptyList<ProcessRecord>()

    fun freeze(completion: (Boolean) -> Unit) {
        val request = requests.get()
        submit({ completion(freezeNow(request)) }, { completion(false) })
    }

    fun resume(completion: (Boolean) -> Unit) {
        requests.incrementAndGet()
        submit({
            runAudio(resumeAudio)
            val resumed = restoreSafely(frozen)
            if (resumed) frozen = emptyList()
            completion(resumed)
        }, { completion(false) })
    }

    fun closeAndResume(completion: () -> Unit) {
        closing = true
        requests.incrementAndGet()
        submit({
            try {
                if (restoreSafely(frozen)) frozen = emptyList()
                completion()
            } finally {
                executor.shutdown()
            }
        }, completion)
    }

    private fun freezeNow(request: Long): Boolean {
        val records = frozen.associateByTo(LinkedHashMap()) { it.identity }
        var rollback = records.isNotEmpty()
        var success = false
        var interrupted = false
        try {
            if (cancelled(request)) return false
            val scope = if (records.isNotEmpty()) {
                runAudio(suspendAudio)
                scope(records.values.toList())
            } else {
                val root = captureRoot(sessionRoot(), Role.SESSION_ROOT)
                    ?: throw IOException("Session root is unavailable")
                records[root.identity] = root
                runAudio(suspendAudio)
                rollback = true
                val helpers = helperRoots().filter { it > 1 && it != root.identity.pid }.distinct()
                    .mapNotNull { captureRoot(it, Role.HELPER_ROOT) }
                helpers.forEach { records[it.identity] = it }
                Scope(root.identity, listOf(root) + helpers)
            }
            val deadline = access.nowMillis() + FREEZE_TIMEOUT_MS
            var previous: Map<TaskIdentity, Long>? = null
            var stable = 0
            for (pass in 0 until FREEZE_MAX_PASSES) {
                if (cancelled(request) || access.nowMillis() >= deadline) break
                try {
                    val before = snapshot(scope, records, requireRoot = true)
                    for ((record, process) in before.processes.entries.sortedBy { stopOrder(it.key.role) }) {
                        if (pass < 2 && record.role in ROOT_ROLES) continue
                        if (process.threads.any { it.state !in STOPPED_STATES }) send(record, SIGSTOP)
                    }
                    access.sleep(if (pass == 0) 70L else 35L)
                    if (cancelled(request) || access.nowMillis() >= deadline) break
                    val after = snapshot(scope, records, requireRoot = true)
                    val fingerprint = after.fingerprint
                    stable = if (after.stopped) {
                        if (fingerprint == previous) stable + 1 else 1
                    } else 0
                    previous = fingerprint
                    if (stable >= STABLE_PASSES) {
                        frozen = records.values.toList()
                        success = true
                        report("Session frozen: ${after.tasks.size} tasks, ${pass + 1} passes")
                        return true
                    }
                } catch (error: IOException) {
                    stable = 0
                    previous = null
                    report("Suspend inspection failed: ${error.message}")
                    access.sleep(35L)
                }
            }
        } catch (_: InterruptedException) {
            interrupted = true
            Thread.interrupted()
        } catch (error: Exception) {
            report("Suspend failed: ${error.message}")
        } finally {
            if (!success && rollback) {
                frozen = records.values.toList()
                try {
                    if (restoreSafely(frozen)) frozen = emptyList()
                } finally {
                    runAudio(resumeAudio)
                }
            }
            if (interrupted) Thread.currentThread().interrupt()
        }
        return false
    }

    private fun cancelled(request: Long) = closing || request != requests.get()

    private fun captureRoot(pid: Int, role: Role): ProcessRecord? {
        if (pid <= 1 || pid == access.myPid) return null
        val process = access.read(pid) ?: return null
        if (process.uid != access.myUid || process.state in DEAD_STATES) return null
        return ProcessRecord(Identity(pid, process.started), role)
    }

    private fun scope(records: List<ProcessRecord>): Scope {
        val roots = records.filter { it.role in ROOT_ROLES }
        return Scope(roots.singleOrNull { it.role == Role.SESSION_ROOT }?.identity, roots)
    }

    private fun snapshot(
        scope: Scope,
        records: MutableMap<Identity, ProcessRecord>,
        requireRoot: Boolean,
    ): Snapshot {
        val visible = access.processes().filter { it.uid == access.myUid && it.pid != access.myPid }
            .associateByTo(HashMap()) { it.pid }
        for (record in records.values + scope.roots) {
            if (record.identity.pid in visible) continue
            val process = access.read(record.identity.pid) ?: continue
            if (process.uid == access.myUid && process.started == record.identity.started) visible[process.pid] = process
        }
        val children = visible.values.groupBy { it.parent }
        val selected = LinkedHashMap<Int, Role>()
        for (record in scope.roots) {
            val root = visible[record.identity.pid]?.takeIf { it.started == record.identity.started }
                ?: continue
            if (root.state in DEAD_STATES) continue
            selected[root.pid] = record.role
            val queue = ArrayDeque<Int>().apply { add(root.pid) }
            val seen = hashSetOf(root.pid)
            while (queue.isNotEmpty()) {
                for (child in children[queue.removeFirst()].orEmpty()) {
                    if (child.pid <= 1 || !seen.add(child.pid)) continue
                    selected.putIfAbsent(child.pid, if (record.role == Role.SESSION_ROOT) Role.SESSION_CHILD else Role.HELPER_CHILD)
                    queue.add(child.pid)
                }
            }
        }
        val root = scope.root?.let { identity -> visible[identity.pid]?.takeIf { it.started == identity.started } }
        if (requireRoot && (root == null || root.state in DEAD_STATES)) throw IOException("Session root exited or changed")
        if (root != null) for (process in visible.values) {
            if (process.tracer == root.pid) selected.putIfAbsent(process.pid, Role.SESSION_CHILD)
        }
        for (record in records.values) {
            if (visible[record.identity.pid]?.started == record.identity.started) selected[record.identity.pid] = record.role
        }
        val result = LinkedHashMap<ProcessRecord, SessionProcessInfo>()
        for ((pid, role) in selected) {
            val process = access.read(pid, includeThreads = true) ?: continue
            if (process.uid != access.myUid || process.started != visible[pid]?.started) {
                throw IOException("Process identity changed during inspection: $pid")
            }
            if (process.state in DEAD_STATES) continue
            val identity = Identity(pid, process.started)
            val record = records.getOrPut(identity) { ProcessRecord(identity, role) }
            result[record] = process
        }
        if (requireRoot && result.keys.none { it.identity == scope.root }) throw IOException("Session root disappeared")
        return Snapshot(result)
    }

    private fun restore(original: List<ProcessRecord>): Boolean {
        if (original.isEmpty()) return true
        val records = original.associateByTo(LinkedHashMap()) { it.identity }
        // stopSession clears the live session PID before this restoration runs.
        val scope = scope(original)
        val progress = HashMap<TaskIdentity, ResumeProgress>()
        val begin = access.nowMillis()
        val deadline = begin + RESTORE_TIMEOUT_MS
        var nextContinue = begin
        var previous: Set<TaskIdentity>? = null
        var quietSince: Long? = null
        var interrupted = Thread.interrupted()
        var confirmed = false
        try {
            for (pass in 0 until RESTORE_MAX_PASSES) {
                if (access.nowMillis() >= deadline) break
                try {
                    val current = snapshot(scope, records, requireRoot = false)
                    if (access.nowMillis() >= nextContinue) {
                        for (record in current.processes.keys.sortedBy { resumeOrder(it.role) }) send(record, SIGCONT)
                        nextContinue = access.nowMillis() + CONTINUE_INTERVAL_MS
                    }
                    val tasks = current.tasks
                    var ready = true
                    for ((identity, thread) in tasks) {
                        if (!progress.getOrPut(identity) { ResumeProgress(thread) }.observe(thread)) ready = false
                    }
                    progress.keys.retainAll(tasks.keys)
                    if (current.processes.any { (record, process) -> record.role in ROOT_ROLES && process.state in TRACED_STATES }) ready = false
                    val identities = tasks.keys.toSet()
                    if (ready && identities == previous) {
                        if (quietSince == null) quietSince = access.nowMillis()
                        if (access.nowMillis() - begin >= RESUME_MINIMUM_MS &&
                            access.nowMillis() - quietSince >= RESUME_QUIET_MS) {
                            confirmed = true
                            return true
                        }
                    } else quietSince = null
                    previous = identities
                } catch (error: IOException) {
                    previous = null
                    quietSince = null
                    report("Resume inspection failed: ${error.message}")
                    if (access.nowMillis() >= nextContinue) {
                        for (record in records.values.sortedBy { resumeOrder(it.role) }) {
                            try { send(record, SIGCONT) } catch (_: IOException) {}
                        }
                        nextContinue = access.nowMillis() + CONTINUE_INTERVAL_MS
                    }
                }
                try { access.sleep(25L) } catch (_: InterruptedException) {
                    interrupted = true
                    Thread.interrupted()
                }
            }
        } finally {
            if (!confirmed) frozen = records.values.toList()
            if (interrupted) Thread.currentThread().interrupt()
        }
        report("Resume unconfirmed; retaining ${records.size} process identities")
        return false
    }

    private fun restoreSafely(records: List<ProcessRecord>): Boolean = try {
        restore(records)
    } catch (error: Exception) {
        report("Resume failed: ${error.message}")
        false
    }

    private fun send(record: ProcessRecord, signal: Int) {
        val process = access.read(record.identity.pid) ?: return
        if (process.started != record.identity.started || process.uid != access.myUid || process.pid == access.myPid) return
        try {
            access.signal(process.pid, signal)
        } catch (error: IOException) {
            val after = access.read(process.pid)
            if (after != null && after.started == process.started) throw error
        }
    }

    private fun submit(task: () -> Unit, rejected: () -> Unit) {
        try { executor.execute(task) } catch (_: RejectedExecutionException) { rejected() }
    }

    private fun runAudio(action: () -> Unit) {
        try { action() } catch (error: Exception) { report("Audio suspend callback failed: ${error.message}") }
    }

    private fun report(message: String) {
        try { access.report(message) } catch (_: Exception) {}
    }

    private fun stopOrder(role: Role) = when (role) {
        Role.SESSION_ROOT -> 1
        Role.HELPER_ROOT -> 2
        else -> 0
    }
    private fun resumeOrder(role: Role) = when (role) {
        Role.SESSION_ROOT -> 0
        Role.HELPER_ROOT -> 1
        Role.SESSION_CHILD -> 2
        Role.HELPER_CHILD -> 3
    }

    private companion object {
        const val SIGSTOP = 19
        const val SIGCONT = 18
        const val FREEZE_TIMEOUT_MS = 8_000L
        const val FREEZE_MAX_PASSES = 256
        const val RESTORE_TIMEOUT_MS = 5_000L
        const val RESTORE_MAX_PASSES = 256
        const val STABLE_PASSES = 2
        const val CONTINUE_INTERVAL_MS = 200L
        const val RESUME_MINIMUM_MS = 500L
        const val RESUME_QUIET_MS = 400L
        val ROOT_ROLES = setOf(Role.SESSION_ROOT, Role.HELPER_ROOT)
        val TRACED_STATES = setOf('T', 't')
        val DEAD_STATES = setOf('Z', 'X')
        val STOPPED_STATES = TRACED_STATES + DEAD_STATES
        val STOP_SIGNALS = 19..22
    }
}
