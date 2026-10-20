package com.droiddeck.launcher.session

import java.io.File
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder

class SessionSuspendControllerTest {
    @get:Rule val temporary = TemporaryFolder()

    @Test fun partialGroupStopsNeedMoreThanTenPasses() {
        for (count in listOf(17, 68)) {
            val fixture = fixture(count)
            assertEquals(listOf(true), fixture.freeze())
            assertTrue(fixture.game.threads.all { it.state == 't' })
            assertTrue(fixture.signals.count { it.pid == GAME && it.signal == STOP } >= count)
        }
    }

    @Test fun cpuThreadMembershipAndLifetimesMustStabilize() {
        val fixture = fixture(17)
        var changes = 0
        fixture.afterSleep = {
            if (fixture.game.threads.all { it.state == 't' }) {
                when (changes) {
                    in 0..2 -> fixture.game.threads.last().cpu++
                    3 -> fixture.game.threads += Task(500, 9000, 'S')
                    4 -> fixture.game.threads.last().apply { started++; state = 'S' }
                }
                changes++
            }
        }
        assertEquals(listOf(true), fixture.freeze())
        assertTrue(changes >= 6)
        assertEquals(18, fixture.game.threads.size)
        assertTrue(fixture.game.threads.all { it.state == 't' })
    }

    @Test fun zombieLeaderDoesNotHideRunningWorkers() {
        val fixture = fixture(17)
        fixture.game.threads.first().state = 'Z'
        fixture.sync()
        assertEquals(listOf(true), fixture.freeze())
        assertEquals('Z', fixture.game.threads.first().state)
        assertTrue(fixture.game.threads.drop(1).all { it.state == 't' })
    }

    @Test fun deadSessionRootCannotCountAsSuccessfullyFrozen() {
        val fixture = fixture(17)
        fixture.root.threads.first().state = 'Z'
        fixture.game.threads.forEach { it.state = 't' }
        fixture.sync()
        assertEquals(listOf(false), fixture.freeze())
        assertEquals(1, fixture.audioResumes)
    }

    @Test fun unavailableOrEmptyInventoryFailsAndKnownProcessesRemainRecoverable() {
        for (broken in Broken.entries) {
            val fixture = fixture(17)
            fixture.afterSleep = {
                if (fixture.root.threads.first().state == 'T') fixture.broken = broken
            }
            assertEquals(listOf(false), fixture.freeze())
            assertEquals(1, fixture.audioResumes)
            fixture.afterSleep = null
            fixture.broken = null
            fixture.sync()
            assertEquals(listOf(true), fixture.resume())
            assertTrue(fixture.game.threads.all { it.state == 'S' })
        }
    }

    @Test fun timeoutRollsBackKnownProcessesAndAudio() {
        val fixture = fixture(17)
        fixture.neverStop = true
        assertEquals(listOf(false), fixture.freeze())
        assertTrue(fixture.now <= 8100)
        assertEquals(ROOT, fixture.signals.first { it.signal == CONT }.pid)
        assertTrue(fixture.game.threads.all { it.state == 'S' })
        assertEquals(1, fixture.audioResumes)
    }

    @Test fun lostOrReusedSessionRootFailsWithoutSignallingReplacement() {
        for (reuse in listOf(false, true)) {
            val fixture = fixture(17)
            var atChange = -1
            fixture.afterSleep = {
                if (atChange < 0 && fixture.root.threads.first().state == 'T') {
                    atChange = fixture.signals.size
                    if (reuse) fixture.root.threads.first().apply { started++; state = 'S' }
                    else fixture.rootGone = true
                }
            }
            assertEquals(listOf(false), fixture.freeze())
            assertTrue(atChange > 0)
            assertFalse(fixture.signals.drop(atChange).any { it.pid == ROOT })
            assertTrue(fixture.game.threads.all { it.state == 'S' })
        }
    }

    @Test fun closeDuringFreezeCancelsAndRestoresSavedRoot() {
        val executor = QueueExecutor()
        val fixture = fixture(68, executor)
        var closed = false
        var requested = false
        fixture.afterSleep = {
            if (!requested && fixture.root.threads.first().state == 'T') {
                requested = true
                fixture.rootPid = -1
                fixture.controller.closeAndResume { closed = true }
            }
        }
        val results = fixture.freeze()
        executor.drain()
        assertEquals(listOf(false), results)
        assertTrue(closed)
        assertTrue(executor.isShutdown)
        assertTrue(fixture.game.threads.all { it.state == 'S' })
        assertEquals(1, fixture.audioResumes)
    }

    @Test fun alreadyFrozenSessionRequiresStoppedAndStableThreads() {
        for (cpuChanges in listOf(false, true)) {
            val fixture = fixture(17)
            assertEquals(listOf(true), fixture.freeze())
            if (cpuChanges) fixture.afterSleep = { fixture.game.threads.last().cpu++ }
            else fixture.game.threads.last().state = 'S'
            fixture.sync()
            assertEquals(listOf(false), fixture.freeze())
            assertTrue(fixture.game.threads.all { it.state == 'S' })
        }
    }

    @Test fun resumePreservesRootFirstOrderingAndSavedIdentities() {
        val fixture = fixture(17)
        assertEquals(listOf(true), fixture.freeze())
        val before = fixture.signals.size
        fixture.rootPid = -1
        assertEquals(listOf(true), fixture.resume())
        assertEquals(ROOT, fixture.signals.drop(before).first().pid)
        assertTrue(fixture.game.threads.all { it.state == 'S' })
    }

    @Test fun rejectedExecutorDoesNotFreezeOnTheCallingThread() {
        val fixture = fixture(17, QueueExecutor().apply { shutdown() })
        assertEquals(listOf(false), fixture.freeze())
        assertTrue(fixture.signals.isEmpty())
        assertEquals(0, fixture.audioSuspends)
    }

    private fun fixture(count: Int, executor: QueueExecutor = InlineExecutor()) =
        Fixture(temporary.newFolder(), count, executor)

    private enum class Broken { PROCESS_STAT, TASK_DIRECTORY, EMPTY_TASKS, TASK_STAT }
    private data class Task(val tid: Int, var started: Long, var state: Char, var cpu: Long = 0)
    private data class Group(val pid: Int, val parent: Int, val threads: MutableList<Task>)
    private data class SentSignal(val pid: Int, val signal: Int)

    private class Fixture(val proc: File, count: Int, executor: QueueExecutor) {
        val root = Group(ROOT, 9000, mutableListOf(Task(ROOT, 1000, 'S')))
        val game = Group(GAME, ROOT, MutableList(count) { Task(GAME + it, 2000L + it, 'S') })
        val signals = mutableListOf<SentSignal>()
        var rootPid = ROOT
        var now = 0L
        var audioSuspends = 0
        var audioResumes = 0
        var neverStop = false
        var broken: Broken? = null
        var rootGone = false
        var afterSleep: (() -> Unit)? = null
        private var continuing = false
        val controller = SessionSuspendController(
            { rootPid }, { emptyList() }, { audioSuspends++ }, { audioResumes++ },
            proc, ::signal, ::sleep, { now }, executor,
        )

        init { sync() }
        fun freeze() = mutableListOf<Boolean>().also { results -> controller.freeze { results += it } }
        fun resume() = mutableListOf<Boolean>().also { results -> controller.resume { results += it } }

        private fun signal(pid: Int, signal: Int) {
            signals += SentSignal(pid, signal)
            val group = if (pid == ROOT) root else game
            if (signal == STOP) {
                continuing = false
                if (pid == ROOT) group.threads.forEach { if (it.state != 'Z') it.state = 'T' }
                else group.threads.firstOrNull {
                    it.state !in listOf('T', 't', 'Z', 'X') && !(neverStop && it == game.threads.last())
                }?.state = 't'
            } else if (signal == CONT) {
                continuing = true
                group.threads.forEach { if (it.state != 'Z') it.state = 'S' }
            }
            sync()
        }

        private fun sleep(millis: Long) {
            now += millis
            if (root.threads.first().state == 'S' && !continuing) game.threads.forEach {
                if (it.state != 'Z') { it.state = 'S'; it.cpu++ }
            }
            afterSleep?.invoke()
            sync()
        }

        fun sync() {
            for (group in listOf(root, game)) {
                val dir = File(proc, group.pid.toString())
                if (group == root && rootGone) { dir.deleteRecursively(); continue }
                dir.mkdirs()
                val statFile = File(dir, "stat")
                if (group == game && broken == Broken.PROCESS_STAT) statFile.delete()
                else statFile.writeText(stat(group, group.threads.first()))
                val taskDir = File(dir, "task")
                if (group == game && broken == Broken.TASK_DIRECTORY) { taskDir.deleteRecursively(); continue }
                taskDir.mkdirs()
                if (group == game && broken == Broken.EMPTY_TASKS) {
                    taskDir.listFiles().orEmpty().forEach { it.deleteRecursively() }
                    continue
                }
                for (task in group.threads) {
                    val entry = File(taskDir, task.tid.toString()).apply { mkdirs() }
                    File(entry, "stat").writeText(if (group == game && broken == Broken.TASK_STAT) "invalid" else stat(group, task))
                }
            }
        }

        private fun stat(group: Group, task: Task): String {
            val fields = MutableList(20) { "0" }
            fields[0] = task.state.toString()
            fields[1] = group.parent.toString()
            fields[11] = task.cpu.toString()
            fields[19] = task.started.toString()
            return "${task.tid} (fixture (worker)) ${fields.joinToString(" ")}"
        }
    }

    private open class QueueExecutor : AbstractExecutorService() {
        private val queue = ArrayDeque<Runnable>()
        private var stopped = false
        override fun execute(command: Runnable) {
            if (stopped) throw RejectedExecutionException()
            queue.add(command)
        }
        fun drain() { while (queue.isNotEmpty()) queue.removeFirst().run() }
        override fun shutdown() { stopped = true }
        override fun shutdownNow(): MutableList<Runnable> {
            stopped = true
            return queue.toMutableList().also { queue.clear() }
        }
        override fun isShutdown() = stopped
        override fun isTerminated() = stopped && queue.isEmpty()
        override fun awaitTermination(timeout: Long, unit: TimeUnit) = isTerminated
    }

    private class InlineExecutor : QueueExecutor() {
        override fun execute(command: Runnable) { super.execute(command); drain() }
    }

    private companion object {
        const val ROOT = 100
        const val GAME = 200
        const val STOP = 19
        const val CONT = 18
    }
}
