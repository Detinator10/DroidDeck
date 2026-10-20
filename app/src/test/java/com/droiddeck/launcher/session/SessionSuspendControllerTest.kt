package com.droiddeck.launcher.session

import java.io.IOException
import java.util.ArrayDeque
import java.util.concurrent.AbstractExecutorService
import java.util.concurrent.RejectedExecutionException
import java.util.concurrent.TimeUnit
import org.junit.Assert.assertEquals
import org.junit.Assert.assertFalse
import org.junit.Assert.assertTrue
import org.junit.Test

class SessionSuspendControllerTest {
    @Test fun everyWorkerMustStopBeforeFreezeCompletes() {
        for (threadCount in listOf(17, 68)) {
            val fixture = Fixture(threadCount)
            val results = mutableListOf<Boolean>()

            fixture.controller.freeze { results += it }

            assertEquals(listOf(true), results)
            assertTrue(fixture.access.game.threads.all { it.state == 't' })
            assertEquals('T', fixture.access.root.state)
            assertTrue(fixture.access.signals.count { it.pid == GAME && it.signal == STOP } >= threadCount)
            assertEquals(1, fixture.audioSuspends)
            assertEquals(0, fixture.audioResumes)
        }
    }

    @Test fun repeatedLargeThreadGroupsFreezeAndResumeWithoutLosingWorkers() {
        val fixture = Fixture(68)
        repeat(6) {
            val freeze = mutableListOf<Boolean>()
            fixture.controller.freeze { freeze += it }
            assertEquals(listOf(true), freeze)
            assertTrue(fixture.access.game.threads.all { it.state == 't' })
            val stoppedTicks = fixture.access.game.threads.sumOf { it.cpu }
            fixture.access.sleep(500)
            assertEquals(stoppedTicks, fixture.access.game.threads.sumOf { it.cpu })

            val signalStart = fixture.access.signals.size
            val resume = mutableListOf<Boolean>()
            fixture.controller.resume { resume += it }

            assertEquals(listOf(true), resume)
            assertEquals('S', fixture.access.root.state)
            assertTrue(fixture.access.game.threads.all { it.state == 'S' })
            assertEquals(ROOT, fixture.access.signals.drop(signalStart).first { it.signal == CONT }.pid)
        }
        assertEquals(6, fixture.audioSuspends)
        assertEquals(6, fixture.audioResumes)
    }

    @Test fun newThreadDuringFreezeMustJoinTheStableInventory() {
        val fixture = Fixture(17)
        var added = false
        fixture.access.afterSleep = {
            if (!added && fixture.access.game.threads.all { it.state == 't' }) {
                added = true
                fixture.access.game.threads += MutableThread(900, 9000, 'S')
            }
        }
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertTrue(added)
        assertEquals(listOf(true), results)
        assertEquals(18, fixture.access.game.threads.size)
        assertTrue(fixture.access.game.threads.all { it.state == 't' })
    }

    @Test fun reusedTidMustBeRecheckedBeforeFreezeCompletes() {
        val fixture = Fixture(17)
        var replaced = false
        fixture.access.afterSleep = {
            if (!replaced && fixture.access.game.threads.all { it.state == 't' }) {
                replaced = true
                val old = fixture.access.game.threads.last()
                fixture.access.game.threads[fixture.access.game.threads.lastIndex] =
                    old.copy(started = old.started + 1000, state = 'S', cpu = 0, stopSignal = 0)
            }
        }
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertTrue(replaced)
        assertEquals(listOf(true), results)
        assertTrue(fixture.access.game.threads.all { it.state == 't' })
        assertTrue(fixture.access.signals.count { it.pid == GAME && it.signal == STOP } >= 18)
    }

    @Test fun lateReparentedTraceeMustJoinFreezeAndResume() {
        val fixture = Fixture(17)
        val late = MutableGroup(600, 1, fixture.access.myUid, ROOT, 6000,
            MutableList(3) { MutableThread(600 + it, 6000L + it, 'S') })
        var added = false
        fixture.access.afterSleep = {
            if (!added && fixture.access.root.state == 'T') {
                added = true
                fixture.access.extraProcesses += late
            }
        }
        val frozen = mutableListOf<Boolean>()

        fixture.controller.freeze { frozen += it }

        assertTrue(added)
        assertEquals(listOf(true), frozen)
        assertTrue(late.threads.all { it.state == 't' })
        assertTrue(fixture.access.signals.any { it.pid == late.pid && it.signal == STOP })
        fixture.controller.resume { assertTrue(it) }
        assertTrue(late.threads.all { it.state == 'S' })
        assertTrue(fixture.access.signals.any { it.pid == late.pid && it.signal == CONT })
    }

    @Test fun uncooperativeWorkerHasABoundedFailureAndIsRolledBack() {
        val fixture = Fixture(17)
        fixture.access.neverStopTid = fixture.access.game.threads.last().tid
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertEquals(listOf(false), results)
        assertEquals('S', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
        assertTrue(fixture.access.signals.first { it.signal == CONT }.time <= 8100)
        assertTrue(fixture.access.nowMillis() < 20000)
        assertTrue(fixture.access.signals.count { it.signal == STOP } < 600)
        assertEquals(1, fixture.audioResumes)
    }

    @Test fun unreadableThreadInventoryIsNotReportedAsGoneOrFrozen() {
        val fixture = Fixture(17)
        fixture.access.failTaskReads = true
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertEquals(listOf(false), results)
        assertEquals('S', fixture.access.root.state)
        assertEquals(1, fixture.audioResumes)
        fixture.access.failTaskReads = false
        val resumed = mutableListOf<Boolean>()
        fixture.controller.resume { resumed += it }
        assertEquals(listOf(true), resumed)
    }

    @Test fun omittedKnownProcessWithUnreadableTasksCannotCountAsGone() {
        val fixture = Fixture(17)
        var omitted = false
        fixture.access.afterSleep = {
            if (!omitted && fixture.access.root.state == 'T') {
                omitted = true
                fixture.access.omitGameFromInventory = true
                fixture.access.failTaskReads = true
            }
        }
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertTrue(omitted)
        assertEquals(listOf(false), results)
        assertEquals('S', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
        assertEquals(1, fixture.audioResumes)
    }

    @Test fun signalFailureStillThawsKnownIdentities() {
        val fixture = Fixture(17)
        fixture.access.failGameStops = true
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertEquals(listOf(false), results)
        assertEquals('S', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
        assertTrue(fixture.access.signals.any { it.pid == ROOT && it.signal == CONT })
        assertTrue(fixture.access.signals.any { it.pid == GAME && it.signal == CONT })
        assertEquals(1, fixture.audioResumes)
    }

    @Test fun unexpectedBackendAndLoggerErrorsStillCompleteAndPreserveRecovery() {
        val fixture = Fixture(17)
        var failed = false
        fixture.access.afterSleep = {
            if (!failed && fixture.access.root.state == 'T') {
                failed = true
                fixture.access.failTaskRuntimeReads = true
                fixture.access.reportThrows = true
            }
        }
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertTrue(failed)
        assertEquals(listOf(false), results)
        assertEquals(1, fixture.audioResumes)
        fixture.access.failTaskRuntimeReads = false
        fixture.access.reportThrows = false
        val resumed = mutableListOf<Boolean>()
        fixture.controller.resume { resumed += it }
        assertEquals(listOf(true), resumed)
        assertEquals('S', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
    }

    @Test fun failedResumeRetainsIdentitiesForAnotherAttempt() {
        val fixture = Fixture(17)
        fixture.controller.freeze { assertTrue(it) }
        fixture.access.blockGameContinue = true
        val first = mutableListOf<Boolean>()

        fixture.controller.resume { first += it }

        assertEquals(listOf(false), first)
        assertTrue(fixture.access.game.threads.all { it.state == 't' && it.stopSignal == STOP })
        fixture.access.blockGameContinue = false
        val second = mutableListOf<Boolean>()
        fixture.controller.resume { second += it }
        assertEquals(listOf(true), second)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
    }

    @Test fun refreezeAfterFailedResumeSuspendsAudioAgainBeforeSuccess() {
        val fixture = Fixture(17)
        fixture.controller.freeze { assertTrue(it) }
        fixture.access.blockGameContinue = true
        fixture.controller.resume { assertFalse(it) }
        assertEquals(1, fixture.audioSuspends)
        assertEquals(1, fixture.audioResumes)
        fixture.access.blockGameContinue = false
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze {
            assertEquals(2, fixture.audioSuspends)
            results += it
        }

        assertEquals(listOf(true), results)
        assertEquals(1, fixture.audioResumes)
        assertEquals('T', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 't' })
    }

    @Test fun refreezeAfterFailedRollbackSuspendsAudioAgainBeforeSuccess() {
        val fixture = Fixture(17)
        fixture.access.neverStopTid = fixture.access.game.threads.last().tid
        fixture.access.blockGameContinue = true
        fixture.controller.freeze { assertFalse(it) }
        assertEquals(1, fixture.audioSuspends)
        assertEquals(1, fixture.audioResumes)
        fixture.access.neverStopTid = null
        fixture.access.blockGameContinue = false
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze {
            assertEquals(2, fixture.audioSuspends)
            results += it
        }

        assertEquals(listOf(true), results)
        assertEquals(1, fixture.audioResumes)
        assertEquals('T', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 't' })
    }

    @Test fun rejectedExecutorDoesNotFreezeOnTheCallerThread() {
        val executor = QueueExecutor().apply { shutdown() }
        val fixture = Fixture(17, executor)
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertEquals(listOf(false), results)
        assertTrue(fixture.access.signals.isEmpty())
        assertEquals(0, fixture.audioSuspends)
    }

    @Test fun closeDuringFreezeCancelsAndThawsBeforeItsCompletion() {
        val executor = QueueExecutor()
        val fixture = Fixture(68, executor)
        var requestedClose = false
        var closed = false
        val frozen = mutableListOf<Boolean>()
        fixture.access.afterSleep = {
            if (!requestedClose && fixture.access.root.state == 'T') {
                requestedClose = true
                fixture.rootPid = -1
                fixture.controller.closeAndResume { closed = true }
            }
        }
        fixture.controller.freeze { frozen += it }

        executor.drain()

        assertTrue(requestedClose)
        assertEquals(listOf(false), frozen)
        assertTrue(closed)
        assertEquals('S', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
        assertTrue(executor.isShutdown)
    }

    @Test fun resumeRequestedDuringFreezeCancelsBeforePublishingSuccess() {
        val executor = QueueExecutor()
        val fixture = Fixture(68, executor)
        var requestedResume = false
        val callbacks = mutableListOf<String>()
        fixture.access.afterSleep = {
            if (!requestedResume && fixture.access.root.state == 'T') {
                requestedResume = true
                fixture.controller.resume { callbacks += "resume:$it" }
            }
        }
        fixture.controller.freeze { callbacks += "freeze:$it" }

        executor.drain()

        assertTrue(requestedResume)
        assertEquals(listOf("freeze:false", "resume:true"), callbacks)
        assertEquals('S', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
        assertTrue(fixture.access.nowMillis() < 3000)
    }

    @Test fun disappearingOriginalRootCannotProduceSuccessfulFreeze() {
        val fixture = Fixture(17)
        var disappeared = false
        var signalCountAtExit = 0
        fixture.access.afterSleep = {
            if (!disappeared && fixture.access.root.state == 'T') {
                disappeared = true
                signalCountAtExit = fixture.access.signals.size
                fixture.access.rootGone = true
            }
        }
        val results = mutableListOf<Boolean>()

        fixture.controller.freeze { results += it }

        assertTrue(disappeared)
        assertEquals(listOf(false), results)
        assertFalse(fixture.access.signals.drop(signalCountAtExit).any { it.pid == ROOT })
        assertEquals(1, fixture.audioResumes)
    }

    @Test fun closeRestoresSavedIdentitiesAfterServiceClearsSessionRoot() {
        val fixture = Fixture(17)
        fixture.controller.freeze { assertTrue(it) }
        fixture.rootPid = -1
        var closed = false

        fixture.controller.closeAndResume { closed = true }

        assertTrue(closed)
        assertEquals('S', fixture.access.root.state)
        assertTrue(fixture.access.game.threads.all { it.state == 'S' })
    }

    @Test fun reusedPidOutsideTheGuestTreeMustNotReceiveContinue() {
        val fixture = Fixture(17)
        fixture.controller.freeze { assertTrue(it) }
        fixture.access.replaceGameWithUnrelatedProcess()
        val signalStart = fixture.access.signals.size
        val resumed = mutableListOf<Boolean>()

        fixture.controller.resume { resumed += it }

        assertEquals(listOf(true), resumed)
        assertFalse(fixture.access.signals.drop(signalStart).any { it.pid == GAME })
    }

    @Test fun ordinaryPtraceEventsCanResumeButLaterStopResetsConfirmation() {
        val fixture = Fixture(17)
        fixture.access.normalTracingOnResume = true
        fixture.access.injectRestopAfterResume = true
        fixture.controller.freeze { assertTrue(it) }
        val resumed = mutableListOf<Boolean>()

        fixture.controller.resume { resumed += it }

        assertEquals(listOf(true), resumed)
        assertTrue(fixture.access.restopInjected)
        assertTrue(fixture.access.nowMillis() - fixture.access.restopAt >= 400)
        assertTrue(fixture.access.game.threads.all { it.state == 't' && it.stopSignal == 133 })
        assertTrue(fixture.access.game.threads.all { it.cpu > 0 })
    }

    @Test fun otherUidsAndTheLauncherAreNeverSignalled() {
        val fixture = Fixture(17)
        fixture.access.extraProcesses += MutableGroup(500, ROOT, 1000, 0, 5000,
            mutableListOf(MutableThread(500, 5000, 'S')))
        fixture.controller.freeze { assertTrue(it) }
        fixture.controller.resume { assertTrue(it) }

        assertTrue(fixture.access.signals.all { it.pid == ROOT || it.pid == GAME })
    }

    private class Fixture(threadCount: Int, executor: AbstractExecutorService = InlineExecutor()) {
        val access = FakeAccess(threadCount)
        var rootPid = ROOT
        var audioSuspends = 0
        var audioResumes = 0
        val controller = SessionSuspendController(
            { rootPid }, { emptyList() }, { audioSuspends++ }, { audioResumes++ }, access, executor,
        )
    }

    private data class MutableThread(
        val tid: Int,
        val started: Long,
        var state: Char,
        var cpu: Long = 0,
        var stopSignal: Int = 0,
    )

    private data class MutableGroup(
        val pid: Int,
        var parent: Int,
        val uid: Int,
        var tracer: Int,
        var started: Long,
        val threads: MutableList<MutableThread>,
    ) {
        val state: Char get() = threads.first().state
        fun snapshot(includeThreads: Boolean) = SessionProcessInfo(
            pid, parent, started, uid, tracer, state,
            if (includeThreads) threads.map {
                SessionThreadInfo(it.tid, it.started, it.state, it.cpu, it.stopSignal)
            } else emptyList(),
        )
    }

    private data class SentSignal(val pid: Int, val signal: Int, val time: Long)

    private class FakeAccess(threadCount: Int) : SessionProcessAccess {
        override val myPid = 9000
        override val myUid = 10119
        val root = MutableGroup(ROOT, myPid, myUid, 0, 1000,
            mutableListOf(MutableThread(ROOT, 1000, 'S')))
        val game = MutableGroup(GAME, ROOT, myUid, ROOT, 2000,
            MutableList(threadCount) { MutableThread(GAME + it, 2000L + it, 'S') })
        val extraProcesses = mutableListOf<MutableGroup>()
        val signals = mutableListOf<SentSignal>()
        private var time = 0L
        private var resumeStarted: Long? = null
        var normalTracingOnResume = false
        var injectRestopAfterResume = false
        var restopInjected = false
        var restopAt = 0L
        var neverStopTid: Int? = null
        var failTaskReads = false
        var failTaskRuntimeReads = false
        var reportThrows = false
        var omitGameFromInventory = false
        var rootGone = false
        var failGameStops = false
        var blockGameContinue = false
        var afterSleep: (() -> Unit)? = null

        override fun nowMillis() = time
        override fun sleep(millis: Long) {
            time += millis
            if (root.state != 'T') {
                for (thread in game.threads) {
                    if (resumeStarted == null) {
                        thread.state = 'S'
                        thread.stopSignal = 0
                        thread.cpu++
                    } else if (normalTracingOnResume && thread.stopSignal != STOP) {
                        thread.state = 't'
                        thread.stopSignal = 133
                        thread.cpu++
                    }
                }
                val started = resumeStarted
                if (started != null && injectRestopAfterResume && !restopInjected && time - started >= 150) {
                    restopInjected = true
                    restopAt = time
                    game.threads.last().apply { state = 't'; stopSignal = STOP }
                }
            }
            afterSleep?.invoke()
        }

        override fun processes() = (listOf(root, game) + extraProcesses)
            .filterNot { omitGameFromInventory && it.pid == GAME || rootGone && it.pid == ROOT }
            .map { it.snapshot(false) }
        override fun read(pid: Int, includeThreads: Boolean): SessionProcessInfo? {
            if (rootGone && pid == ROOT) return null
            val group = (listOf(root, game) + extraProcesses).firstOrNull { it.pid == pid } ?: return null
            if (includeThreads && pid == GAME && failTaskRuntimeReads) error("unexpected backend failure")
            if (includeThreads && pid == GAME && failTaskReads) throw IOException("task inventory unavailable")
            return group.snapshot(includeThreads)
        }

        override fun signal(pid: Int, signal: Int) {
            signals += SentSignal(pid, signal, time)
            val group = (listOf(root, game) + extraProcesses).first { it.pid == pid }
            if (signal == STOP) {
                if (pid == GAME && failGameStops) throw IOException("signal refused")
                resumeStarted = null
                if (pid == ROOT) {
                    group.threads.forEach { it.state = 'T'; it.stopSignal = STOP }
                } else {
                    // A parked PRoot leaves each delivered group signal on another runnable sibling.
                    group.threads.firstOrNull { it.state != 'T' && it.state != 't' && it.tid != neverStopTid }
                        ?.apply { state = 't'; stopSignal = STOP }
                }
            } else if (signal == CONT) {
                if (pid == GAME && blockGameContinue) return
                if (pid == ROOT && resumeStarted == null) resumeStarted = time
                group.threads.forEach {
                    it.state = if (pid == GAME && normalTracingOnResume) 't' else 'S'
                    it.stopSignal = if (pid == GAME && normalTracingOnResume) 133 else 0
                }
            }
        }

        fun replaceGameWithUnrelatedProcess() {
            game.started++
            game.parent = 1
            game.tracer = 0
            game.threads.forEach { it.state = 'S'; it.stopSignal = 0 }
        }

        override fun report(message: String) {
            if (reportThrows) error("diagnostic logger failed")
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
