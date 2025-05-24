package com.juul.kable

import com.juul.kable.State.Connected
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.cancel

// Mock for Observation.Handler
class MockObservationHandler : Observation.Handler {
    var startCalled = 0
    var stopCalled = 0
    var lastCharacteristicStarted: Characteristic? = null
    var lastCharacteristicStopped: Characteristic? = null

    override suspend fun startObservation(characteristic: Characteristic) {
        startCalled++
        lastCharacteristicStarted = characteristic
    }

    override suspend fun stopObservation(characteristic: Characteristic) {
        stopCalled++
        lastCharacteristicStopped = characteristic
    }

    fun reset() {
        startCalled = 0
        stopCalled = 0
        lastCharacteristicStarted = null
        lastCharacteristicStopped = null
    }
}

// Mock for Logging
class MockLogger : Logging {
    val messages = mutableListOf<String>()
    override fun verbose(message: String) {
        messages.add("VERBOSE: $message")
    }

    override fun debug(message: String) {
        messages.add("DEBUG: $message")
    }

    override fun info(message: String) {
        messages.add("INFO: $message")
    }

    override fun warn(message: String) {
        messages.add("WARN: $message")
    }

    override fun error(message: String) {
        messages.add("ERROR: $message")
    }

    override fun error(throwable: Throwable, message: String) {
        messages.add("ERROR: $message, Exception: ${throwable.message}")
    }

    fun reset() {
        messages.clear()
    }
}

import com.juul.kable.State.Connected
import com.juul.kable.State.Connecting
import com.juul.kable.State.Disconnected
import com.juul.kable.logs.LogEngine
import com.juul.kable.logs.Logging
import com.juul.kable.logs.Logging.Format.Compact
import com.juul.kable.logs.Logging.Level.Data
import com.juul.khronicle.ConsoleLogger
import com.juul.khronicle.Log
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.test.runTest
import kotlin.test.AfterTest
import kotlin.test.BeforeTest
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.uuid.Uuid

private fun generateCharacteristic() = characteristicOf(
    service = Uuid.random(),
    characteristic = Uuid.random(),
)

@OptIn(ExperimentalCoroutinesApi::class)
class ObservationTest {

    private val logging = Logging().apply {
        level = Data
    }

    @BeforeTest
    fun setup() {
        Log.dispatcher.install(ConsoleLogger)
    }

    @AfterTest
    fun tearDown() {
        Log.dispatcher.clear()
    }

    @Test
    fun manySubscribers_startsObservationOnce() = runTest {
        val state = MutableStateFlow<State>(Connected(this))
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")

        repeat(10) {
            observation.onSubscription { }
        }
        counter.assert(
            startCount = 1,
            stopCount = 0,
        )
    }

    @Test
    fun subscribersGoesToZero_stopsObservationOnce() = runTest {
        val state = MutableStateFlow<State>(Disconnected())
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")
        val onSubscriptionActions = List(10) { suspend { } }

        state.value = Connected(this)
        onSubscriptionActions.forEach { action ->
            observation.onSubscription(action)
        }
        counter.assert(
            startCount = 1,
            stopCount = 0,
        )

        onSubscriptionActions.forEach { action ->
            observation.onCompletion(action)
        }
        counter.assert(
            startCount = 1,
            stopCount = 1,
        )
    }

    @Test
    fun subscribersGoesToZero_whileDisconnected_doesNotStopObservation() = runTest {
        val state = MutableStateFlow<State>(Connected(this))
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")
        val onSubscriptionActions = List(10) { suspend { } }

        onSubscriptionActions.forEach { action ->
            observation.onSubscription(action)
        }
        onSubscriptionActions.take(5).forEach { action ->
            observation.onCompletion(action)
        }

        state.value = Disconnected()

        onSubscriptionActions.drop(5).forEach { action ->
            observation.onCompletion(action)
        }
        counter.assert(
            startCount = 1,
            stopCount = 0,
        )
    }

    @Test
    fun hasSubscribers_reconnects_reObservesOnce() = runTest {
        val state = MutableStateFlow<State>(Connected(this))
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")

        repeat(10) {
            observation.onSubscription { }
        }
        counter.assert(
            startCount = 1,
            stopCount = 0,
        )

        // Simulate reconnect.
        state.value = Connecting.Observes
        observation.onConnected()
        counter.assert(
            startCount = 2,
            stopCount = 0,
        )
    }

    @Test
    fun addingSubscribersDuringConnect_startsObserveOnce() = runTest {
        val state = MutableStateFlow<State>(Disconnected())
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")

        repeat(5) {
            observation.onSubscription { }
        }
        counter.assert(
            startCount = 0,
            stopCount = 0,
        )

        state.value = Connecting.Observes
        observation.onConnected()
        counter.assert(
            startCount = 1,
            stopCount = 0,
        )

        // Simulate additional subscribers before state has been updated to `Connected`.
        repeat(5) {
            observation.onSubscription { }
        }
        state.value = Connected(this)
        repeat(5) {
            observation.onSubscription { }
        }

        counter.assert(
            startCount = 1,
            stopCount = 0,
        )
    }

    @Test
    fun noSubscribers_onConnected_doesNotStartObservation() = runTest {
        val state = MutableStateFlow<State>(Disconnected())
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")

        state.value = Connecting.Observes
        repeat(10) {
            observation.onConnected()
        }

        counter.assert(
            startCount = 0,
            stopCount = 0,
        )
    }

    @Test
    fun notConnected_attemptToStartObservation_actionIsNotExecuted() = runTest {
        val state = MutableStateFlow<State>(Disconnected())
        val handler = object : Observation.Handler {
            override suspend fun startObservation(characteristic: Characteristic) =
                throw NotConnectedException()

            override suspend fun stopObservation(characteristic: Characteristic) {}
        }
        val characteristic = generateCharacteristic()
        val logEngine = RecordingLogEngine()
        val logging = Logging().apply {
            level = Data
            format = Compact
            engine = logEngine
        }
        val identifier = "test"
        val observation = Observation(state, handler, characteristic, logging, identifier)

        state.value = Connecting.Observes
        var didExecuteAction = false
        observation.onSubscription {
            didExecuteAction = true
        }
        assertFalse(didExecuteAction)

        assertEquals(
            expected = listOf(
                RecordingLogEngine.Record.Verbose(
                    throwable = null,
                    tag = "Kable/Observation",
                    message = "$identifier Suppressed failure: ${NotConnectedException()}",
                ),
            ),
            actual = logEngine.records.toList(),
        )
    }

    @Test
    fun onConnectedWithSubscriber_multipleTimes_startsObservationMultipleTimes() = runTest {
        val state = MutableStateFlow<State>(Disconnected())
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")

        observation.onSubscription { }
        counter.assert(
            startCount = 0,
            stopCount = 0,
        )

        // Simulate numerous reconnects.
        state.value = Connecting.Observes
        repeat(10) {
            observation.onConnected()
        }

        counter.assert(
            startCount = 10,
            stopCount = 0,
        )
    }

    @Test
    fun connectionDropsWhileConnecting_doesNotThrow() = runTest {
        val state = MutableStateFlow<State>(Disconnected())
        val characteristic = generateCharacteristic()
        val handler = object : Observation.Handler {
            override suspend fun startObservation(characteristic: Characteristic) =
                throw NotConnectedException()

            override suspend fun stopObservation(characteristic: Characteristic) =
                throw NotConnectedException()
        }
        val logEngine = RecordingLogEngine()
        val logging = Logging().apply {
            level = Data
            format = Compact
            engine = logEngine
        }
        val identifier = "test"
        val observation = Observation(state, handler, characteristic, logging, identifier)

        observation.onSubscription { }
        state.value = Connecting.Observes
        observation.onConnected()

        assertEquals(
            expected = listOf(
                RecordingLogEngine.Record.Verbose(
                    throwable = null,
                    tag = "Kable/Observation",
                    message = "$identifier Suppressed failure: ${NotConnectedException()}",
                ),
            ),
            actual = logEngine.records.toList(),
        )
    }

    @Test
    fun failureDuringStartObservation_propagates() = runTest {
        val state = MutableStateFlow<State>(Disconnected())
        val characteristic = generateCharacteristic()
        val handler = object : Observation.Handler {
            override suspend fun startObservation(characteristic: Characteristic) = error("start")
            override suspend fun stopObservation(characteristic: Characteristic) {}
        }
        val observation = Observation(state, handler, characteristic, logging, identifier = "test")

        observation.onSubscription { }
        state.value = Connecting.Observes
        val failure = assertFailsWith<IllegalStateException> {
            observation.onConnected()
        }
        assertEquals(
            expected = "start",
            actual = failure.message,
        )
    }

    @Test
    fun failureDuringStopObservation_propagates() = runTest {
        val state = MutableStateFlow<State>(Connected(this))
        val characteristic = generateCharacteristic()
        val handler = object : Observation.Handler {
            override suspend fun startObservation(characteristic: Characteristic) {}
            override suspend fun stopObservation(characteristic: Characteristic) = error("stop")
        }
        val observation = Observation(state, handler, characteristic, logging, identifier = "test")

        val onSubscriptionAction = suspend { }
        observation.onSubscription(onSubscriptionAction)
        val failure = assertFailsWith<IllegalStateException> {
            observation.onCompletion(onSubscriptionAction)
        }
        assertEquals(
            expected = "stop",
            actual = failure.message,
        )
    }

    @Test
    fun failureInSubscriptionAction_propagates() = runTest {
        val state = MutableStateFlow<State>(Connected(this))
        val characteristic = generateCharacteristic()
        val counter = ObservationCounter(characteristic)
        val observation = Observation(state, counter, characteristic, logging, identifier = "test")

        val onSubscriptionAction = suspend { error("action") }
        val failure = assertFailsWith<IllegalStateException> {
            observation.onSubscription(onSubscriptionAction)
        }
        assertEquals(
            expected = "action",
            actual = failure.message,
        )
    }

    @Test
    fun `new subscriber, peripheral disconnected, action and startObservation not called`() = runTest {
        val characteristic = generateCharacteristic()
        val mockHandler = MockObservationHandler()
        val mockLogger = MockLogger()
        val stateFlow = MutableStateFlow<State>(Disconnected())

        val observation = Observation(
            state = stateFlow,
            handler = mockHandler,
            characteristic = characteristic,
            logging = mockLogger,
            identifier = "test"
        )

        var onSubscriptionCalled = false
        observation.onSubscription {
            onSubscriptionCalled = true
        }

        assertFalse(onSubscriptionCalled, "onSubscription action should not be called when disconnected")
        assertEquals(0, mockHandler.startCalled, "startObservation should not be called when disconnected")
    }

    @Test
    fun `new subscriber, peripheral connected, observation not started, action and startObservation called`() = runTest {
        val characteristic = generateCharacteristic()
        val mockHandler = MockObservationHandler()
        val mockLogger = MockLogger()
        // Casting `this` (TestCoroutineScope) to CoroutineScope for the `Connected` state.
        val stateFlow = MutableStateFlow<State>(Connected(this as CoroutineScope))


        val observation = Observation(
            state = stateFlow,
            handler = mockHandler,
            characteristic = characteristic,
            logging = mockLogger,
            identifier = "test"
        )

        var onSubscriptionCalled = false
        observation.onSubscription {
            onSubscriptionCalled = true
        }

        assertTrue(onSubscriptionCalled, "onSubscription action should be called when connected and observation not started")
        assertEquals(1, mockHandler.startCalled, "startObservation should be called when connected and observation not started")
    }

    @Test
    fun `new subscriber, peripheral connected, observation already started, action called, startObservation not called again`() = runTest {
        val characteristic = generateCharacteristic()
        val mockHandler = MockObservationHandler()
        val mockLogger = MockLogger()
        val stateFlow = MutableStateFlow<State>(Connected(this as CoroutineScope))

        val observation = Observation(
            state = stateFlow,
            handler = mockHandler,
            characteristic = characteristic,
            logging = mockLogger,
            identifier = "test"
        )

        // First subscriber
        var firstOnSubscriptionCalled = false
        observation.onSubscription {
            firstOnSubscriptionCalled = true
        }

        assertTrue(firstOnSubscriptionCalled, "First onSubscription action should be called")
        assertEquals(1, mockHandler.startCalled, "startObservation should be called for the first subscriber")

        // Second subscriber
        var secondOnSubscriptionCalled = false
        observation.onSubscription {
            secondOnSubscriptionCalled = true
        }

        assertTrue(secondOnSubscriptionCalled, "Second onSubscription action should be called")
        assertEquals(1, mockHandler.startCalled, "startObservation should not be called again for the second subscriber")
    }

    @Test
    fun `onConnected behavior, actions called, startObservation called`() = runTest {
        val characteristic = generateCharacteristic()
        val mockHandler = MockObservationHandler()
        val mockLogger = MockLogger()
        val stateFlow = MutableStateFlow<State>(Disconnected())

        val observation = Observation(
            state = stateFlow,
            handler = mockHandler,
            characteristic = characteristic,
            logging = mockLogger,
            identifier = "test"
        )

        var firstOnSubscriptionCalled = false
        observation.onSubscription {
            firstOnSubscriptionCalled = true
        }

        var secondOnSubscriptionCalled = false
        observation.onSubscription {
            secondOnSubscriptionCalled = true
        }

        assertFalse(firstOnSubscriptionCalled, "First onSubscription action should not be called before connected")
        assertFalse(secondOnSubscriptionCalled, "Second onSubscription action should not be called before connected")
        assertEquals(0, mockHandler.startCalled, "startObservation should not be called before connected")

        // Transition to connected
        stateFlow.value = Connected(this as CoroutineScope)
        observation.onConnected() // Manually trigger as state change might not be enough depending on Observation internal logic

        assertTrue(firstOnSubscriptionCalled, "First onSubscription action should be called after connected")
        assertTrue(secondOnSubscriptionCalled, "Second onSubscription action should be called after connected")
        assertEquals(1, mockHandler.startCalled, "startObservation should be called after connected")
    }

    @Test
    fun `onCompletion behavior, stopObservation called correctly`() = runTest {
        val characteristic = generateCharacteristic()
        val mockHandler = MockObservationHandler()
        val mockLogger = MockLogger()
        val stateFlow = MutableStateFlow<State>(Connected(this as CoroutineScope))

        val observation = Observation(
            state = stateFlow,
            handler = mockHandler,
            characteristic = characteristic,
            logging = mockLogger,
            identifier = "test"
        )

        // Scenario 1: One subscriber, cancels, stopObservation called
        val job1 = launch {
            observation.onSubscription { }
        }
        assertEquals(1, mockHandler.startCalled, "startObservation should be called for the first subscriber")
        assertEquals(0, mockHandler.stopCalled, "stopObservation should not be called yet")

        job1.cancel()
        observation.onCompletion { } // Simulate the onCompletion call that would happen in a real flow
        assertEquals(1, mockHandler.stopCalled, "stopObservation should be called after the only subscriber cancels")

        // Reset for Scenario 2
        mockHandler.reset()
        var onSub1Called = false
        var onSub2Called = false
        val sub1Action = suspend { onSub1Called = true }
        val sub2Action = suspend { onSub2Called = true }


        // Scenario 2: Two subscribers, first cancels, stopObservation not called. Second cancels, stopObservation called.
        val job2 = launch { observation.onSubscription(sub1Action) }
        val job3 = launch { observation.onSubscription(sub2Action) }


        assertEquals(1, mockHandler.startCalled, "startObservation should be called once for two subscribers")
        assertEquals(0, mockHandler.stopCalled, "stopObservation should not be called yet")

        job2.cancel()
        observation.onCompletion(sub1Action) // Simulate onCompletion for subscriber 1
        assertEquals(0, mockHandler.stopCalled, "stopObservation should not be called after only one of two subscribers cancels")

        job3.cancel()
        observation.onCompletion(sub2Action) // Simulate onCompletion for subscriber 2
        assertEquals(1, mockHandler.stopCalled, "stopObservation should be called after both subscribers cancel")
    }
}

private class ObservationCounter(
    private val characteristic: Characteristic,
) : Observation.Handler {

    var startCount = 0
    var stopCount = 0

    override suspend fun startObservation(characteristic: Characteristic) {
        if (this.characteristic == characteristic) startCount++
    }

    override suspend fun stopObservation(characteristic: Characteristic) {
        if (this.characteristic == characteristic) stopCount++
    }

    fun assert(
        startCount: Int,
        stopCount: Int,
    ) {
        assertEquals(
            expected = startCount,
            actual = this.startCount,
            message = "Start observation count",
        )
        assertEquals(
            expected = stopCount,
            actual = this.stopCount,
            message = "Stop observation count",
        )
    }
}

private class RecordingLogEngine : LogEngine {

    val records = mutableListOf<Record>()

    sealed class Record {
        abstract val throwable: Throwable?
        abstract val tag: String
        abstract val message: String

        data class Verbose(
            override val throwable: Throwable?,
            override val tag: String,
            override val message: String,
        ) : Record()

        data class Debug(
            override val throwable: Throwable?,
            override val tag: String,
            override val message: String,
        ) : Record()

        data class Info(
            override val throwable: Throwable?,
            override val tag: String,
            override val message: String,
        ) : Record()

        data class Warn(
            override val throwable: Throwable?,
            override val tag: String,
            override val message: String,
        ) : Record()

        data class Error(
            override val throwable: Throwable?,
            override val tag: String,
            override val message: String,
        ) : Record()

        data class Assert(
            override val throwable: Throwable?,
            override val tag: String,
            override val message: String,
        ) : Record()
    }

    override fun verbose(throwable: Throwable?, tag: String, message: String) {
        records += Record.Verbose(throwable, tag, message)
    }

    override fun debug(throwable: Throwable?, tag: String, message: String) {
        records += Record.Debug(throwable, tag, message)
    }

    override fun info(throwable: Throwable?, tag: String, message: String) {
        records += Record.Info(throwable, tag, message)
    }

    override fun warn(throwable: Throwable?, tag: String, message: String) {
        records += Record.Warn(throwable, tag, message)
    }

    override fun error(throwable: Throwable?, tag: String, message: String) {
        records += Record.Error(throwable, tag, message)
    }

    override fun assert(throwable: Throwable?, tag: String, message: String) {
        records += Record.Assert(throwable, tag, message)
    }
}
