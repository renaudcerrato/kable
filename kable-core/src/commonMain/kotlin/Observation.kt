package com.juul.kable

import com.juul.kable.State.Connecting.Observes
import com.juul.kable.logs.Logger
import com.juul.kable.logs.Logging
import kotlinx.atomicfu.atomic
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

internal class Observation(
    private val state: StateFlow<State>,
    private val handler: Handler,
    private val characteristic: Characteristic,
    logging: Logging,
    identifier: String,
) {

    interface Handler {
        suspend fun startObservation(characteristic: Characteristic)
        suspend fun stopObservation(characteristic: Characteristic)
    }

    private val logger = Logger(logging, tag = "Kable/Observation", identifier)

    private val subscribers = mutableListOf<OnSubscriptionAction>()
    private val mutex = Mutex()

    private val _didStartObservation = atomic(false)
    private var didStartObservation: Boolean
        get() = _didStartObservation.value
        set(value) { _didStartObservation.value = value }

    private val isConnected: Boolean
        get() = state.value.isAtLeast<Observes>()

    suspend fun onSubscription(action: OnSubscriptionAction) = mutex.withLock {
        subscribers += action
    if (isConnected) { // If connected, this new subscriber's action should be called
        if (!didStartObservation) { // If observation hasn't started, start it
            suppressNotConnectedException { // Or handle potential NotConnectedException if state changes during lock
                startObservation()
                action() // Call the new subscriber's action
            }
        } else {
            // Observation already started, just call the new subscriber's action
            suppressNotConnectedException { // Or handle potential NotConnectedException
                 action()
            }
            }
        }
    // If not connected, action() will be called by onConnected() later for all subscribers.
    }

    suspend fun onCompletion(action: OnSubscriptionAction) = mutex.withLock {
        subscribers -= action
        val shouldStopObservation = didStartObservation && subscribers.isEmpty() && isConnected
        if (shouldStopObservation) stopObservation()
    }

    suspend fun onConnected() = mutex.withLock {
        if (isConnected) {
            if (subscribers.isNotEmpty()) {
                suppressNotConnectedException {
                    startObservation()
                    subscribers.forEach { it() }
                }
            } else {
                didStartObservation = false
            }
        }
    }

    private suspend fun startObservation() {
        handler.startObservation(characteristic)
        didStartObservation = true
    }

    private suspend fun stopObservation() {
        suppressNotConnectedException {
            handler.stopObservation(characteristic)
        }
        didStartObservation = false
    }

    /**
     * While spinning up or down an observation the connection may drop, resulting in an unnecessary
     * [NotConnectedException] being thrown.
     *
     * Since observations are automatically cleared (by the underlying platform) on disconnect,
     * these exceptions can be ignored, as the corresponding [action] will be rendered unnecessary
     * (clearing an observation is not needed if connection has been lost).
     */
    private inline fun suppressNotConnectedException(action: () -> Unit) {
        try {
            action.invoke()
        } catch (e: NotConnectedException) {
            logger.verbose { message = "Suppressed failure: $e" }
        }
    }
}
