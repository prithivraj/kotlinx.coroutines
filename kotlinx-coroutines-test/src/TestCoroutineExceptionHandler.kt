/*
 * Copyright 2016-2021 JetBrains s.r.o. Use of this source code is governed by the Apache 2.0 license.
 */

package kotlinx.coroutines.test

import kotlinx.coroutines.*
import kotlin.coroutines.*

/**
 * Access uncaught coroutine exceptions captured during test execution.
 */
@Deprecated("Consider whether a plain `CoroutineExceptionHandler` would suffice. If not, please report your use case at https://github.com/Kotlin/kotlinx.coroutines/issues.", level = DeprecationLevel.ERROR)
public interface UncaughtExceptionCaptor {
    /**
     * List of uncaught coroutine exceptions.
     *
     * The returned list is a copy of the currently caught exceptions.
     * During [cleanupTestCoroutines] the first element of this list is rethrown if it is not empty.
     */
    public val uncaughtExceptions: List<Throwable>

    /**
     * Call after the test completes to ensure that there were no uncaught exceptions.
     *
     * The first exception in uncaughtExceptions is rethrown. All other exceptions are
     * printed using [Throwable.printStackTrace].
     *
     * @throws Throwable the first uncaught exception, if there are any uncaught exceptions.
     */
    public fun cleanupTestCoroutines()
}

/**
 * An exception handler that captures uncaught exceptions in tests.
 *
 * In order to work as intended, this exception handler requires that the [Job] of the test coroutine scope is a
 * [SupervisorJob].
 */
// @Deprecated("In order to work correctly, this exception handler requires that " +

//     "the test coroutine scope's Job is a SupervisorJob, which may lead to tests running indefinitely " +
//     "instead of quickly crashing. Please consider specifying another `CoroutineExceptionHandler`", level = DeprecationLevel.WARNING)
public class TestCoroutineExceptionHandler :
    AbstractCoroutineContextElement(CoroutineExceptionHandler), CoroutineExceptionHandler
{
    private val _exceptions = mutableListOf<Throwable>()
    private var _coroutinesCleanedUp = false

    @Suppress("INVISIBLE_MEMBER")
    override fun handleException(context: CoroutineContext, exception: Throwable) {
        synchronized(_exceptions) {
            if (_coroutinesCleanedUp) {
                handleCoroutineExceptionImpl(context, exception)
                return
            }
            _exceptions += exception
        }
    }

    public val uncaughtExceptions: List<Throwable>
        get() = synchronized(_exceptions) { _exceptions.toList() }

    public fun cleanupTestCoroutines() {
        synchronized(_exceptions) {
            _coroutinesCleanedUp = true
            val exception = _exceptions.firstOrNull() ?: return
            // log the rest
            _exceptions.drop(1).forEach { it.printStackTrace() }
            throw exception
        }
    }
}
