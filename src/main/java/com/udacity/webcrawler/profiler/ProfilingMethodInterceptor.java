package com.udacity.webcrawler.profiler;

import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.util.Objects;
/**
 * A method interceptor that checks whether {@link Method}s are annotated with the {@link Profiled}
 * annotation. If they are, the method interceptor records how long the method invocation took.
 */
final class ProfilingMethodInterceptor implements InvocationHandler {
  private final Clock clock;
  private final ZonedDateTime startTime;
  private final ProfilingState state;
  private final Object target;

  // TODO: You will need to add more instance fields and constructor arguments to this class.
  ProfilingMethodInterceptor(Clock clock,  Object target, ProfilingState state, ZonedDateTime startTime) {
    this.clock = Objects.requireNonNull(clock);
    this.state = Objects.requireNonNull(state);
    this.target = Objects.requireNonNull(target);
    this.startTime = Objects.requireNonNull(startTime); }
  // obj ? remove?
  @Override
  public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
  Object o;
  Instant start = null;
  boolean profiled = method.getAnnotation(Profiled.class) != null;
  if (profiled) {
    start = clock.instant();
  }
  try {
    o = method.invoke(this.target, args);
  } catch (InvocationTargetException invocationTargetException) {
    throw invocationTargetException.getTargetException(); }
    catch (IllegalAccessException illegalAccessException) {
    throw new RuntimeException(illegalAccessException);
    }
  finally {
    if (profiled) {
      Duration duration = Duration.between(start, clock.instant());
      state.record(this.target.getClass(), method, duration);
    }
  }
    return o;
    // TODO: This method interceptor should inspect the called method to see if it is a profiled
    //       method. For profiled methods, the interceptor should record the start time, then
    //       invoke the method using the object that is being profiled. Finally, for profiled
    //       methods, the interceptor should record how long the method call took, using the
    //       ProfilingState methods.

  }

}