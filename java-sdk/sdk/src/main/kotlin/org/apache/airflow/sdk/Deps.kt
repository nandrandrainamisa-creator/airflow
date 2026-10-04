/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.airflow.sdk

import org.apache.airflow.sdk.internal.Refs

/**
 * Vocabulary for declaring a Dag's task graph in Java, and the base of every
 * generated `<Dag>Deps` wiring view.
 *
 * A [Builder.Deps] class inherits [lit] and [Flow] by simple name, so it needs
 * no import and `Flow` does not collide with `java.util.concurrent.Flow`.
 */
interface Deps {
  /**
   * A point in the task graph: one task, one task group, or a set of them.
   *
   * [Flow] declares a dependency where nothing flows but the ordering. An
   * edge that carries a value is declared by passing the upstream's handle
   * instead.
   */
  interface Flow {
    /** The tasks at this point in the flow. */
    fun nodes(): List<TaskDef>

    /**
     * Runs the tasks here before each of [next], carrying no value.
     *
     * ```java
     * loaded.before(cleaned, notified); // cleanup and notify both wait for load
     * ```
     *
     * Variadic, so one call fans out, and it returns its own receiver: a
     * fan-out has no single next task to hand back. Declaring an edge that
     * already exists changes nothing.
     *
     * @param next Tasks that run after the ones here.
     * @return This point in the flow.
     */
    fun before(vararg next: Flow): Flow {
      next.forEach { link(this, it) }
      return this
    }

    /**
     * Runs the tasks here after each of [previous], carrying no value.
     *
     * ```java
     * cleaned.after(loaded, transformed); // cleanup waits for load and transform
     * ```
     *
     * @param previous Tasks that run before the ones here.
     * @return This point in the flow.
     */
    fun after(vararg previous: Flow): Flow {
      previous.forEach { link(it, this) }
      return this
    }

    companion object {
      /**
       * Treats several tasks as one point in the flow, so a single call draws
       * every edge between two sets:
       *
       * ```java
       * Flow.of(a, b).before(c, d); // c and d both wait for a and b
       * ```
       */
      @JvmStatic
      fun of(vararg flows: Flow): Flow = FlowSet(flows.toList())
    }
  }

  /**
   * Wraps an inline constant as a task argument, as in
   * `transform(extract(), lit(0.9))`. It is passed to the task as a constant
   * and creates no dependency edge.
   *
   * @param value Constant to bind; may be null for a nullable parameter.
   */
  fun <T> lit(value: T?): Arg<T> = Arg.lit(value)

  /**
   * The task group with this ID, for ordering a whole group against a task
   * or another group: `extract().before(group("staging"))`.
   *
   * A group exists once a [Builder.Task] method names it with `group`.
   *
   * @param id Full group ID, such as `staging.checks`.
   * @throws IllegalArgumentException if no task of the Dag names the group.
   */
  fun group(id: String): Flow = Refs.group(id)
}

/** Several tasks or groups as one point in the flow, which no single handle can represent. */
internal class FlowSet(
  internal val flows: List<Deps.Flow>,
) : Deps.Flow {
  override fun nodes(): List<TaskDef> = flows.flatMap { it.nodes() }
}

/**
 * Draws an ordering edge from each endpoint of [upstream] to each of
 * [downstream]. An edge between two tasks is recorded on the downstream task;
 * one with a task group at either end is recorded on the group's Dag and
 * expanded onto tasks when the Dag is registered, once the group is complete.
 */
private fun link(
  upstream: Deps.Flow,
  downstream: Deps.Flow,
) {
  for (up in upstream.endpoints()) {
    for (down in downstream.endpoints()) {
      if (up is TaskDef && down is TaskDef) {
        down.dependsOn(up)
      } else {
        val dag = (up as? TaskGroupRef ?: down as TaskGroupRef).dag
        dag.groupEdges += up to down
      }
    }
  }
}

/** The tasks and groups at this point in the flow, each a [TaskDef] or [TaskGroupRef]. */
private fun Deps.Flow.endpoints(): List<Any> =
  when (this) {
    is TaskGroupRef -> listOf(this)
    is FlowSet -> flows.flatMap { it.endpoints() }
    else -> nodes()
  }
