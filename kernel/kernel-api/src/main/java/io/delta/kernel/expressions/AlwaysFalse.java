/*
 * Copyright (2023) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.delta.kernel.expressions;

import io.delta.kernel.annotation.Evolving;
import java.util.Collections;

/**
 * Predicate which always evaluates to {@code false}.
 *
 * @since 3.0.0
 */
@Evolving
public final class AlwaysFalse extends Predicate {
  public static final AlwaysFalse ALWAYS_FALSE = new AlwaysFalse();

  private AlwaysFalse() {
    super("ALWAYS_FALSE", Collections.emptyList());
  }
}
