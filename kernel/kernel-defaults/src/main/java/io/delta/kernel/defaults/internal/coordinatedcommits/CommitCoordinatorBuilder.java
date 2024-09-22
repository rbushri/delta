/*
 * Copyright (2024) The Delta Lake Project Authors.
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
package io.delta.kernel.defaults.internal.coordinatedcommits;

import io.delta.storage.commit.CommitCoordinatorClient;
import java.util.*;
import org.apache.hadoop.conf.Configuration;

/** A builder interface for {@link CommitCoordinatorClient}. */
public abstract class CommitCoordinatorBuilder {
  private Configuration initHadoopConf;

  public CommitCoordinatorBuilder(Configuration initHadoopConf) {
    this.initHadoopConf = initHadoopConf;
  }
  /** Name of the commit-coordinator */
  public abstract String getName();

  /** Returns a commit-coordinator client based on the given conf */
  public abstract CommitCoordinatorClient build(Map<String, String> conf);
}
