/*
 Licensed to Diennea S.r.l. under one
 or more contributor license agreements. See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership. Diennea S.r.l. licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at

 http://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing,
 software distributed under the License is distributed on an
 "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 KIND, either express or implied.  See the License for the
 specific language governing permissions and limitations
 under the License.

 */

package herddb.model.commands;

import herddb.model.Statement;

/**
 * Triggers a full checkpoint on all tablespaces.
 * <p>
 * The optional second SQL argument ({@code EXECUTE CHECKPOINT 'ts', timeoutSeconds})
 * is parsed for backward compatibility but no longer has any effect: the checkpoint
 * no longer blocks on external indexing-service catch-up. Use
 * {@code EXECUTE WAITFORINDEXES 'ts', timeoutSeconds} as an explicit sync barrier
 * when clients need to observe a tailer-consistent view.
 */
public class CheckpointStatement extends Statement {

    public CheckpointStatement(String tableSpace) {
        super(tableSpace);
    }
}
