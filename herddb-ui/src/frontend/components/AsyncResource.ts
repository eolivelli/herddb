/*
 * Licensed to Diennea S.r.l. under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Diennea S.r.l. licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
import { useEffect, useState } from 'react';

export interface AsyncState<T> {
    data: T | null;
    loading: boolean;
    error: string | null;
}

/**
 * Tiny "load this once on mount" hook used by all v2 pages. Re-runs when
 * any element of `deps` changes. Cancels stale results so a slower
 * earlier request cannot overwrite a faster later one.
 */
export function useAsync<T>(
    loader: () => Promise<T>,
    deps: ReadonlyArray<unknown>,
): AsyncState<T> {
    const [state, setState] = useState<AsyncState<T>>({
        data: null,
        loading: true,
        error: null,
    });

    useEffect(() => {
        let cancelled = false;
        setState({ data: null, loading: true, error: null });
        loader()
            .then((data) => {
                if (!cancelled) {
                    setState({ data, loading: false, error: null });
                }
            })
            .catch((e) => {
                if (!cancelled) {
                    setState({
                        data: null,
                        loading: false,
                        error: e instanceof Error ? e.message : String(e),
                    });
                }
            });
        return () => {
            cancelled = true;
        };
        // The caller controls the dependency list explicitly.
        // eslint-disable-next-line react-hooks/exhaustive-deps
    }, deps);

    return state;
}
