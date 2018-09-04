/**
 * Copyright (C) 2015 The Gravitee team (http://gravitee.io)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.gravitee.gateway.reactor.handler.alert;

import io.gravitee.alert.api.event.Event;
import io.gravitee.gateway.api.ExecutionContext;
import io.gravitee.gateway.api.Request;
import io.gravitee.gateway.api.Response;
import io.gravitee.gateway.api.handler.Handler;
import io.gravitee.plugin.alert.AlertEngineService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Azize ELAMRANI (azize.elamrani at graviteesource.com)
 * @author GraviteeSource Team
 */
public class AlertHandler implements Handler<Response> {

    private static final Logger LOGGER = LoggerFactory.getLogger(AlertHandler.class);

    private final AlertEngineService alertEngineService;
    private final Request serverRequest;
    private final ExecutionContext executionContext;
    private final Handler<Response> next;

    public AlertHandler(final AlertEngineService alertEngineService, final Request serverRequest,
                        final ExecutionContext executionContext, final Handler<Response> next) {
        this.alertEngineService = alertEngineService;
        this.serverRequest = serverRequest;
        this.executionContext = executionContext;
        this.next = next;
    }

    @Override
    public void handle(Response result) {
        // Push result to the next handler
        next.handle(result);

        try {
            if (alertEngineService != null) {
                final Event.Builder props = new Event.Builder()
                        .timestamp(serverRequest.timestamp())
                        .type("REQUEST")
                        .prop("Request id", serverRequest.id())
                        .prop("Context path", executionContext.getAttribute(ExecutionContext.ATTR_CONTEXT_PATH))
                        .prop("API", executionContext.getAttribute(ExecutionContext.ATTR_API))
                        .prop("APPLICATION", executionContext.getAttribute(ExecutionContext.ATTR_APPLICATION))
                        .prop("Plan", executionContext.getAttribute(ExecutionContext.ATTR_PLAN))
                        .prop("Response status", result.status())
                        .prop("Response time in ms", serverRequest.metrics().getApiResponseTimeMs())
                        .prop("Latency in ms", serverRequest.metrics().getProxyLatencyMs());

                final Long quotaCount = (Long) executionContext.getAttribute(ExecutionContext.ATTR_QUOTA_COUNT);
                final Long quotaLimit = (Long) executionContext.getAttribute(ExecutionContext.ATTR_QUOTA_LIMIT);
                if (quotaCount != null && quotaLimit != null) {
                    props
                            .prop("Quota count", quotaCount)
                            .prop("Quota limit", quotaLimit)
                            .prop("Quota percent", Double.valueOf(quotaCount) / Double.valueOf(quotaLimit) * 100);
                }
                alertEngineService.send(props.build());
            }
        } catch (Exception ex) {
            LOGGER.error("An error occurs while sending alert", ex);
        }
    }
}