package org.springframework.cloud.gateway.filter.route;

import java.net.URI;
import java.util.Iterator;
import java.util.Map;
import java.util.Optional;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanTextMap;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.instrument.web.HttpSpanInjector;
import org.springframework.cloud.sleuth.instrument.web.HttpTraceKeysInjector;
import org.springframework.cloud.sleuth.instrument.web.TraceRequestAttributes;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.support.HttpRequestPathHelper;
import reactor.core.publisher.Mono;

import static org.springframework.cloud.gateway.support.ServerWebExchangeUtils.GATEWAY_REQUEST_URL_ATTR;

/**
 * @author Spencer Gibb
 */
public class TraceRouteFilter implements RouteFilter {

	private static final Log log = LogFactory.getLog(TraceRouteFilter.class);

	private HttpRequestPathHelper requestPathHelper = new HttpRequestPathHelper();

	private final Tracer tracer;
	private final TraceKeys traceKeys;
	private final HttpSpanInjector spanInjector;
	private final HttpTraceKeysInjector httpTraceKeysInjector;

	public TraceRouteFilter(Tracer tracer, TraceKeys traceKeys, HttpSpanInjector spanInjector, HttpTraceKeysInjector httpTraceKeysInjector) {
		this.tracer = tracer;
		this.traceKeys = traceKeys;
		this.spanInjector = spanInjector;
		this.httpTraceKeysInjector = httpTraceKeysInjector;
	}

	@Override
	public WebFilter apply(String... args) {
		//validate(2, args);

		return (exchange, chain) -> {
			// Pre filter
			Span span = this.tracer.getCurrentSpan();
			if (log.isDebugEnabled()) {
				log.debug("Current span is " + span + "");
			}
			markRequestAsHandled(exchange);

			Span newSpan = this.tracer.createSpan(span.getName(), span);
			newSpan.tag(Span.SPAN_LOCAL_COMPONENT_TAG_NAME, "gateway");

			ServerHttpRequest.Builder requestBuilder = exchange.getRequest().mutate();
			this.spanInjector.inject(newSpan, new WebFluxTextMap(requestBuilder));

			Optional<URI> requestUrl = exchange.getAttribute(GATEWAY_REQUEST_URL_ATTR);
			this.httpTraceKeysInjector.addRequestTags(newSpan, requestUrl.get(), exchange.getRequest().getMethod().name());

			if (log.isDebugEnabled()) {
				log.debug("New Zuul Span is " + newSpan + "");
			}

			// pre filter run
			this.tracer.getCurrentSpan().logEvent(Span.CLIENT_SEND);

			span = this.tracer.getCurrentSpan();
			// Client Customizer
			if (span == null) {
				requestBuilder.header(Span.SAMPLED_NAME, Span.SPAN_NOT_SAMPLED);
			} else {
				requestBuilder.header(Span.SAMPLED_NAME, span.isExportable() ?
						Span.SPAN_SAMPLED : Span.SPAN_NOT_SAMPLED);
				requestBuilder.header(Span.TRACE_ID_NAME, span.traceIdString());
				requestBuilder.header(Span.SPAN_ID_NAME, Span.idToHex(span.getSpanId()));
				requestBuilder.header(Span.SPAN_NAME_NAME, span.getName());
				Long parentId = getParentId(span);
				if (parentId != null) {
					requestBuilder.header(Span.PARENT_ID_NAME, Span.idToHex(parentId));
				}
				requestBuilder.header(Span.PROCESS_ID_NAME, span.getProcessId());
			}

			span.logEvent(Span.CLIENT_SEND);
			if (log.isDebugEnabled()) {
				log.debug("Span in the after request customized is" + span);
			}

			ServerWebExchange mutatedExchange = exchange.mutate().request(requestBuilder.build()).build();
			return chain.filter(mutatedExchange).then(postFilterWork(mutatedExchange, newSpan));
		};
	}

	private Long getParentId(Span span) {
		return !span.getParents().isEmpty()
				? span.getParents().get(0) : null;
	}

	private Mono<Void> postFilterWork(ServerWebExchange exchange, Span newSpan) {
		//TODO: figure out close newSpan on error
		/*ZuulFilterResult result = super.runFilter();
		if (log.isDebugEnabled()) {
			log.debug("Result of Zuul filter is [" + result.getStatus() + "]");
		}
		if (ExecutionStatus.SUCCESS != result.getStatus()) {
			if (log.isDebugEnabled()) {
				log.debug("The result of Zuul filter execution was not successful thus "
						+ "will close the current span " + newSpan);
			}
			this.tracer.close(newSpan);
		}*/

		// Post filter
		// TODO: the client sent event should come from the client not the filter!
		Span currentSpan = this.tracer.getCurrentSpan();
		if (currentSpan != null) {
			currentSpan.logEvent(Span.CLIENT_RECV);
			if (log.isDebugEnabled()) {
				log.debug("Closing current client span " + currentSpan);
			}
			int httpStatus = 0;
			if (exchange.getResponse().getStatusCode() != null) {
				httpStatus = exchange.getResponse().getStatusCode().value();
			}
			if (httpStatus > 0) {
				this.tracer.addTag(this.traceKeys.getHttp().getStatusCode(),
						String.valueOf(httpStatus));
			}
			this.tracer.close(currentSpan);
		}

		return Mono.empty();
	}

	// TraceFilter will not create the "fallback" span
	private void markRequestAsHandled(ServerWebExchange exchange) {
		exchange.getAttributes().put(TraceRequestAttributes.HANDLED_SPAN_REQUEST_ATTR, "true");
	}

	class WebFluxTextMap implements SpanTextMap {


		private ServerHttpRequest.Builder requestBuilder;

		public WebFluxTextMap(ServerHttpRequest.Builder requestBuilder) {
			this.requestBuilder = requestBuilder;
		}

		@Override
		public Iterator<Map.Entry<String, String>> iterator() {
			throw new UnsupportedOperationException("iterator() hasn't been implemented");
			//return null;
		}

		@Override
		public void put(String name, String value) {
			this.requestBuilder.header(name, value);
		}
	}
}
