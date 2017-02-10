/*
 * Copyright 2013-2016 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.springframework.cloud.gateway.filter;

import java.lang.invoke.MethodHandles;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.cloud.sleuth.Span;
import org.springframework.cloud.sleuth.SpanReporter;
import org.springframework.cloud.sleuth.SpanTextMap;
import org.springframework.cloud.sleuth.TraceKeys;
import org.springframework.cloud.sleuth.Tracer;
import org.springframework.cloud.sleuth.instrument.web.HttpSpanExtractor;
import org.springframework.cloud.sleuth.instrument.web.HttpTraceKeysInjector;
import org.springframework.cloud.sleuth.instrument.web.SleuthWebProperties;
import org.springframework.cloud.sleuth.instrument.web.TraceRequestAttributes;
import org.springframework.cloud.sleuth.instrument.web.TraceWebAutoConfiguration;
import org.springframework.cloud.sleuth.sampler.AlwaysSampler;
import org.springframework.cloud.sleuth.sampler.NeverSampler;
import org.springframework.cloud.sleuth.util.ExceptionUtils;
import org.springframework.core.Ordered;
import org.springframework.http.HttpStatus;
import org.springframework.http.server.reactive.ServerHttpRequest;
import org.springframework.http.server.reactive.ServerHttpResponse;
import org.springframework.util.StringUtils;
import org.springframework.web.context.request.async.WebAsyncUtils;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilterChain;
import org.springframework.web.server.support.HttpRequestPathHelper;

import reactor.core.publisher.Mono;

/**
 * Filter that takes the value of the {@link Span#SPAN_ID_NAME} and
 * {@link Span#TRACE_ID_NAME} header from either request or response and uses them to
 * create a new span.
 *
 * <p>
 * In order to keep the size of spans manageable, this only add tags defined in
 * {@link TraceKeys}. If you need to add additional tags, such as headers subtype this and
 * override {@link #addRequestTags} or {@link #addResponseTags}.
 *
 * @author Jakub Nabrdalik, 4financeIT
 * @author Tomasz Nurkiewicz, 4financeIT
 * @author Marcin Grzejszczak
 * @author Spencer Gibb
 * @author Dave Syer
 * @since 1.0.0
 *
 * @see Tracer
 * @see TraceKeys
 * @see TraceWebAutoConfiguration#traceFilter
 */
public class TraceFilter implements GlobalFilter, Ordered {

	private static final Log log = LogFactory.getLog(MethodHandles.lookup().lookupClass());

	private static final String HTTP_COMPONENT = "http";

	protected static final int ORDER = Ordered.HIGHEST_PRECEDENCE + 5;

	private final int order = ORDER;

	protected static final String TRACE_REQUEST_ATTR = TraceFilter.class.getName()
			+ ".TRACE";

	protected static final String TRACE_ERROR_HANDLED_REQUEST_ATTR = TraceFilter.class.getName()
			+ ".ERROR_HANDLED";

	private final Tracer tracer;
	private final TraceKeys traceKeys;
	private final Pattern skipPattern;
	private final SpanReporter spanReporter;
	private final HttpSpanExtractor spanExtractor;
	private final HttpTraceKeysInjector httpTraceKeysInjector;

	private HttpRequestPathHelper requestPathHelper = new HttpRequestPathHelper();

	public TraceFilter(Tracer tracer, TraceKeys traceKeys, SpanReporter spanReporter,
					   HttpSpanExtractor spanExtractor,
					   HttpTraceKeysInjector httpTraceKeysInjector) {
		this(tracer, traceKeys, Pattern.compile(SleuthWebProperties.DEFAULT_SKIP_PATTERN), spanReporter,
				spanExtractor, httpTraceKeysInjector);
	}

	public TraceFilter(Tracer tracer, TraceKeys traceKeys, Pattern skipPattern,
					   SpanReporter spanReporter, HttpSpanExtractor spanExtractor,
					   HttpTraceKeysInjector httpTraceKeysInjector) {
		this.tracer = tracer;
		this.traceKeys = traceKeys;
		this.skipPattern = skipPattern;
		this.spanReporter = spanReporter;
		this.spanExtractor = spanExtractor;
		this.httpTraceKeysInjector = httpTraceKeysInjector;
	}

	@Override
	public int getOrder() {
		return order;
	}

	@Override
	public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {
		ServerHttpRequest request = exchange.getRequest();
		ServerHttpResponse response = exchange.getResponse();

		String uri = this.requestPathHelper.getLookupPathForRequest(exchange);

		String sampledHeaderValue = request.getHeaders().getFirst(Span.SAMPLED_NAME);
		boolean skip = this.skipPattern.matcher(uri).matches()
				|| Span.SPAN_NOT_SAMPLED.equals(sampledHeaderValue);

		Optional<Span> spanFromRequest = getSpanFromAttribute(exchange);
		spanFromRequest.ifPresent(span -> continueSpan(exchange, span));

		if (log.isDebugEnabled()) {
			log.debug("Received a request to uri [" + uri + "] that should not be sampled [" + skip + "]");
		}

		// in case of a response with exception status a exception controller will close the span
		/*if (!httpStatusSuccessful(response) && isSpanContinued(exchange)) {
			Span parentSpan = parentSpan(spanFromRequest);
			//processErrorRequest(filterChain, request, new TraceHttpServletResponse(response, parentSpan), spanFromRequest);
			return Mono.empty();
		}*/
		String name = HTTP_COMPONENT + ":" + uri;
		Throwable exception = null;
		try {
			spanFromRequest = createSpan(exchange, skip, spanFromRequest, name);
			final Span finalSpan = spanFromRequest.get();
			response.beforeCommit(() -> {
				if (log.isTraceEnabled()) {
					log.trace("Will annotate SS once the response is flushed");
				}
				annotateWithServerSendIfLogIsNotAlreadyPresent(finalSpan);
				return Mono.empty();
			});

			final Span span = spanFromRequest.get();
			return chain.filter(exchange).doOnError(throwable -> {
				//TODO move exception handling from then below
				addResponseTags(exchange.getResponse(), throwable);
			}).then(() -> {
				Span spanIfRequestNotHandled = createSpanIfRequestNotHandled(exchange, span, name, skip);
				detachOrCloseSpans(exchange, spanIfRequestNotHandled);
				return Mono.empty();
			});
		} catch (Throwable e) {
			exception = e;
			this.tracer.addTag(Span.SPAN_ERROR_TAG_NAME, ExceptionUtils.getExceptionMessage(e));
			return Mono.error(e);
		} finally {
			/*if (isAsyncStarted(request) || request.isAsyncStarted()) {
				if (log.isDebugEnabled()) {
					log.debug("The span " + spanFromRequest + " will get detached by a HandleInterceptor");
				}
				// TODO: how to deal with response annotations and async?
				return Mono.empty();
			}*/
		}
	}

	private Span parentSpan(Optional<Span> span) {
		if (!span.isPresent()) {
			return null;
		}
		if (span.get().hasSavedSpan()) {
			return span.get().getSavedSpan();
		}
		return span.get();
	}

	/*private void processErrorRequest(FilterChain filterChain, HttpServletRequest request,
			HttpServletResponse response, Span spanFromRequest)
			throws IOException, ServletException {
		if (log.isDebugEnabled()) {
			log.debug("The span " + spanFromRequest + " was already detached once and we're processing an error");
		}
		try {
			filterChain.doFilter(request, response);
		} finally {
			request.setAttribute(TRACE_ERROR_HANDLED_REQUEST_ATTR, true);
			addResponseTags(response, null);
			this.tracer.close(spanFromRequest);
		}
	}*/

	private void continueSpan(ServerWebExchange exchange, Span spanFromRequest) {
		this.tracer.continueSpan(spanFromRequest);
		exchange.getAttributes().put(TraceRequestAttributes.SPAN_CONTINUED_REQUEST_ATTR, "true");
		if (log.isDebugEnabled()) {
			log.debug("There has already been a span in the request " + spanFromRequest);
		}
	}

	// This method is a fallback in case if handler interceptors didn't catch the request.
	// In that case we are creating an artificial span so that it can be visible in Zipkin.
	private Span createSpanIfRequestNotHandled(ServerWebExchange exchange,
											   Span spanFromRequest, String name, boolean skip) {
		if (!requestHasAlreadyBeenHandled(exchange)) {
			spanFromRequest = this.tracer.createSpan(name);
			exchange.getAttributes().put(TRACE_REQUEST_ATTR, spanFromRequest);
			if (log.isDebugEnabled() && !skip) {
				log.debug("The request with uri [" + exchange.getRequest().getURI() + "] hasn't been handled by any of Sleuth's components. "
						+ "That means that most likely you're using custom HandlerMappings and didn't add Sleuth's TraceHandlerInterceptor. "
						+ "Sleuth will create a span to ensure that the graph of calls remains valid in Zipkin");
			}
		}
		return spanFromRequest;
	}

	private boolean requestHasAlreadyBeenHandled(ServerWebExchange exchange) {
		return exchange.getAttribute(TraceRequestAttributes.HANDLED_SPAN_REQUEST_ATTR).isPresent();
	}

	private void detachOrCloseSpans(ServerWebExchange exchange,
									Span spanFromRequest) {
		Span span = spanFromRequest;
		if (span != null) {
			ServerHttpResponse response = exchange.getResponse();
			addResponseTags(response, null);
			if (span.hasSavedSpan() && requestHasAlreadyBeenHandled(exchange)) {
				recordParentSpan(span.getSavedSpan());
			} else if (!requestHasAlreadyBeenHandled(exchange)) {
				span = this.tracer.close(span);
			}
			recordParentSpan(span);
			// in case of a response with exception status will close the span when exception dispatch is handled
			// checking if tracing is in progress due to async / different order of view controller processing
			if (httpStatusSuccessful(response) && this.tracer.isTracing()) {
				if (log.isDebugEnabled()) {
					log.debug("Closing the span " + span + " since the response was successful");
				}
				this.tracer.close(span);
			} else if (errorAlreadyHandled(exchange) && this.tracer.isTracing()) {
				if (log.isDebugEnabled()) {
					log.debug(
							"Won't detach the span " + span + " since error has already been handled");
				}
			} else if (this.tracer.isTracing()) {
				if (log.isDebugEnabled()) {
					log.debug("Detaching the span " + span + " since the response was unsuccessful");
				}
				this.tracer.detach(span);
			}
		}
	}

	private void recordParentSpan(Span parent) {
		if (parent == null) {
			return;
		}
		if (parent.isRemote()) {
			if (log.isDebugEnabled()) {
				log.debug("Trying to send the parent span " + parent + " to Zipkin");
			}
			parent.stop();
			// should be already done by HttpServletResponse wrappers
			annotateWithServerSendIfLogIsNotAlreadyPresent(parent);
			this.spanReporter.report(parent);
		} else {
			// should be already done by HttpServletResponse wrappers
			annotateWithServerSendIfLogIsNotAlreadyPresent(parent);
		}
	}

	private boolean httpStatusSuccessful(ServerHttpResponse response) {
		if (response.getStatusCode() != null && response.getStatusCode().value() == 0) {
			return false;
		}
		return response.getStatusCode().is2xxSuccessful() || response.getStatusCode().is3xxRedirection();
	}

	private Optional<Span> getSpanFromAttribute(ServerWebExchange exchange) {
		return exchange.getAttribute(TRACE_REQUEST_ATTR);
	}

	private boolean errorAlreadyHandled(ServerWebExchange exchange) {
		Optional<Object> errorHandled = exchange.getAttribute(TRACE_ERROR_HANDLED_REQUEST_ATTR);
		return errorHandled.map(o -> Boolean.valueOf(String.valueOf(o))).orElse(false);
	}

	private boolean isSpanContinued(ServerWebExchange exchange) {
		return getSpanFromAttribute(exchange).isPresent();
	}

	/**
	 * In order not to send unnecessary data we're not adding request tags to the server
	 * side spans. All the tags are there on the client side.
	 */
	private void addRequestTagsForParentSpan(ServerWebExchange exchange, Span spanFromRequest) {
		if (spanFromRequest.getName().contains("parent")) {
			addRequestTags(spanFromRequest, exchange);
		}
	}

	/**
	 * Creates a span and appends it as the current request's attribute
	 */
	private Optional<Span> createSpan(ServerWebExchange exchange,
							boolean skip, Optional<Span> spanFromRequest, String name) {
		if (spanFromRequest.isPresent()) {
			if (log.isDebugEnabled()) {
				log.debug("Span has already been created - continuing with the previous one");
			}
			return spanFromRequest;
		}

		ServerHttpRequest request = exchange.getRequest();
		Span span = null;
		Span parent = this.spanExtractor.joinTrace(new ImmutableTextMap(request));
		if (parent != null) {
			if (log.isDebugEnabled()) {
				log.debug("Found a parent span " + parent + " in the request");
			}
			addRequestTagsForParentSpan(exchange, parent);
			span = parent;
			this.tracer.continueSpan(span);
			if (parent.isRemote()) {
				parent.logEvent(Span.SERVER_RECV);
			}
			exchange.getAttributes().put(TRACE_REQUEST_ATTR, span);
			if (log.isDebugEnabled()) {
				log.debug("Parent span is " + parent + "");
			}
		} else {
			if (skip) {
				span = this.tracer.createSpan(name, NeverSampler.INSTANCE);
			}
			else {
				String header = request.getHeaders().getFirst(Span.SPAN_FLAGS);
				if (Span.SPAN_SAMPLED.equals(header)) {
					span = this.tracer.createSpan(name, new AlwaysSampler());
				} else {
					span = this.tracer.createSpan(name);
				}
			}
			span.logEvent(Span.SERVER_RECV);
			exchange.getAttributes().put(TRACE_REQUEST_ATTR, span);
			if (log.isDebugEnabled()) {
				log.debug("No parent span present - creating a new span");
			}
		}
		return Optional.of(span);
	}

	/** Override to add annotations not defined in {@link TraceKeys}. */
	protected void addRequestTags(Span span, ServerWebExchange exchange) {
		ServerHttpRequest request = exchange.getRequest();
		String uri = this.requestPathHelper.getLookupPathForRequest(exchange);
		this.httpTraceKeysInjector.addRequestTags(span, getFullUrl(request),
				request.getHeaders().getHost().getHostName(), uri, request.getMethod().name());
		for (String name : this.traceKeys.getHttp().getHeaders()) {
			List<String> values = request.getHeaders().get(name);
			if (!values.isEmpty()) {
				String key = this.traceKeys.getHttp().getPrefix() + name.toLowerCase();
				String value = values.size() == 1 ? values.get(0)
						: StringUtils.collectionToDelimitedString(values, ",", "'", "'");
				this.httpTraceKeysInjector.tagSpan(span, key, value);
			}
		}
	}

	/** Override to add annotations not defined in {@link TraceKeys}. */
	protected void addResponseTags(ServerHttpResponse response, Throwable e) {
		HttpStatus statusCode = response.getStatusCode();
		if (statusCode.equals(HttpStatus.OK) && e != null) {
			// Filter chain threw exception but the response status may not have been set
			// yet, so we have to guess.
			this.tracer.addTag(this.traceKeys.getHttp().getStatusCode(),
					String.valueOf(HttpServletResponse.SC_INTERNAL_SERVER_ERROR));
		}
		// only tag valid http statuses
		else if (statusCode.is1xxInformational() || statusCode.is4xxClientError() || statusCode.is5xxServerError()) {
			this.tracer.addTag(this.traceKeys.getHttp().getStatusCode(),
					String.valueOf(statusCode.value()));
		}
	}

	protected boolean isAsyncStarted(HttpServletRequest request) {
		return WebAsyncUtils.getAsyncManager(request).isConcurrentHandlingStarted();
	}

	private String getFullUrl(ServerHttpRequest request) {
		return request.getURI().toString();
		/*String queryString = request.getQueryString();
		if (queryString == null) {
			return requestURI.toString();
		} else {
			return requestURI.append('?').append(queryString).toString();
		}*/
	}

	static void annotateWithServerSendIfLogIsNotAlreadyPresent(Span span) {
		if (span == null) {
			return;
		}
		for (org.springframework.cloud.sleuth.Log log1 : span.logs()) {
			if (Span.SERVER_SEND.equals(log1.getEvent())) {
				if (log.isTraceEnabled()) {
					log.trace("Span was already annotated with SS, will not do it again");
				}
				return;
			}
		}
		if (log.isTraceEnabled()) {
			log.trace("Will set SS on the span");
		}
		span.logEvent(Span.SERVER_SEND);
	}

	class ImmutableTextMap implements SpanTextMap {
		private ServerHttpRequest request;

		public ImmutableTextMap(ServerHttpRequest request) {
			this.request = request;
		}

		@Override
		public Iterator<Map.Entry<String, String>> iterator() {
			return request.getHeaders().toSingleValueMap().entrySet().iterator();
		}

		@Override
		public void put(String key, String value) {
			throw new UnsupportedOperationException("put() hasn't been implemented");
		}
	}

}

