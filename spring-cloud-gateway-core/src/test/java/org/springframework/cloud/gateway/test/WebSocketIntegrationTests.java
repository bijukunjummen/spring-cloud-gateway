/*
 * Copyright 2013-2017 the original author or authors.
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
 *
 */

package org.springframework.cloud.gateway.test;

import io.netty.handler.codec.http.websocketx.WebSocketFrame;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringBootConfiguration;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.is;
import static org.springframework.boot.test.context.SpringBootTest.WebEnvironment.RANDOM_PORT;

import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxProcessor;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ReplayProcessor;
import reactor.ipc.netty.NettyContext;
import reactor.ipc.netty.NettyPipeline;
import reactor.ipc.netty.http.client.HttpClient;
import reactor.ipc.netty.http.client.HttpClientRequest;
import reactor.ipc.netty.http.client.HttpClientResponse;
import reactor.ipc.netty.http.server.HttpServer;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

/**
 * @author Spencer Gibb
 */
@RunWith(SpringRunner.class)
@SpringBootTest(webEnvironment = RANDOM_PORT)
@DirtiesContext
public class WebSocketIntegrationTests {

	private static final String auth = "bearer abc";

	private NettyContext httpServer = null;

	@Autowired
	private HttpClient httpClient;

	@After
	public void disposeHttpServer() {
		if (httpServer != null)
			httpServer.dispose();
	}

	@Test
	public void webSocketsWork() {
		httpServer = HttpServer.create(0)
				.newHandler((in, out) -> out.sendWebsocket((i, o) -> o.sendString(
						Mono.just("test"))))
				.block(Duration.ofSeconds(30));

		String res = this.httpClient
				.get("ws://localhost:"+this.httpServer.address().getPort()+"/test",
						out -> out.addHeader("Authorization", auth)
								.sendWebsocket())
				.flatMap(in -> in.receive()
						.asString())
				.log()
				.collectList()
				.block(Duration.ofSeconds(30))
				.get(0);

		Assert.assertThat(res, is("test"));
	}

	@Test
	public void duplexEcho() throws Exception {

		int c = 10;
		CountDownLatch clientLatch = new CountDownLatch(c);
		CountDownLatch serverLatch = new CountDownLatch(c);

		FluxProcessor<String, String> server =
				ReplayProcessor.<String>create().serialize();
		FluxProcessor<String, String> client =
				ReplayProcessor.<String>create().serialize();
		FluxProcessor<String, String> response =
				ReplayProcessor.<String>create().serialize();

		server.log("server")
				.subscribe(s -> {
					// System.out.println("Received "+s);
					serverLatch.countDown();
				});
		client.log("client")
				.subscribe(s -> {
					// System.out.println("Sending "+s);
					clientLatch.countDown();
				});
		response.log().subscribe();

		httpServer = HttpServer.create(0)
				.newHandler((in, out) -> out.sendWebsocket((i, o) -> o.sendString(
						i.receive()
								.asString()
								.take(c)
								.subscribeWith(server))))
				.block(Duration.ofSeconds(30));

		Flux.intervalMillis(200)
				.map(num -> {
					// System.out.println(num);
					return "msg"+String.valueOf(num);
				})
				.subscribe(client::onNext);

		this.httpClient
				.ws("ws://localhost:"+this.httpServer.address().getPort()+"/test")
				.then(in -> in.receiveWebsocket((i, o) ->
						o.options(NettyPipeline.SendOptions::flushOnEach)
								.sendString(i.receive()
										.asString()
										.subscribeWith(client)))
				)
				.subscribe();

		Assert.assertTrue(serverLatch.await(10, TimeUnit.SECONDS));
		Assert.assertTrue(clientLatch.await(10, TimeUnit.SECONDS));
	}

	@EnableAutoConfiguration
	@SpringBootConfiguration
	@Import(BaseWebClientTests.DefaultTestConfig.class)
	public static class TestConfig { }
}
