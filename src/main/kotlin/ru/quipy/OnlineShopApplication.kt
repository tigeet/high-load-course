package ru.quipy

import org.eclipse.jetty.http2.server.HTTP2CServerConnectionFactory
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.boot.web.embedded.jetty.JettyServerCustomizer
import org.springframework.boot.web.embedded.jetty.JettyServletWebServerFactory
import org.springframework.boot.web.embedded.tomcat.TomcatConnectorCustomizer
import org.springframework.context.annotation.Bean
import ru.quipy.common.utils.NamedThreadFactory
import java.util.concurrent.Executors


@SpringBootApplication
class OnlineShopApplication {
    val log: Logger = LoggerFactory.getLogger(OnlineShopApplication::class.java)

    companion object {
        val appExecutor = Executors.newFixedThreadPool(2048, NamedThreadFactory("main-app-executor"))
    }

    @Bean
    fun jettyServerCustomizer(): JettyServletWebServerFactory {
        val jettyServletWebServerFactory = JettyServletWebServerFactory()

        val c = JettyServerCustomizer {
            (it.connectors[0].getConnectionFactory("h2c") as HTTP2CServerConnectionFactory).maxConcurrentStreams = 10_000_000
        }

        jettyServletWebServerFactory.serverCustomizers.add(c)
        return jettyServletWebServerFactory
    }

//    @Bean
//    fun tomcatConnectorCustomizer(): TomcatConnectorCustomizer {
//        return TomcatConnectorCustomizer {
//            try {
//                (it.protocolHandler.findUpgradeProtocols().get(0) as Http2Protocol).maxConcurrentStreams = 10_000_000
//            } catch (e: Exception) { }
//        }
//    }
}

fun main(args: Array<String>) {
    runApplication<OnlineShopApplication>(*args)
}

