package com.indentationerror.dds;

import java.io.*;
import java.net.InetSocketAddress;
import java.lang.*;

import com.sun.net.httpserver.HttpsServer;

import java.nio.charset.Charset;
import java.nio.file.Files;
import java.security.KeyFactory;
import java.security.KeyStore;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManagerFactory;
import com.sun.net.httpserver.*;
import javax.net.ssl.SSLEngine;
import javax.net.ssl.SSLParameters;

import javax.net.ssl.SSLContext;
import java.security.SecureRandom;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;

import java.security.interfaces.RSAPrivateKey;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpsExchange;

public class APIServer {
    private DatabaseInstance databaseInstance;
    private static String DEFAULT_ROOT_SITE = "<!DOCTYPE html><html><head><title>Welcome to DDS!</title><style>body {width: 35em; margin: 0 auto; font-family: Tahoma, Verdana, Arial, sans-serif;}</style></head><body><h1>Welcome to DDS!</h1><p>If you see this page, the DDS REST API server is successfully installed and working. Further configuration is required.</p><p>For online documentation and support please refer to <a href=\"https://dds.indentationerror.com/\">dds.indentationerror.com</a></p><p><em>Thank you for using DDS.</em></p></body></html>";
    public static class RootHandler implements HttpHandler {
        @Override
        public void handle(HttpExchange t) throws IOException {
            HttpsExchange httpsExchange = (HttpsExchange) t;
            t.getResponseHeaders().add("Access-Control-Allow-Origin", "*");
            t.sendResponseHeaders(200, DEFAULT_ROOT_SITE.getBytes().length);
            OutputStream os = t.getResponseBody();
            os.write(DEFAULT_ROOT_SITE.getBytes());
            os.close();
        }
    }

    private static RSAPrivateKey readPrivateKey(File file) throws Exception {
        String key = new String(Files.readAllBytes(file.toPath()), Charset.defaultCharset());

        String privateKeyPEM = key
                .replace("-----BEGIN PRIVATE KEY-----", "")
                .replaceAll(System.lineSeparator(), "")
                .replace("-----END PRIVATE KEY-----", "");

        byte[] encoded = Base64.getDecoder().decode(privateKeyPEM);

        KeyFactory keyFactory = KeyFactory.getInstance("RSA");
        PKCS8EncodedKeySpec keySpec = new PKCS8EncodedKeySpec(encoded);
        return (RSAPrivateKey) keyFactory.generatePrivate(keySpec);
    }

    public APIServer(DatabaseInstance databaseInstance) {
        this.databaseInstance = databaseInstance;
    }
    public void start() throws Exception {
        try {
            InetSocketAddress address = new InetSocketAddress(8000);
            HttpsServer httpsServer = HttpsServer.create(address, 0);

            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            KeyStore ks = KeyStore.getInstance(KeyStore.getDefaultType());
            ks.load(null); // This isn't coming from a java keystore file as we want to use raw x509 like a normal program
            char[] options = {'0','1','2','3','4','5','6','7','8','9','a','b','c','d','e','f'};
            char[] randomPassword = new char[32];
            SecureRandom random = new SecureRandom();
            for (int i = 0; i < randomPassword.length; i++) {
                randomPassword[i] = options[random.nextInt(options.length)];
            }

            ks.setKeyEntry(
                    "serverCert",
                    APIServer.readPrivateKey(new File("certs/privkey.pem")),
                    randomPassword,
                    cf.generateCertificates(new FileInputStream("certs/fullchain.pem")).toArray(new Certificate[0])
            );
            TrustManagerFactory tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            tmf.init(ks);

            KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(ks, randomPassword);

            SSLContext sslContext = SSLContext.getInstance("TLS");
            sslContext.init(keyManagerFactory.getKeyManagers(), tmf.getTrustManagers(), random);

            System.out.println("Configuring server");
            httpsServer.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
                public void configure(HttpsParameters params) {
                    try {
                        // initialise the SSL context
                        SSLContext context = getSSLContext();
                        SSLEngine engine = context.createSSLEngine();
                        params.setNeedClientAuth(false);
                        params.setCipherSuites(engine.getEnabledCipherSuites());
                        params.setProtocols(engine.getEnabledProtocols());

                        // Set the SSL parameters
                        SSLParameters sslParameters = context.getSupportedSSLParameters();
                        params.setSSLParameters(sslParameters);
                        System.out.println("New SSL connection");
                    } catch (Exception ex) {
                        System.err.println("Failed to create HTTPS port");
                    }
                }
            });
            httpsServer.createContext("/", new RootHandler());
            httpsServer.createContext("/api/v1", new APIHandlerV1(this.databaseInstance));
            httpsServer.setExecutor(new ThreadPoolExecutor(4, 8, 30, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(100)));
            httpsServer.start();
            System.out.println("Initialising server");
        } catch (Exception exception) {
            System.err.println("Failed to create HTTPS server on port " + 8000 + " of localhost");
            exception.printStackTrace();
        }
    }

}