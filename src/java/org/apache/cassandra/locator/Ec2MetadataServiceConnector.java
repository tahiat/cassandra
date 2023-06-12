/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.cassandra.locator;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.utils.Pair;

public interface Ec2MetadataServiceConnector
{
    String DEFAULT_METADATA_SERVICE_URL = "http://169.254.169.254";

    static Ec2MetadataServiceConnector create(SnitchProperties props)
    {
        String type = props.get("ec2_metadata_type", "v1");
        if (type.equals("v1"))
            return V1Connector.create(props);
        else if (type.equals("v2"))
            return V2Connector.create(props);
        else
            throw new ConfigurationException("ec2_metadata_type must be either v1 or v2");
    }

    default String awsApiCall(String query) throws IOException, ConfigurationException
    {
        return awsApiCall(query, "GET", ImmutableMap.of());
    }

    String awsApiCall(String query, String method, Map<String, String> extraHeaders) throws ConfigurationException, IOException;

    final class HttpException extends IOException
    {
        public final int responseCode;
        private final String responseMessage;

        public HttpException(int responseCode, String responseMessage)
        {
            super("HTTP response code: " + responseCode + " (" + responseMessage + ')');
            this.responseCode = responseCode;
            this.responseMessage = responseMessage;
        }
    }

    class V1Connector implements Ec2MetadataServiceConnector
    {
        protected final String metadataServiceUrl;

        static V1Connector create(SnitchProperties props)
        {
            String url = props.get("ec2_metadata_url", DEFAULT_METADATA_SERVICE_URL);
            return new V1Connector(url);
        }

        public V1Connector(String metadataServiceUrl)
        {
            this.metadataServiceUrl = metadataServiceUrl;
        }

        @Override
        public String awsApiCall(String query, String method, Map<String, String> extraHeaders) throws ConfigurationException, IOException
        {
            HttpURLConnection conn = null;
            try
            {
                // Populate the region and zone by introspection, fail if 404 on metadata
                conn = (HttpURLConnection) new URL(metadataServiceUrl + query).openConnection();
                extraHeaders.forEach(conn::setRequestProperty);
                conn.setRequestMethod(method);
                if (conn.getResponseCode() != 200)
                    throw new HttpException(conn.getResponseCode(), conn.getResponseMessage());

                // Read the information. I wish I could say (String) conn.getContent() here...
                int cl = conn.getContentLength();
                byte[] b = new byte[cl];
                try (DataInputStream d = new DataInputStream((InputStream) conn.getContent()))
                {
                    d.readFully(b);
                }
                return new String(b, StandardCharsets.UTF_8);
            }
            finally
            {
                if (conn != null)
                    conn.disconnect();
            }
        }
    }

    class V2Connector extends V1Connector
    {
        private static final int MAX_TOKEN_TIME_IN_SECONDS = 21600;
        private static final String AWS_EC2_METADATA_TOKEN_HEADER = "X-aws-ec2-metadata-token";
        private static final String AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER = "X-aws-ec2-metadata-token-ttl-seconds";
        private static final String TOKEN_QUERY = "/latest/api/token";

        private Pair<String, Long> token;
        private final Duration tokenTTL;

        static V2Connector create(SnitchProperties props)
        {
            String url = props.get("ec2_metadata_url", DEFAULT_METADATA_SERVICE_URL);

            String tokenTTLString = props.get(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER,
                                              Integer.toString(MAX_TOKEN_TIME_IN_SECONDS));

            Duration tokenTTL;
            try
            {
                tokenTTL = Duration.ofSeconds(Integer.parseInt(tokenTTLString));

                if (tokenTTL.toSeconds() > MAX_TOKEN_TIME_IN_SECONDS || tokenTTL.toSeconds() < 30)
                {
                    throw new ConfigurationException(String.format("property %s was set to %s which is more than maximum allowed range of [30..21600]: %s, defaulting to %s",
                                                                   AWS_EC2_METADATA_TOKEN_HEADER, tokenTTL, MAX_TOKEN_TIME_IN_SECONDS, MAX_TOKEN_TIME_IN_SECONDS));
                }
            }
            catch (NumberFormatException ex)
            {
                throw new ConfigurationException(String.format("Unable to parse integer from %s, value to parse: %s. Defaulting to %s",
                                                               AWS_EC2_METADATA_TOKEN_HEADER, tokenTTLString, MAX_TOKEN_TIME_IN_SECONDS));
            }

            return new V2Connector(url, tokenTTL);
        }

        public V2Connector(String metadataServiceUrl, Duration tokenTTL)
        {
            super(metadataServiceUrl);
            this.tokenTTL = tokenTTL;
        }

        @Override
        public String awsApiCall(String query, String method, Map<String, String> extraHeaders) throws ConfigurationException, IOException
        {
            Map<String, String> headers = new HashMap<>(extraHeaders);
            int retries = 1;
            for (int retry = 0; retry <= retries; retry++)
            {
                String token = getToken();
                try
                {
                    headers.put(AWS_EC2_METADATA_TOKEN_HEADER, token);
                    return super.awsApiCall(query, method, headers);
                }
                catch (HttpException ex)
                {
                    if (retry == retries)
                        throw ex;

                    if (ex.responseCode == 401) // invalidate token if it is 401
                        this.token = null;
                }
            }

            throw new AssertionError();
        }

        /**
         * Get a session token to enable requests to the meta data service.
         * <a href="https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html">IMDSv2</a>
         */
        private synchronized String getToken()
        {
            if (token != null && token.right < System.currentTimeMillis())
                return token.left;

            try
            {
                token = Pair.create(super.awsApiCall(TOKEN_QUERY, "PUT",
                                                     ImmutableMap.of(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER,
                                                                     String.valueOf(tokenTTL))),
                                    System.currentTimeMillis() + tokenTTL.toMillis() - TimeUnit.SECONDS.toMillis(5));
                return token.left;
            }
            catch (IOException e)
            {
                throw new RuntimeException(e);
            }
        }
    }
}
