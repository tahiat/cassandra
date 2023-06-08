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
import java.io.FilterInputStream;
import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.FileUtils;

/**
 * An implementation of the Ec2Snitch which uses Instance Metadata Service v2 (IMDSv2) which requires you
 * to get a session token first before calling the metaData service
 */
public class Ec2SnitchIMDSv2 extends Ec2Snitch
{
    private static final Logger logger = LoggerFactory.getLogger(Ec2SnitchIMDSv2.class);
    private static final int MAX_TOKEN_TIME_IN_SECONDS = 21600;
    private static final String AWS_EC2_METADATA_TOKEN_HEADER = "X-aws-ec2-metadata-token";
    private static final String AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER = "X-aws-ec2-metadata-token-ttl-seconds";
    private static final String TOKEN_ENDPOINT = "http://169.254.169.254/latest/api/token";

    private Supplier<ApiCallResult> tokenSupplier;
    private long tokenTTL;

    public Ec2SnitchIMDSv2() throws IOException
    {
        super(new SnitchProperties());
    }

    public Ec2SnitchIMDSv2(SnitchProperties snitchProperties) throws IOException
    {
        super(snitchProperties);

        String parsedTokenTTL = snitchProperties.get(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER, Integer.toString(MAX_TOKEN_TIME_IN_SECONDS));

        try
        {
            tokenTTL = Integer.parseInt(parsedTokenTTL);

            if (tokenTTL > MAX_TOKEN_TIME_IN_SECONDS || tokenTTL < 1)
            {
                logger.info(String.format("property %s was set to %s which is more than maximum allowed range of (0, 21600]: %s, defaulting to %s",
                                          AWS_EC2_METADATA_TOKEN_HEADER, tokenTTL, MAX_TOKEN_TIME_IN_SECONDS, MAX_TOKEN_TIME_IN_SECONDS));
                tokenTTL = MAX_TOKEN_TIME_IN_SECONDS;
            }
        }
        catch (NumberFormatException ex)
        {
            logger.error(String.format("Unable to parse integer from %s, value to parse: %s. Defaulting to %s",
                                       AWS_EC2_METADATA_TOKEN_HEADER, parsedTokenTTL, MAX_TOKEN_TIME_IN_SECONDS));
            tokenTTL = MAX_TOKEN_TIME_IN_SECONDS;
        }

        tokenSupplier = Suppliers.memoizeWithExpiration(this::getNewToken, Math.max(5, tokenTTL - 100), TimeUnit.SECONDS);
    }

    @Override
    String awsApiCall(final String url) throws IOException
    {
        ApiCallResult token = tokenSupplier.get();

        if (token == null)
            throw new IllegalStateException("Unable to get token!");

        HttpURLConnection conn = (HttpURLConnection) new URL(url).openConnection();
        conn.setRequestProperty(AWS_EC2_METADATA_TOKEN_HEADER, token.body);

        // we need to return ApiCallResult but this would break awsApiCall
        ApiCallResult result = getResult(conn);

        // TODO
        // this should return whole ApiCallResult the implementations could inspect further
        // ideally, when tokenSupplier.get() return ApiCallResult with not 200 code,
        // it should return that one and proceed only in case tokenSupplier.get() returned 200.
        return result.body;
    }

    /**
     * Get a session token to enable requests to the meta data service.
     * https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/configuring-instance-metadata-service.html
     *
     * @throws IOException
     */
    private ApiCallResult getNewToken()
    {
        try
        {
            final URL url = new URL(TOKEN_ENDPOINT);

            final HttpURLConnection http = (HttpURLConnection) url.openConnection();
            http.setRequestProperty(AWS_EC2_METADATA_TOKEN_TTL_SECONDS_HEADER, String.valueOf(tokenTTL));
            http.setRequestMethod("PUT");

            return getResult(http);
        }
        catch (Exception ex)
        {
            logger.error("Unable to get a token!", ex);
        }

        return null;
    }

    public static class ApiCallResult
    {
        String body;
        int httpCode;
        Map<String, List<String>> headers;
    }

    private ApiCallResult getResult(final HttpURLConnection conn) throws IOException
    {
        DataInputStream d = null;
        try
        {
            ApiCallResult result = new ApiCallResult();
            final int cl = conn.getContentLength();
            final byte[] b = new byte[cl];
            d = new DataInputStream((FilterInputStream) conn.getContent());
            d.readFully(b);

            result.body = new String(b, StandardCharsets.UTF_8);
            result.headers = conn.getHeaderFields();
            result.httpCode = conn.getResponseCode();

            return result;
        }
        finally
        {
            FileUtils.close(d);
            conn.disconnect();
        }
    }
}
