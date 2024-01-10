/*
 * Copyright (2021) The Delta Lake Project Authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.delta.storage;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.ProvisionedThroughput;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A concrete implementation of {@link BaseS3DynamoDBLogStore} that uses an external DynamoDB table
 * to provide the mutual exclusion during calls to `putExternalEntry`.
 * <p>
 * DynamoDB entries are of form
 * - key
 * -- tablePath (HASH, STRING)
 * -- filename (RANGE, STRING)
 * <p>
 * - attributes
 * -- tempPath (STRING, relative to _delta_log)
 * -- complete (STRING, representing boolean, "true" or "false")
 * -- commitTime (NUMBER, epoch seconds)
 */
public class S3DynamoDBLogStore extends BaseS3DynamoDBLogStore {
    private static final Logger LOG = LoggerFactory.getLogger(S3DynamoDBLogStore.class);

    /**
     * Configuration keys for the DynamoDB client.
     */
    public static final String DYNAMO_DB_CONF_PREFIX = "S3DynamoDBLogStore";
    public static final String DDB_CLIENT_REGION = "ddb.region";
    public static final String DDB_CREATE_TABLE_RCU = "provisionedThroughput.rcu";
    public static final String DDB_CREATE_TABLE_WCU = "provisionedThroughput.wcu";

    /**
     * Member fields
     */
    private final String regionName;


    public S3DynamoDBLogStore(Configuration hadoopConf) throws IOException {
        super(hadoopConf);
        regionName = getParam(hadoopConf, DDB_CLIENT_REGION, "us-east-1");
        LOG.info("using regionName {}", regionName);
        initClient(hadoopConf);
    }

    @Override
    protected AmazonDynamoDB getClient() throws java.io.IOException {
        try {
            return AmazonDynamoDBClientBuilder.standard()
                    .withCredentials(getAwsCredentialsProvider())
                    .withRegion(Regions.fromName(regionName))
                    .build();
        } catch (ReflectiveOperationException e) {
            throw new java.io.IOException(e);
        }
    }

    @Override
    protected ProvisionedThroughput getProvisionedThroughput(Configuration hadoopConf) {
        final long rcu = Long.parseLong(getParam(hadoopConf, DDB_CREATE_TABLE_RCU, "5"));
        final long wcu = Long.parseLong(getParam(hadoopConf, DDB_CREATE_TABLE_WCU, "5"));
        return new ProvisionedThroughput(rcu,wcu);
    }

    @Override
    protected String getConfPrefix() {
        return DYNAMO_DB_CONF_PREFIX;
    }

}
