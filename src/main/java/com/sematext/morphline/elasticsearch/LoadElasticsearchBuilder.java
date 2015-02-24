/*
 * Copyright 2013 Sematext Group, Inc.
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

package com.sematext.morphline.elasticsearch;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.elasticsearch.common.xcontent.XContentBuilder;
import org.kitesdk.morphline.api.Command;
import org.kitesdk.morphline.api.CommandBuilder;
import org.kitesdk.morphline.api.MorphlineContext;
import org.kitesdk.morphline.api.MorphlineRuntimeException;
import org.kitesdk.morphline.api.Record;
import org.kitesdk.morphline.base.AbstractCommand;
import org.kitesdk.morphline.base.FieldExpression;
import org.kitesdk.morphline.base.Metrics;
import org.kitesdk.morphline.base.Notifications;

import com.codahale.metrics.Timer;
import com.google.common.annotations.VisibleForTesting;
import com.typesafe.config.Config;

/**
 * A command that loads a record into an Elasticsearch server.
 */
public class LoadElasticsearchBuilder implements CommandBuilder {

  public static final String DOCUMENT_LOADER_TYPE = "documentLoader";
  public static final String ELASTICSEARCH_CONFIGURATION = "elasticsearchConfig";
  public static final String TTL = "ttl";
  public static final String INDEX_NAME = "index";
  public static final String TYPE = "type";
  public static final String IGNORE_FIELDS = "ignoreFields";

  @Override
  public Collection<String> getNames() {
    return Collections.singletonList("loadElasticsearch");
  }

  @Override
  public Command build(Config config, Command parent, Command child, MorphlineContext context) {
    return new LoadElasticsearch(this, config, parent, child, context);
  }

  ///////////////////////////////////////////////////////////////////////////////
  // Nested classes:
  ///////////////////////////////////////////////////////////////////////////////
  private static final class LoadElasticsearch extends AbstractCommand {
    private DocumentLoader loader;
    private final Timer elapsedTime;
    private final FieldExpression indexName;
    private final FieldExpression indexType;
    private final Set<String> ignoreFields;
    private final int ttl;

    LoadElasticsearch(CommandBuilder builder, Config config, Command parent, Command child, MorphlineContext context) {
      super(builder, config, parent, child, context);
      elapsedTime = getTimer(Metrics.ELAPSED_TIME);
      Config elasticsearchConfig = getConfigs().getConfig(config, ELASTICSEARCH_CONFIGURATION);
      String loaderType = getConfigs().getString(config, DOCUMENT_LOADER_TYPE);
      indexName = new FieldExpression(getConfigs().getString(config, INDEX_NAME), getConfig());
      indexType = new FieldExpression(getConfigs().getString(config, TYPE), getConfig());
      ignoreFields = new LinkedHashSet<String>(getConfigs().getStringList(config, IGNORE_FIELDS, new ArrayList<String>()));
      ttl = getConfigs().getInt(config, TTL);
      validateArguments();

      DocumentLoaderFactory documentLoaderFactory = new DocumentLoaderFactory();
      try {
        loader = documentLoaderFactory.getClient(loaderType, elasticsearchConfig);
      } catch (IllegalArgumentException e) {
        throw new MorphlineRuntimeException(e);
      }
    }

    @VisibleForTesting
    public void setLoader(DocumentLoader loader) {
      this.loader = loader;
    }

    @Override
    protected void doNotify(Record notification) {
      for (Object event : Notifications.getLifecycleEvents(notification)) {
        if (event == Notifications.LifecycleEvent.BEGIN_TRANSACTION) {
          try {
            loader.beginTransaction();
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        } else if (event == Notifications.LifecycleEvent.COMMIT_TRANSACTION) {
          try {
            loader.commitTransaction();
          } catch (Exception e) {
            throw new MorphlineRuntimeException(e);
          }
        } else if (event == Notifications.LifecycleEvent.ROLLBACK_TRANSACTION) {
          try {
            loader.rollbackTransaction();
          } catch (IOException e) {
            throw new MorphlineRuntimeException(e);
          }
        } else if (event == Notifications.LifecycleEvent.SHUTDOWN) {
          try {
            loader.shutdown();
          } catch (Exception e) {
            throw new MorphlineRuntimeException(e);
          }
        }
      }
      super.doNotify(notification);
    }

    @Override
    protected boolean doProcess(Record record) {
      Timer.Context timerContext = elapsedTime.time();

      try {
        XContentBuilder documentBuilder = jsonBuilder().startObject();
        Map<String, Collection<Object>> map = record.getFields().asMap();
        for (Map.Entry<String, Collection<Object>> entry : map.entrySet()) {
          String key = entry.getKey();
          if (ignoreFields.contains(key)) {
            continue;
          }
          Collection<Object> values = entry.getValue();
          Iterator<Object> iterator = values.iterator();
          if (values.size() > 1) {
            documentBuilder.startArray(key);
            while (iterator.hasNext()) {
              documentBuilder.value(iterator.next());
            }
            documentBuilder.endArray();
          } else if (iterator.hasNext()) {
            documentBuilder.field(key, iterator.next());
          }
        }
        documentBuilder.endObject();
        String name = indexName.evaluate(record).get(0).toString();
        String type = indexType.evaluate(record).get(0).toString();
        loader.addDocument(documentBuilder.bytes(), name, type, ttl);
      } catch (Exception e) {
        throw new MorphlineRuntimeException(e);
      } finally {
        timerContext.stop();
      }

      // pass record to next command in chain:
      return super.doProcess(record);
    }
  }
}
