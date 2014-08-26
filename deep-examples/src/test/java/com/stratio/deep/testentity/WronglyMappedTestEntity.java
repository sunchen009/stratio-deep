/*
 * Copyright 2014, Stratio.
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

package com.stratio.deep.testentity;

import com.stratio.deep.annotations.DeepEntity;
import com.stratio.deep.annotations.DeepField;
import com.stratio.deep.entity.IDeepType;

@DeepEntity
public class WronglyMappedTestEntity implements IDeepType {

    @DeepField(isPartOfPartitionKey = true)
    private String id;

    @DeepField(fieldName = "domain_name")
    private String domain;

    @DeepField
    private String url;

    @DeepField(fieldName = "not_existent_field")
    private String wronglyMappedField;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getDomain() {
        return domain;
    }

    public void setDomain(String domain) {
        this.domain = domain;
    }

    public String getUrl() {
        return url;
    }

    public void setUrl(String url) {
        this.url = url;
    }

    public String getWronglyMappedField() {
        return wronglyMappedField;
    }

    public void setWronglyMappedField(String wronglyMappedField) {
        this.wronglyMappedField = wronglyMappedField;
    }

    @Override
    public String toString() {
        return "WronglyMappedTestEntity{" +
                "id='" + id + '\'' +
                ", domain='" + domain + '\'' +
                ", url='" + url + '\'' +
                ", wronglyMappedField='" + wronglyMappedField + '\'' +
                '}';
    }
}
