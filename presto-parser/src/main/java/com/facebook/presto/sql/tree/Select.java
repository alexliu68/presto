/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.presto.sql.tree;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

public class Select
        extends Node
{
    private final boolean distinct;
    private final List<SelectItem> selectItems;
    private final Map<String, String> hints;

    public Select(boolean distinct, List<SelectItem> selectItems, Map<String, String> hints)
    {
        this.distinct = distinct;
        this.selectItems = ImmutableList.copyOf(checkNotNull(selectItems, "selectItems"));
        this.hints = ImmutableMap.copyOf(checkNotNull(hints, "hints"));
    }

    public boolean isDistinct()
    {
        return distinct;
    }

    public List<SelectItem> getSelectItems()
    {
        return selectItems;
    }

    public Map<String, String> getHints()
    {
        return hints;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitSelect(this, context);
    }

    @Override
    public String toString()
    {
        return Objects.toStringHelper(this)
                .add("distinct", distinct)
                .add("selectItems", selectItems)
                .add("hints", hints)
                .omitNullValues()
                .toString();
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        Select select = (Select) o;

        if (distinct != select.distinct) {
            return false;
        }
        if (!selectItems.equals(select.selectItems)) {
            return false;
        }
        if (!hints.equals(select.hints)) {
            return false;
        }

        return true;
    }

    @Override
    public int hashCode()
    {
        int result = (distinct ? 1 : 0);
        result = 31 * result + selectItems.hashCode() + hints.hashCode();
        return result;
    }
}
