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

package org.apache.flink.table.api.dataview;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.typeinfo.TypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.dataview.ListViewTypeInfoFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@link ListView} provides List functionality for accumulators used by user-defined aggregate
 * functions {@link org.apache.flink.api.common.functions.AggregateFunction}.
 *
 * <p>A {@link ListView} can be backed by a Java {@link ArrayList} or a state backend, depending on
 * the context in which the aggregate function is used.
 *
 * <p>At runtime {@link ListView} will be replaced by a
 * <code>org.apache.flink.table.dataview.StateListView</code> if it is backed by a state backend.
 *
 * <p>Example of an accumulator type with a {@link ListView} and an aggregate function that uses it:
 *
 * <pre>
 * {@code
 *
 *  public class MyAccum {
 *    public ListView<String> list;
 *    public long count;
 *  }
 *
 *  public class MyAgg extends AggregateFunction<Long, MyAccum> {
 *
 *   @Override
 *   public MyAccum createAccumulator() {
 *     MyAccum accum = new MyAccum();
 *     accum.list = new ListView<>(Types.STRING);
 *     accum.count = 0L;
 *     return accum;
 *   }
 *
 *   public void accumulate(MyAccum accumulator, String id) {
 *     accumulator.list.add(id);
 *     ... ...
 *     accumulator.get()
 *     ... ...
 *   }
 *
 *   @Override
 *   public Long getValue(MyAccum accumulator) {
 *     accumulator.list.add(id);
 *     ... ...
 *     accumulator.get()
 *     ... ...
 *     return accumulator.count;
 *   }
 * }
 *
 * }
 * </pre>
 *
 * @param <T> element type
 */
@PublicEvolving
@TypeInfo(ListViewTypeInfoFactory.class)
public class ListView<T> implements DataView {

	private final transient TypeInformation<T> elementTypeInfo;
	private final List<T> list;

	public ListView(TypeInformation<T> elementTypeInfo, List<T> list) {
		this.elementTypeInfo = elementTypeInfo;
		this.list = list;
	}

	/**
	 * Creates a list view for elements of the specified type.
	 *
	 * @param elementTypeInfo The type of the list view elements.
	 */
	public ListView(TypeInformation<T> elementTypeInfo) {
		this(elementTypeInfo, new ArrayList<T>());
	}

	/**
	 * Creates a list view.
	 */
	public ListView() {
		this(null);
	}

	public TypeInformation<T> getElementTypeInfo() {
		return elementTypeInfo;
	}

	public List<T> getList() {
		return list;
	}

	/**
	 * Returns an iterable of the list view.
	 *
	 * @throws Exception Thrown if the system cannot get data.
	 * @return The iterable of the list or <code>null</code> if the list is empty.
	 */
	public Iterable<T> get() throws Exception {
		if (!list.isEmpty()) {
			return list;
		} else {
			return null;
		}
	}

	/**
	 * Adds the given value to the list.
	 *
	 * @throws Exception Thrown if the system cannot add data.
	 * @param value The element to be appended to this list view.
	 */
	public void add(T value) throws Exception {
		list.add(value);
	}

	/**
	 * Adds all of the elements of the specified list to this list view.
	 *
	 * @throws Exception Thrown if the system cannot add all data.
	 * @param list The list with the elements that will be stored in this list view.
	 */
	public void addAll(List<T> list) throws Exception {
		this.list.addAll(list);
	}

	/**
	 * Removes all of the elements from this list view.
	 */
	@Override
	public void clear() {
		list.clear();
	}

	@Override
	public boolean equals(Object other) {

		if (this == other) {
			return true;
		}

		if (other == null || getClass() != other.getClass()) {
			return false;
		}

		ListView listView = (ListView) other;
		return (this.list == null && listView.list == null) ||
			this.list.equals(listView.list);
	}

	@Override
	public int hashCode() {
		return list.hashCode();
	}
}
