---
title: "Data Types"
nav-parent_id: python_datastream_api
nav-pos: 10
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

* This will be replaced by the TOC
{:toc}

A data type describes the type of a value in the DataStream ecosystem. It can be used to declare input and/or output types of operations. Meanwhile, you don't always have to declare types for operations like Java/Scala. If type has not been declared, data would be serialized or deserialized using pickle.

## Supported Data Types

There are seven different categories of data types:

1. **Primitive Types**
2. **Array Types**
3. **Row and Tuple Type**
