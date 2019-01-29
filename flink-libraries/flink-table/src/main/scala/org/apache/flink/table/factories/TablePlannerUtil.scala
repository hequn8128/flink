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

package org.apache.flink.table.factories

import java.util
import java.util.{ServiceConfigurationError, ServiceLoader, Map => JMap}

import org.apache.flink.table.api._
import org.apache.flink.table.descriptors.{Descriptor, FormatDescriptorValidator, SchemaValidator}
import org.apache.flink.table.util.Logging
import org.apache.flink.util.Preconditions

import _root_.scala.collection.JavaConverters._
import _root_.scala.collection.mutable

object TablePlannerUtil extends Logging {

  private lazy val defaultLoader = ServiceLoader.load(classOf[TablePlannerFactory])


  def generatePlannerDiscriptor(config: TableConfig): ToolConnectorDescriptor = {
    new ToolConnectorDescriptor(config)
  }

  /**
    * Finds a table factory of the given class and descriptor.
    *
    * @param plannerClass desired factory class
    * @param descriptor descriptor describing the factory configuration
    * @tparam T factory class type
    * @return the matching factory
    */
  def find[T](plannerClass: Class[T], descriptor: Descriptor): T = {
    Preconditions.checkNotNull(descriptor)

    findInternal(plannerClass, descriptor.toProperties, None)
  }

  /**
    * Finds a table factory of the given class, property map, and classloader.
    *
    * @param factoryClass desired factory class
    * @param propertyMap properties that describe the factory configuration
    * @param classLoader classloader for service loading
    * @tparam T factory class type
    * @return the matching factory
    */
  private def findInternal[T](
      factoryClass: Class[T],
      propertyMap: JMap[String, String],
      classLoader: Option[ClassLoader])
  : T = {

    Preconditions.checkNotNull(factoryClass)
    Preconditions.checkNotNull(propertyMap)

    val properties = propertyMap.asScala.toMap

    val foundFactories = discoverFactories(classLoader)

    val classFactories = filterByFactoryClass(
      factoryClass,
      properties,
      foundFactories)

    val contextFactories = filterByContext(
      factoryClass,
      properties,
      foundFactories,
      classFactories)


    filterBySupportedProperties(
      factoryClass,
      properties,
      foundFactories,
      contextFactories)
  }

  /**
    * Filters factories with matching context by factory class.
    */
  private def filterByFactoryClass[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TablePlannerFactory])
  : Seq[TablePlannerFactory] = {

    val classFactories = foundFactories.filter(f => factoryClass.isAssignableFrom(f.getClass))
    if (classFactories.isEmpty) {
      throw new NoMatchingTablePlannerFactoryException(
        s"No factory implements '${factoryClass.getCanonicalName}'.",
        factoryClass,
        foundFactories,
        properties)
    }
    classFactories
  }

  /**
    * Filters for factories with matching context.
    *
    * @return all matching factories
    */
  private def filterByContext[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TablePlannerFactory],
      classFactories: Seq[TablePlannerFactory])
  : Seq[TablePlannerFactory] = {

    val matchingFactories = classFactories.filter { factory =>
      val requestedContext = normalizeContext(factory)

      val plainContext = mutable.Map[String, String]()
      plainContext ++= requestedContext

      // check if required context is met
      plainContext.forall { e =>
        properties.contains(e._1) &&
          ((properties(e._1).equalsIgnoreCase(e._2)) ||
            properties(e._1) == e._2)
      }
    }

    if (matchingFactories.isEmpty) {
      throw new NoMatchingTablePlannerFactoryException(
        "No context matches.",
        factoryClass,
        foundFactories,
        properties)
    }

    matchingFactories
  }


  /**
    * Filters the matching class factories by supported properties.
    */
  private def filterBySupportedProperties[T](
      factoryClass: Class[T],
      properties: Map[String, String],
      foundFactories: Seq[TablePlannerFactory],
      classFactories: Seq[TablePlannerFactory])
  : T = {

    val plainGivenKeys = mutable.ArrayBuffer[String]()
    properties.keys.foreach { k =>
      // replace arrays with wildcard
      val key = k.replaceAll(".\\d+", ".#")
      // ignore duplicates
      if (!plainGivenKeys.contains(key)) {
        plainGivenKeys += key
      }
    }
    var lastKey: Option[String] = None
    val supportedFactories = classFactories.filter { factory =>
      val requiredContextKeys = normalizeContext(factory).keySet
      val (supportedKeys, wildcards) = normalizeSupportedProperties(factory)
      // ignore context keys
      val givenContextFreeKeys = plainGivenKeys.filter(!requiredContextKeys.contains(_))
      // perform factory specific filtering of keys
      val givenFilteredKeys = filterSupportedPropertiesFactorySpecific(
        factory,
        givenContextFreeKeys)

      givenFilteredKeys.forall { k =>
        lastKey = Option(k)
        supportedKeys.contains(k) || wildcards.exists(k.startsWith)
      }
    }

    if (supportedFactories.isEmpty && classFactories.length == 1 && lastKey.isDefined) {
      // special case: when there is only one matching factory but the last property key
      // was incorrect
      val factory = classFactories.head
      val (supportedKeys, _) = normalizeSupportedProperties(factory)
      throw new NoMatchingTablePlannerFactoryException(
        s"""
           |The matching factory '${factory.getClass.getName}' doesn't support '${lastKey.get}'.
           |
          |Supported properties of this factory are:
           |${supportedKeys.sorted.mkString("\n")}""".stripMargin,
        factoryClass,
        foundFactories,
        properties, null)
    } else if (supportedFactories.isEmpty) {
      throw new NoMatchingTablePlannerFactoryException(
        s"No factory supports all properties.",
        factoryClass,
        foundFactories,
        properties,
        null)
    } else if (supportedFactories.length > 1) {
      throw new AmbiguousTablePlannerFactoryException(
        supportedFactories,
        factoryClass,
        foundFactories,
        properties)
    }

    supportedFactories.head.asInstanceOf[T]
  }

  /**
    * Prepares the supported properties of a factory to be used for match operations.
    */
  private def normalizeSupportedProperties(factory: TablePlannerFactory): (Seq[String], Seq[String]) = {
    val supportedPropertiesJava = factory.supportedProperties()
    if (supportedPropertiesJava == null) {
      throw new TableException(
        s"Supported properties of factory '${factory.getClass.getName}' must not be null.")
    }
    val supportedKeys = supportedPropertiesJava.asScala.map(_.toLowerCase)

    // extract wildcard prefixes
    val wildcards = extractWildcardPrefixes(supportedKeys)

    (supportedKeys, wildcards)
  }

  /**
    * Converts the prefix of properties with wildcards (e.g., "format.*").
    */
  private def extractWildcardPrefixes(propertyKeys: Seq[String]): Seq[String] = {
    propertyKeys
      .filter(_.endsWith("*"))
      .map(s => s.substring(0, s.length - 1))
  }


  /**
    * Performs filtering for special cases (i.e. table format factories with schema derivation).
    */
  private def filterSupportedPropertiesFactorySpecific(
       factory: TablePlannerFactory,
       keys: Seq[String])
  : Seq[String] = factory match {

    case formatFactory: TableFormatFactory[_] =>
      val includeSchema = formatFactory.supportsSchemaDerivation()
      // ignore non-format (or schema) keys
      keys.filter { k =>
        if (includeSchema) {
          k.startsWith(SchemaValidator.SCHEMA + ".") ||
            k.startsWith(FormatDescriptorValidator.FORMAT + ".")
        } else {
          k.startsWith(FormatDescriptorValidator.FORMAT + ".")
        }
      }

    case _ =>
      keys
  }

  /**
    * Prepares the properties of a context to be used for match operations.
    */
  private def normalizeContext(factory: TablePlannerFactory): Map[String, String] = {
    val requiredContextJava = factory.requiredContext()
    if (requiredContextJava == null) {
      throw new TableException(
        s"Required context of factory '${factory.getClass.getName}' must not be null.")
    }
    requiredContextJava.asScala.map(e => (e._1.toLowerCase, e._2)).toMap
  }


  /**
    * Searches for factories using Java service providers.
    *
    * @return all factories in the classpath
    */
  private def discoverFactories[T](classLoader: Option[ClassLoader]): Seq[TablePlannerFactory] = {
    try {
      val iterator = classLoader match {
        case Some(customClassLoader) =>
          val customLoader = ServiceLoader.load(classOf[TablePlannerFactory], customClassLoader)
          customLoader.iterator()
        case None =>
          defaultLoader.iterator()
      }
      iterator.asScala.toSeq
    } catch {
      case e: ServiceConfigurationError =>
        LOG.error("Could not load service provider for table factories.", e)
        throw new TableException("Could not load service provider for table factories.", e)
    }
  }
}

class ToolConnectorDescriptor(tableConfig: TableConfig) extends Descriptor {
  /**
    * Converts this descriptor into a set of properties.
    */
  override def toProperties: util.Map[String, String] = {
    val map = new util.HashMap[String, String]()
    val mode = if (tableConfig.getExecutionMode == ExecutionMode.BatchMode) "batch" else "stream"
    map.put("mode", mode)
    map
  }
}
