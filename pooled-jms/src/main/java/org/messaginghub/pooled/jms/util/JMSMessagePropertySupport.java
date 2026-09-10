/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.messaginghub.pooled.jms.util;

import jakarta.jms.JMSException;
import jakarta.jms.MessageFormatException;

/**
 * Provides methods for use when working with JMS Message Properties and their values.
 */
public class JMSMessagePropertySupport {

    private JMSMessagePropertySupport() {}

    //----- Conversions Validation for Message Properties --------------------//

    /**
     * Attempts to convert a named property into a different type.
     *
     * @param <T> The return type after conversion
     * @param name The name of the property whose value is being converted
     * @param value The value to convert
     * @param target The target type that the value is being transformed into.
     *
     * @return the new converted value type.
     *
     * @throws JMSException if an error occurs while performing the conversion
     */
    @SuppressWarnings("unchecked")
    public static <T> T convertPropertyTo(String name, Object value, Class<T> target) throws JMSException {
        if (value == null) {
            if (Boolean.class.equals(target)) {
                return (T) Boolean.FALSE;
            } else if (Float.class.equals(target) || Double.class.equals(target)) {
                throw new NullPointerException("property " + name + " was null");
            } else if (Number.class.isAssignableFrom(target)) {
                throw new NumberFormatException("property " + name + " was null");
            } else {
                return null;
            }
        }

        final T rc = (T) TypeConversionSupport.convert(value, target);

        if (rc == null) {
            throw new MessageFormatException("Property " + name + " was a " + value.getClass().getName() + " and cannot be read as a " + target.getName());
        }

        return rc;
    }

    //----- Property Name Validation Methods ---------------------------------//

    /**
     * Checks if the given JMS property name against constraints defined in the JMS specification with
     * an option to only perform simple null and empty checks.
     *
     * @param propertyName
     * 	The property name to check against the specification
     * @param validateNames
     * 	Should the name be validated against the specification or just simple null and empty checks be done
     */
    public static void checkPropertyNameIsValid(String propertyName, boolean validateNames) throws IllegalArgumentException {
        if (propertyName == null) {
            throw new IllegalArgumentException("Property name must not be null");
        } else if (propertyName.length() == 0) {
            throw new IllegalArgumentException("Property name must not be the empty string");
        }

        if (validateNames) {
            checkIdentifierLetterAndDigitRequirements(propertyName);
            checkIdentifierIsntNullTrueFalse(propertyName);
            checkIdentifierIsntLogicOperator(propertyName);
        }
    }

    /**
     * Checks if the given identifier equals any string values that match logical operators.
     *
     * @param identifier The identifier to check
     */
    public static void checkIdentifierIsntLogicOperator(String identifier) {
        // Identifiers cannot be NOT, AND, OR, BETWEEN, LIKE, IN, IS, or ESCAPE.
        if ("NOT".equals(identifier) || "AND".equals(identifier) || "OR".equals(identifier) ||
            "BETWEEN".equals(identifier) || "LIKE".equals(identifier) || "IN".equals(identifier) ||
            "IS".equals(identifier) || "ESCAPE".equals(identifier)) {

            throw new IllegalArgumentException("Identifier not allowed in JMS: '" + identifier + "'");
        }
    }

    /**
     * Checks if the given identifier equals any string values that match null, true or false.
     *
     * @param identifier The identifier to check
     */
    public static void checkIdentifierIsntNullTrueFalse(String identifier) {
        // Identifiers cannot be the names NULL, TRUE, and FALSE.
        if ("NULL".equals(identifier) || "TRUE".equals(identifier) || "FALSE".equals(identifier)) {
            throw new IllegalArgumentException("Identifier not allowed in JMS: '" + identifier + "'");
        }
    }

    /**
     * Checks if the given identifier equals any string values that violates JMS letter and digit restrictions
     *
     * @param identifier The identifier to check
     */
    public static void checkIdentifierLetterAndDigitRequirements(String identifier) {
        // An identifier is an unlimited-length sequence of letters and digits, the first of
        // which must be a letter.  A letter is any character for which the method
        // Character.isJavaLetter returns true.  This includes '_' and '$'.  A letter or digit
        // is any character for which the method Character.isJavaLetterOrDigit returns true.
        final char startChar = identifier.charAt(0);

        if (!(Character.isJavaIdentifierStart(startChar))) {
            throw new IllegalArgumentException("Identifier does not begin with a valid JMS identifier start character: '" + identifier + "' ");
        }

        // JMS part character
        final int length = identifier.length();

        for (int i = 1; i < length; i++) {
            char ch = identifier.charAt(i);
            if (!(Character.isJavaIdentifierPart(ch))) {
                throw new IllegalArgumentException("Identifier contains invalid JMS identifier character '" + ch + "': '" + identifier + "' ");
            }
        }
    }

    //----- Property Type Validation Methods ---------------------------------//

    /**
     * Checks if the given value is a valid type for use in JMS properties
     *
     * @param value The value type to check for compliance.
     *
     * @throws MessageFormatException if any violations are found
     */
    public static void checkValidObject(Object value) throws MessageFormatException {
        final boolean valid = value instanceof Boolean ||
                              value instanceof Byte ||
                              value instanceof Short ||
                              value instanceof Integer ||
                              value instanceof Long ||
                              value instanceof Float ||
                              value instanceof Double ||
                              value instanceof Character ||
                              value instanceof String ||
                              value == null;

        if (!valid) {
            throw new MessageFormatException("Only objectified primitive objects and String types are allowed but was: " + value + " type: " + value.getClass());
        }
    }
}
